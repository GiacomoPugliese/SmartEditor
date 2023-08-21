# Import necessary libraries
import streamlit as st
import pandas as pd
import requests
import os
import shutil
import time
import zipfile
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaInMemoryUpload
from google.oauth2.credentials import Credentials
import json
import boto3
from PIL import Image
from io import BytesIO
import matplotlib.pyplot as plt
import numpy as np
from fpdf import FPDF
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
import moviepy.editor as mp
from moviepy.editor import VideoFileClip, concatenate_videoclips
import os
import subprocess
import boto3
import tempfile
import pyheif

# create s3 client
s3_client = boto3.client('s3', 
                         aws_access_key_id='AKIARK3QQWNWXGIGOFOH',
                         aws_secret_access_key='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')
bucket_name = 'li-general-tasks'


def generate_images(template_id, output_id, merge_fields_arr, uploaded_csv):
    # Google Drive service setup
    CLIENT_SECRET_FILE = 'credentials.json'
    API_NAME = 'drive'
    API_VERSION = 'v3'
    SCOPES = ['https://www.googleapis.com/auth/drive']

    with open(CLIENT_SECRET_FILE, 'r') as f:
        client_info = json.load(f)['web']

    creds_dict = st.session_state['creds']
    creds_dict['client_id'] = client_info['client_id']
    creds_dict['client_secret'] = client_info['client_secret']
    creds_dict['refresh_token'] = creds_dict.get('_refresh_token')

    # Create Credentials from creds_dict
    creds = Credentials.from_authorized_user_info(creds_dict)

    # Build the Google Drive service
    drive_service = build('drive', 'v3', credentials=creds)
    
    # Build the Google Slides service
    slides_service = build('slides', 'v1', credentials=creds)

    # Read the uploaded CSV file into a pandas DataFrame
    df = pd.read_csv(uploaded_csv)

    # Iterate over each row of the DataFrame
    for _, row in df.iterrows():
        # Make a temporary copy of the template
        body = {'name': 'Temp Copy'}
        copy = drive_service.files().copy(fileId=template_id, body=body).execute()
        copy_id = copy['id']

        # Populate the copy with data from the current row
        requests_list = []
        for field in merge_fields_arr:
            requests_list.append({
                'replaceAllText': {
                    'containsText': {
                        'text': field ,
                        'matchCase': False
                    },
                    'replaceText': str(row[field])
                }
            })
        slides_service.presentations().batchUpdate(presentationId=copy_id, body={'requests': requests_list}).execute()

        # Export the populated copy as a JPEG image
        presentation = slides_service.presentations().get(presentationId=copy_id).execute()
        slide = presentation['slides'][0]
        slide_id = slide['objectId']
        thumbnail_info = f"https://slides.googleapis.com/v1/presentations/{copy_id}/pages/{slide_id}/thumbnail?access_token={creds.token}"
        
        response = requests.get(thumbnail_info)
        content_type = response.headers['Content-Type']

        if content_type == 'application/json; charset=UTF-8':
            json_response = json.loads(response.text)
            image_url = json_response['contentUrl']
            image_data = requests.get(image_url).content
        else:
            # Handle different content types here
            print(f"Unexpected content type: {content_type}")
            continue

        # Save the image data to a local file for verification
        with open(f'Image_{_+1}.jpeg', 'wb') as image_file:
            image_file.write(image_data)

        # Upload the JPEG image to the specified Google Drive folder
        media = MediaInMemoryUpload(image_data, mimetype='image/jpeg', resumable=True)
        file_metadata = {'name': 'Image_' + str(_+1) + '.jpeg', 'parents': [output_id]}
        uploaded_image = drive_service.files().create(body=file_metadata, media_body=media).execute()

        # Delete the temporary copy
        drive_service.files().delete(fileId=copy_id).execute()

    # Return a confirmation message
    return 'Images have been successfully generated and uploaded to the specified Google Drive folder.'

def create_pdf_id(image_paths):
    pdf = FPDF(orientation = "P", unit = "mm", format = "A4")
    pdf.set_auto_page_break(auto = False)
    images_per_page = 8
    images_per_row = 2
    margin = 10
    space_between_images = 2
    available_width = ((210 - 2 * margin) - space_between_images) / images_per_row
    available_height = ((297 - 2 * margin) - 3 * space_between_images) / 4

    for i in range(0, len(image_paths), images_per_page):
        pdf.add_page()
        images = image_paths[i:i+images_per_page]

        for j, image in enumerate(images):
            row = j // images_per_row
            col = j % images_per_row
            x = margin + col * (available_width + space_between_images)
            y = margin + row * (available_height + space_between_images)

            with Image.open(image) as img:
                ratio = min(available_width / img.width, available_height / img.height)
                image_width = ratio * img.width
                image_height = ratio * img.height

                # Generate a unique temporary file name for each image
                temp_img_path = "tmp_{}.jpeg".format(str(uuid.uuid4()))
                img.save(temp_img_path, "JPEG")

            pdf.image(temp_img_path, x = x, y = y, w = image_width, h = image_height)

            # Delete the temporary image file
            os.remove(temp_img_path)

    pdf.output("Images/images.pdf")

def create_pdf_door(image_paths):
    pdf = FPDF(orientation = "P", unit = "mm", format = "A4")
    pdf.set_auto_page_break(auto = False)
    
    margin = 10  # adjust as needed
    available_width = 210 - 2 * margin
    available_height = 297 - 2 * margin

    for image in image_paths:
        pdf.add_page()
        
        # Open the image file
        with Image.open(image) as img:
            # Rotate the image 90 degrees
            img = img.rotate(-90, expand=True)
            print(image)
            
            # Calculate new width and height
            ratio = min(available_width / img.width, available_height / img.height)
            image_width = ratio * img.width
            image_height = ratio * img.height

            # Position the image in the center of the area
            x = (210 - image_width) / 2
            y = (297 - image_height) / 2

            # Generate a unique temporary file name for each image
            img_path_rotated = "tmp_rotated_{}.jpg".format(str(uuid.uuid4()))
            img.save(img_path_rotated)

        # Now width or height will adjust based on image aspect ratio
        pdf.image(img_path_rotated, x = x, y = y, w = image_width, h = image_height)
        
        # Delete the temporary rotated image file
        os.remove(img_path_rotated)

    pdf.output("Images/images.pdf")

def upload_to_s3(local_image_path, image_name):
    # Set AWS details (replace with your own details)
    AWS_REGION_NAME = 'us-east-2'
    AWS_ACCESS_KEY = 'AKIARK3QQWNWXGIGOFOH'
    AWS_SECRET_KEY = 'ClAUaloRIp3ebj9atw07u/o3joULLY41ghDiDc2a'
    BUCKET_NAME = 'li-general-tasks'
    S3_INPUT_PREFIX = 'input_videos/'

    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # Upload the image data to S3
    with open(local_image_path, 'rb') as image_file:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f'{S3_INPUT_PREFIX}{image_name}',
            Body=image_file,
        )

    # Return the public URL of the uploaded image in S3
    return f"https://{BUCKET_NAME}.s3.amazonaws.com/{S3_INPUT_PREFIX}{image_name}"

def generate_pdf(template_id, images_folder_id, upload_folder_id, uploaded_csv):
    print(images_folder_id)
    # Google Drive service setup
    CLIENT_SECRET_FILE = 'credentials.json'
    API_NAME = 'drive'
    API_VERSION = 'v3'
    SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/presentations']

    with open(CLIENT_SECRET_FILE, 'r') as f:
        client_info = json.load(f)['web']

    creds_dict = st.session_state['creds']
    creds_dict['client_id'] = client_info['client_id']
    creds_dict['client_secret'] = client_info['client_secret']
    creds_dict['refresh_token'] = creds_dict.get('_refresh_token')

    # Create Credentials from creds_dict
    creds = Credentials.from_authorized_user_info(creds_dict)

    # Build the Google Drive and Slides services
    drive_service = build('drive', 'v3', credentials=creds)
    slides_service = build('slides', 'v1', credentials=creds)

    # Read the uploaded CSV file into a pandas DataFrame
    df = pd.read_csv(uploaded_csv)

    # Get the list of images in the Google Drive folder
    images_request = drive_service.files().list(q=f"'{images_folder_id}' in parents").execute()
    images_files = images_request.get('files', [])
    image_paths = []

    # Map each image to the person's name
    images_map = {}
    for image_file in images_files:
        image_name = os.path.splitext(image_file['name'].replace('_', ' '))[0]
        images_map[image_name] = image_file['id']

    # Iterate over each row of the DataFrame
    for _, row in df.iterrows():
        # Make a temporary copy of the template
        body = {'name': 'Temp Copy'}
        copy = drive_service.files().copy(fileId=template_id, body=body).execute()
        copy_id = copy['id']

        # Retrieve the details of the copy
        presentation_copy = slides_service.presentations().get(presentationId=copy_id).execute()

        # Get the ID of the top-most image shape in the slide
        image_shape_id = None
        top_most_position = None
        for slide in presentation_copy['slides']:
            for shape in slide['pageElements']:
                if 'image' in shape:  # Check if the shape has the 'image' key'
                    position = shape['transform']['translateY']
                    if top_most_position is None or position < top_most_position:
                        top_most_position = position
                        image_shape_id = shape['objectId']
            if image_shape_id:
                break

        # Populate the copy with data from the current row
        requests_list = []
        for col in df.columns:
            if col == 'name':
                image_id = images_map.get(row[col])
                if image_id:
                    # Download the image from Google Drive to a local file
                    image_request = drive_service.files().get_media(fileId=image_id)
                    
                    image_data_io = BytesIO()
                    downloader = MediaIoBaseDownload(image_data_io, image_request)
                    done = False
                    while done is False:
                        status, done = downloader.next_chunk()

                    image_data_io.seek(0)
                    image_data = image_data_io.read()

                    # Get the metadata of the file
                    file_metadata = drive_service.files().get(fileId=image_id).execute()
                    image_format = file_metadata.get('mimeType', '').split('/')[-1].lower()
                    local_image_path = f"Images/{row['name']}.{image_format}"
                    print(image_format)
                    # Open the image from the response content
                    if image_format == 'png' or image_format == 'heic' or image_format == 'heif':
                        if image_format == 'png':
                            img = Image.open(BytesIO(image_data))
                        else:  # 'heic' in image_format
                            heif_file = pyheif.read(BytesIO(image_data))
                            img = Image.frombytes(
                                heif_file.mode,
                                heif_file.size,
                                heif_file.data,
                                "raw",
                                heif_file.mode,
                                heif_file.stride,
                            )
                        img_rgb = img.convert('RGB')
                        img_rgb.save(local_image_path, format='JPEG')
                    else:
                        img = Image.open(BytesIO(image_data))
                        # Check if the image mode is not 'RGB' and convert it if needed
                        if img.mode != 'RGB':
                            img = img.convert('RGB')
                        # Calculate new width while keeping aspect ratio
                        width = int(img.width * (176 / img.height))
                        # Resize the image
                        img = img.resize((width, 176))
                        # Save the image locally
                        img.save(local_image_path, format='JPEG')

                    # Upload the local image to S3
                    s3_image_url = upload_to_s3(local_image_path, f"Image_{row['name']}.jpeg")
                    requests_list.append({
                        'replaceImage': {
                            'imageObjectId': image_shape_id,  # Use the retrieved image shape ID
                            'imageReplaceMethod': 'IMAGE_REPLACE_METHOD_UNSPECIFIED',
                            'url': s3_image_url
                        }
                    })
            requests_list.append({
                'replaceAllText': {
                    'containsText': {
                        'text': f'{col}',
                        'matchCase': False
                    },
                    'replaceText': str(row[col])
                }
                })
        slides_service.presentations().batchUpdate(presentationId=copy_id, body={'requests': requests_list}).execute()

        # Export the populated copy as a JPEG image
        presentation = slides_service.presentations().get(presentationId=copy_id).execute()
        slide = presentation['slides'][0]
        slide_id = slide['objectId']
        thumbnail_info = f"https://slides.googleapis.com/v1/presentations/{copy_id}/pages/{slide_id}/thumbnail?access_token={creds.token}"
        
        response = requests.get(thumbnail_info)
        content_type = response.headers['Content-Type']

        if content_type == 'application/json; charset=UTF-8':
            json_response = json.loads(response.text)
            image_url = json_response['contentUrl']
            image_data = requests.get(image_url).content
        else:
            # Handle different content types here
            print(f"Unexpected content type: {content_type}")
            continue

        # Save the image data to a local file for verification
        image_path = f"Images/{row['name']}.jpeg"
        with open(image_path, 'wb') as image_file:
            image_file.write(image_data)
        image_paths.append(image_path)

        # Delete the temporary copy
        drive_service.files().delete(fileId=copy_id).execute()

    # Call your existing PDF creation functions
    if st.session_state["id"]:
        create_pdf_id(image_paths)
    else:
        create_pdf_door(image_paths)

    time.sleep(1)
    # Upload the PDF to Google Drive
    file_metadata = {
        'name': 'images.pdf',
        'parents': [upload_folder_id]
    }
    media = MediaFileUpload('Images/images.pdf',
                            mimetype='application/pdf')
    file = drive_service.files().create(body=file_metadata,
                                        media_body=media,
                                        fields='id').execute()

    print(f"PDF has been uploaded with file ID: {file.get('id')}")
    st.success("PDF Creation Complete!")

    # Clean up the local image files
    for image_path in image_paths:
        os.remove(image_path)

    # Return a confirmation message
    return 'PDFs have been successfully generated and uploaded to the specified Google Drive folder.'