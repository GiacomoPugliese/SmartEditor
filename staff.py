# Import necessary libraries
import streamlit as st
import pandas as pd
import shotstack_sdk as shotstack
from shotstack_sdk.api import edit_api
from shotstack_sdk.model.template_render import TemplateRender
from shotstack_sdk.model.merge_field import MergeField
import requests
import os
import shutil
import time
import zipfile
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials
import json
import boto3
from PIL import Image
from io import BytesIO
import matplotlib.pyplot as plt
import numpy as np
from fpdf import FPDF
import uuid
import pyheif

def create_pdf_id(image_paths):
    pdf = FPDF(orientation = "P", unit = "mm", format = "A4")  # Format of a typical printer paper
    pdf.set_auto_page_break(auto = False)
    images_per_page = 8  # now it's 8 images per page (2 columns, 4 rows)
    images_per_row = 2
    margin = 10
    space_between_images = 2  # additional space between images
    available_width = ((210 - 2 * margin) - space_between_images) / images_per_row
    available_height = ((297 - 2 * margin) - 3 * space_between_images) / 4  # four rows now

    for i in range(0, len(image_paths), images_per_page):
        pdf.add_page()
        images = image_paths[i:i+images_per_page]

        for j, image in enumerate(images):
            row = j // images_per_row
            col = j % images_per_row
            x = margin + col * (available_width + space_between_images)
            y = margin + row * (available_height + space_between_images)

            # Open the image file
            with Image.open(image) as img:
                # Calculate new width and height
                ratio = min(available_width / img.width, available_height / img.height)
                image_width = ratio * img.width
                image_height = ratio * img.height

            # Now width or height will adjust based on image aspect ratio
            pdf.image(image, x = x, y = y, w = image_width, h = image_height)

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

# create s3 client
s3_client = boto3.client('s3', 
                         aws_access_key_id='AKIARK3QQWNWXGIGOFOH',
                         aws_secret_access_key='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')

bucket_name = 'li-general-tasks'

if 'restart' not in st.session_state:
    st.session_state['restart'] = True
    st.session_state['download'] = False
    st.session_state['creds'] = ""
    st.session_state['intern'] = True
    st.session_state['id'] = True

if st.session_state['restart']:
    # Remove the 'Images' directory if it exists
    if os.path.exists('Images'):
        shutil.rmtree('Images')
    st.session_state['restart'] = False
    # Create the 'Images' directory
    os.makedirs('Images')

    # Remove the 'Videos' directory if it exists
    if os.path.exists('Videos'):
        shutil.rmtree('Videos')
    # Create the 'Videos' directory
    os.makedirs('Videos')

# Title of the app
st.title("Automatic Video and Image Editor")

st.subheader("Google authentication")

def nav_to(url):
    nav_script = """
        <meta http-equiv="refresh" content="0; url='%s'">
    """ % (url)
    st.write(nav_script, unsafe_allow_html=True)
try:
    if st.button("Authenticate Google Account"):
        st.session_state['begin_auth'] = True
        # Request OAuth URL from the FastAPI backend
        response = requests.get(f"{'https://photo-labeler-842ac8d73e7a.herokuapp.com'}/auth?user_id={'intros'}")
        if response.status_code == 200:
            # Get the authorization URL from the response
            auth_url = response.json().get('authorization_url')
            st.markdown(f"""
                <a href="{auth_url}" target="_blank" style="color: #8cdaf2;">
                    Click to continue to authentication page (before finalizing)


                </a>
                """, unsafe_allow_html=True)
            st.text("\n\n\n")
            # Redirect user to the OAuth URL
            # nav_to(auth_url)

    if st.session_state['begin_auth']:    
        if st.button("Finalize Google Authentication"):
            with st.spinner("Finalizing authentication..."):
                for i in range(6):
                    # Request token from the FastAPI backend
                    response = requests.get(f"{'https://photo-labeler-842ac8d73e7a.herokuapp.com'}/token/{'intros'}")
                    if response.status_code == 200:
                        st.session_state['creds'] = response.json().get('creds')
                        print(st.session_state['creds'])
                        st.success("Google account successfully authenticated!")
                        st.session_state['final_auth'] = True
                        break
                    time.sleep(1)
            if not st.session_state['final_auth']:
                st.error('Experiencing network issues, please refresh page and try again.')
                st.session_state['begin_auth'] = False

except Exception as e:
    print(e)

st.subheader("IDs and Doortags")

col1, col2 = st.columns(2)

with col1:
    option = st.selectbox(
        'Pick an output format',
        ['Intern Door', 'Intern ID', 'Staff Door', 'Staff ID'])
    if option == 'Intern Door' or option == 'Intern ID':
        st.session_state['intern'] = True
    else:
        st.session_state['intern'] = False
    if option == 'Intern ID' or option == 'Staff ID':
        st.session_state['id'] = True
    else:
        st.session_state['id'] = False

with col2:
    program_name = 'STAFF'

    if option =='Intern Door' or option == 'Intern ID':
        program_name = st.text_input("Intern's Program Name:")

col1, col2 = st.columns(2)

with col1:
    # Get the ID of the Google Drive folder to upload the images to
    upload_folder_id = st.text_input("ID of the Google Drive folder to upload the pdf to:")

with col2:
    # Get the ID of the Google Drive folder containing the images
    images_folder_id = st.text_input("ID of the Google Drive folder containing the photos:")

# File upload widget
uploaded_file = st.file_uploader(label="Upload a CSV file for processing", type=['csv'])

# Configure the Shotstack API
configuration = shotstack.Configuration(host = "https://api.shotstack.io/v1")
configuration.api_key['DeveloperKey'] = "ymfTz2fdKw58Oog3dxg5haeUtTOMDfXH4Qp9zlx2"

if uploaded_file is not None and program_name and upload_folder_id and images_folder_id:
    # Load the CSV file into a dataframe
    dataframe = pd.read_csv(uploaded_file)

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

    # Get the list of images in the Google Drive folder
    images_request = drive_service.files().list(q=f"'{images_folder_id}' in parents").execute()
    images_files = images_request.get('files', [])

    # Map each image to the person's name
    images_map = {}
    for image_file in images_files:
        image_name = os.path.splitext(image_file['name'].replace('_', ' '))[0]
        images_map[image_name] = image_file['id']

    # Create API client
    with shotstack.ApiClient(configuration) as api_client:
        api_instance = edit_api.EditApi(api_client)

        progress_report = st.empty()
        i = 1
        image_paths = []
        # Loop over the rows of the dataframe
        for _, row in dataframe.iterrows():

            # Find the image for this person
            image_id = None
            for name in images_map:
                if row['name'] in name:
                    image_id = images_map[name]
                    break

            if image_id is None:
                st.write(f"No image found for {row['name']}")
                continue

            # Get the image file from Google Drive
            image_file = drive_service.files().get(fileId=image_id, fields='webContentLink').execute()
            image_url = image_file['webContentLink']

            local_image_path = f"{row['name']}.jpg"
            response = requests.get(image_url, stream=True)

            if response.status_code == 200:
                # Open the image from the response content
                if 'png' in image_url or 'heic' in image_url:
                    if 'png' in image_url:
                        img = Image.open(BytesIO(response.content))
                    else:  # 'heic' in image_url
                        heif_file = pyheif.read(BytesIO(response.content))
                        img = Image.frombytes(
                            heif_file.mode, 
                            heif_file.size, 
                            heif_file.data,
                            "raw",
                            heif_file.mode,
                            heif_file.stride,
                        )
                    rgb_im = img.convert('RGB')
                    rgb_im.save(local_image_path, format='JPEG')
                else:
                    img = Image.open(BytesIO(response.content))
                    # Calculate new width while keeping aspect ratio
                    width = int(img.width * (176 / img.height))
                    # Resize the image
                    img = img.resize((width, 176))
                    # Save the image locally
                    img.save(local_image_path)
                    
            # Upload the image file to S3
            image_path_in_s3 = f"images/{row['name']}.jpg"
            s3_client.upload_file(local_image_path, bucket_name, image_path_in_s3)

            # Delete the local image file
            os.remove(local_image_path)

            # Create the URL for the image file in S3
            image_url_s3 = f"https://{bucket_name}.s3.amazonaws.com/{image_path_in_s3}"
            
            print(image_url_s3)
            # Create the merge fields for this row
            merge_fields = [
                MergeField(find="name", replace=row['name']),
                MergeField(find="role", replace=row['role']),
                MergeField(find="location", replace=row['location']),
                MergeField(find="image", replace=image_url_s3), 
                MergeField(find="program", replace=program_name),

            ]

            # Check if 'class' column exists in the dataframe
            if 'class' in dataframe.columns:
                # Add class field if exists in dataframe. If it's NaN, replace with empty string
                class_value = row['class'] if pd.notna(row['class']) else ""
                merge_fields.append(MergeField(find="class", replace=str(class_value)))


            if st.session_state['intern'] == False:
                template_id = "de584bc7-7e4c-423a-9350-e938af11494f"
            else:
                template_id = "99815af5-fa50-4360-a5be-cca5ae2f5ea2"

            # Create the template render object
            template = TemplateRender(
                id = template_id,
                merge = merge_fields
            )

            try:
                # Post the template render
                api_response = api_instance.post_template_render(template)

                # Display the message
                message = api_response['response']['message']
                id = api_response['response']['id']
                st.write(f"{message}")

                # Poll the API until the video is ready
                status = 'queued'
                while status == 'queued':
                    time.sleep(6)
                    status_response = api_instance.get_render(id)
                    status = status_response.response.status

                # Construct the image URL
                image_url = f"https://cdn.shotstack.io/au/v1/yn3e0zspth/{id}.jpg"

                # Download the image and save it to the 'Images' directory
                image_response = requests.get(image_url, stream=True)
                image_response.raise_for_status()  # Raise an exception in case of HTTP errors
                with open(f'Images/{row["name"]}.jpg', 'wb') as file:
                    for chunk in image_response.iter_content(chunk_size=8192):
                        file.write(chunk)

                # Add the local path to the image to the list of image paths
                image_paths.append(f'Images/{row["name"]}.jpg')


                print(image_url)

                # Download the image and save it to the 'Images' directory
                image_data = requests.get(image_url).content
                with open(f'Images/{row["name"]}.jpg', 'wb') as handler:
                    handler.write(image_data)

            except Exception as e:
                st.write(f"Unable to resolve API call: {e}")

            progress_report.text(f"Image progress: {i}/{len(dataframe)}")
            i+=1

        if st.session_state["id"]:
            create_pdf_id(image_paths)
        else:
            # Create a PDF from the downloaded images
            create_pdf_door(image_paths)

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

        st.write(f"PDF has been uploaded with file ID: {file.get('id')}")

st.subheader("Video Intro Generator")

col1, col2 = st.columns(2)

with col1:
    # Get the ID of the Google Drive folder to upload the videos to
    folder_id = st.text_input("ID of the Google Drive folder to upload the videos to:")

with col2:
    # Text input for the program name
    program = st.text_input("Enter the Program Name:")

# File upload widget
uploaded= st.file_uploader(label="Upload a CSV file", type=['csv'])

# Configure the Shotstack API
configuration = shotstack.Configuration(host = "https://api.shotstack.io/v1")
configuration.api_key['DeveloperKey'] = "ymfTz2fdKw58Oog3dxg5haeUtTOMDfXH4Qp9zlx2"

if uploaded is not None and program:
    # Load the CSV file into a dataframe
    dataframe = pd.read_csv(uploaded)

    # Create API client
    with shotstack.ApiClient(configuration) as api_client:
        api_instance = edit_api.EditApi(api_client)

        progress_report = st.empty()
        i = 1
        # Loop over the rows of the dataframe
        for _, row in dataframe.iterrows():

            # Create the merge fields for this row
            merge_fields = [
                MergeField(find="program_name", replace=program),
                MergeField(find="name", replace=row['name']),
                MergeField(find="school", replace=row['school']),
                MergeField(find="location", replace=row['location']),
                MergeField(find="class", replace=str(row['class']))
            ]

            # Create the template render object
            template = TemplateRender(
                id = "775d5f85-71f6-4e47-9e10-6c9eb0c0f477",
                merge = merge_fields
            )

            try:
                # Post the template render
                api_response = api_instance.post_template_render(template)

                # Display the message
                message = api_response['response']['message']
                id = api_response['response']['id']
                st.write(f"{message}")

                # Poll the API until the video is ready
                status = 'queued'
                while status == 'queued':
                    time.sleep(6)
                    status_response = api_instance.get_render(id)
                    status = status_response.response.status
                
                # Construct the video URL
                video_url = f"https://cdn.shotstack.io/au/v1/yn3e0zspth/{id}.mp4"

                print(video_url)
                # Download the video and save locally
                video_response = requests.get(video_url)


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

                # Loop over the video files
                for filename in os.listdir('Videos'):
                    # Create a media file upload object
                    media = MediaFileUpload(os.path.join('Videos', filename), mimetype='video/mp4')

                    # Create the file on Google Drive
                    request = drive_service.files().create(
                        media_body=media,
                        body={
                            'name': filename,
                            'parents': [folder_id]
                        }
                    )

                    # Execute the request
                    file = request.execute()

                    # Print the ID of the uploaded file
                    st.write('File ID: %s' % file.get('id'))

            except Exception as e:
                st.write(f"Unable to resolve API call: {e}")

            progress_report.text(f"Video progress: {i}/{len(dataframe)}")
            i+=1