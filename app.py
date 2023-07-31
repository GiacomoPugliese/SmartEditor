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
from helper import process_row
from concurrent.futures import ProcessPoolExecutor, as_completed
# import pyheif

hide_streamlit_style = """ <style> #MainMenu {visibility: hidden;} footer {visibility: hidden;} </style> """ 
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

def create_pdf_id(image_paths):
    time.sleep(3)
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

try:
    
    if 'restart' not in st.session_state:
        st.session_state['restart'] = True
        st.session_state['download'] = False
        st.session_state['creds'] = ""
        st.session_state['intern'] = True
        st.session_state['id'] = True
        st.session_state['begin_auth'] = False
        st.session_state['final_auth'] = False

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

except:
    pass

# Title of the app
st.title("Automatic Video and Image Editor")
st.caption("By Giacomo Pugliese")

with st.expander("Click to view full directions for this site"):
    st.subheader("IDs and Doortags")
    st.write("- Select which template you want to make, as well as the google drive folder ids for your photos and intended output destination.")
    st.write("- If using an intern template, also indicate which program the interns are in")
    st.write("- Upload a csv with columns PRECISELY titled 'name', 'role' (high school for interns, job description for staff), 'location', and 'class' (you can omit class column if using a staff template)")
    st.write("- Click 'Process Tags' to being renderings of the chosen template and view them in your destination google drive folder'")
    st.subheader("Video Intro Generator")
    st.write("- Enter the intended output google drive folder id, as well as the program name of the students")
    st.write("- Upload a csv with columns PRECISELY titled 'name', 'school', 'location', and 'class'")
    st.write("- Click 'Process Videos' to being intro video renderings and view them in your destination google drive folder'")

st.subheader("Google authentication")

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
    pass


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

label_button = st.button("Process Tags")

if uploaded_file is not None and program_name and upload_folder_id and images_folder_id and label_button and st.session_state['final_auth']:
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

    image_paths = []
    arguments = [(row, images_map, drive_service, dataframe, program_name, st.session_state['intern']) for _, row in dataframe.iterrows()]

    progress = st.empty()
    i = 1
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = []
        for arg in arguments:
            future = executor.submit(process_row, arg)
            futures.append(future)

        image_paths = []
        for future in as_completed(futures):
            result = future.result()
            image_paths.append(result)
            progress.text(f"File progress:  {i}/{len(dataframe)}")
            i += 1


    print(image_paths)
    removed_paths = []
    for path in image_paths:
        if path != None:
            removed_paths.append(path)
    image_paths = removed_paths
    print(image_paths)

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

    print(f"PDF has been uploaded with file ID: {file.get('id')}")
    st.success("PDF Creation Complete!")

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

video_button = st.button("Process Videos")

if uploaded is not None and program and video_button and st.session_state['final_auth']:
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
                MergeField(find="name", replace=row.get('name', row.get('name1', ''))),
                MergeField(find="school", replace=row.get('school', row.get('name2', ''))),
                MergeField(find="location", replace=row.get('location', row.get('name3', ''))),
                MergeField(find="class", replace='Class of ' + str(row['class']) if 'class' in row else row.get('name4', '')),
                MergeField(find="name5", replace=row.get('name5', '')),
                MergeField(find="name6", replace=row.get('name6', '')),
                MergeField(find="name7", replace=row.get('name7', '')),
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
                print(f"{message}")

                # Poll the API until the video is ready
                status = 'queued'
                while status == 'queued':
                    time.sleep(1)
                    status_response = api_instance.get_render(id)
                    status = status_response.response.status

                # Construct the video URL
                video_url = f"https://cdn.shotstack.io/au/v1/yn3e0zspth/{id}.mp4"

                print(video_url)
                # Download the video
                video_data = requests.get(video_url).content
                
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

                name = row.get('name', 'name1')
                video_file = f"{name}.mp4"
                time.sleep(6)

                with open(video_file, 'wb') as f:
                    f.write(video_data)

                # Create a media file upload object
                media = MediaFileUpload(video_file, mimetype='video/mp4')

                # Create the file on Google Drive
                request = drive_service.files().create(
                    media_body=media,
                    body={
                        'name': video_file,
                        'parents': [folder_id]
                    }
                )

                # Execute the request
                file = request.execute()

                # Print the ID of the uploaded file
                print('File ID: %s' % file.get('id'))

                time.sleep(3)
                Remove temporary file
                os.remove(video_file)
            except Exception as e:
                print(f"Unable to resolve API call: {e}")

            progress_report.text(f"Video progress: {i}/{len(dataframe)}")
            i+=1
    st.success("Videos successfully generated!")