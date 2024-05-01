# Import necessary libraries
import streamlit as st
import pandas as pd
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
from helper import generate_images, generate_pdf
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import os
import subprocess
import re
# import pyheif


st.set_page_config(
    page_title='ImageEditor',
    page_icon='🖼️'
)   
hide_streamlit_style = """ <style> #MainMenu {visibility: hidden;} footer {visibility: hidden;} </style> """ 
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

def extract_id_from_url(url):
    match = re.search(r'(?<=folders/)[a-zA-Z0-9_-]+', url)
    if match:
        return match.group(0)
    match = re.search(r'(?<=spreadsheets/d/)[a-zA-Z0-9_-]+', url)
    if match:
        return match.group(0)
    match = re.search(r'(?<=presentation/d/)[a-zA-Z0-9_-]+', url)
    if match:
        return match.group(0)
    return None

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
                         aws_secret_access_key='ClAUaloRIp3ebj9atw07u/o3joULLY41ghDiDc2a')

bucket_name = 'li-general-task'

def reset_s3():
    # Set AWS details (replace with your own details).
    AWS_REGION_NAME = 'us-east-2'
    AWS_ACCESS_KEY = 'AKIARK3QQWNWXGIGOFOH'
    AWS_SECRET_KEY = 'ClAUaloRIp3ebj9atw07u/o3joULLY41ghDiDc2a'

    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # Delete objects within subdirectories in the bucket 'li-general-task'
    subdirs = ['input_videos/', 'output_videos/', 'images/']
    for subdir in subdirs:
        objects = s3.list_objects_v2(Bucket='li-general-task', Prefix=subdir)
        for obj in objects.get('Contents', []):
            if obj['Key'] != 'input_videos/outro.mp4':
                s3.delete_object(Bucket='li-general-task', Key=obj['Key'])
                
        # Add a placeholder object to represent the "directory"
        s3.put_object(Bucket='li-general-task', Key=subdir)

def set_link_sharing(file_id, drive_service):
    try:
        # Create the new permission
        permission = {
            'type': 'anyone',
            'role': 'reader'
        }
        # Set the new permission
        drive_service.permissions().create(fileId=file_id, body=permission).execute()
        return True
    except:
        return False

try:
    
    if 'restart' not in st.session_state:
        reset_s3()
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
st.title("LI Image Editor")
st.caption("By Giacomo Pugliese")

with st.expander("Click to view full directions for this site"):
    st.subheader("Google Authentication")
    st.write("- Click 'Authenticate Google Account', and then on the generated link.")
    st.write("- Follow the steps of Google login until you get to the final page.")
    st.write("- Click on 'Finalize Authentication' to proceed to rest of website.")
    st.subheader("IDs and Doortags")
    st.write("- Select which type of template you want to make.")
    st.write("- Design and upload the link for a template in Google slides for the ID/Doortags with place holder text PRECISELY titled 'name', 'program', 'role' (high school name for interns, job description for staff), 'location', and 'class' (you can omit class column if using a staff template). ")
    st.write("- Upload a csv with columns named containing the placeholder text from the slides template.")
    st.write("- Enter the link of the Google drive folder containing photos that correspond exactly to the rows within the 'name' column.")
    st.write("- Enter the intended output Google drive folder link.")
    st.write("- Click 'Process Tags' to begin renderings of the chosen template and view the pdf containing them in your destination Google drive folder.")
    st.subheader("Image Generation from a Template")
    st.write("- Enter the intended output Google drive folder link.")
    st.write("- Design and upload the link for a template in Google slides with place holder text for your desired merge fields.")
    st.write("- Upload a Google Sheets URL with columns PRECISELY titled whatever you want your merge fields to be, as well as a column named 'link' where you want the image links to be inserted.")
    st.write("- Click 'Generate Images' to begin the image generation and view them in your destination Google drive folder.")

st.header("Google Authentication")

try:
    if st.button("Authenticate Google Account"):
        st.session_state['begin_auth'] = True
        # Request OAuth URL from the FastAPI backend
        response = requests.get(f"{'https://leadership-initiatives-0c372bea22f2.herokuapp.com'}/auth?user_id={'intros'}")
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
                    response = requests.get(f"{'https://leadership-initiatives-0c372bea22f2.herokuapp.com'}/token/{'intros'}")
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


st.header("IDs and Doortags")

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
    slides_temp = st.text_input("Google Slides Template URL")

col1, col2 = st.columns(2)

with col1:
    # Get the ID of the Google Drive folder to upload the images to
    upload_folder_id = st.text_input("URL of the Google Drive folder to upload the pdf to:")

with col2:
    # Get the ID of the Google Drive folder containing the images
    images_folder_id = st.text_input("URL of the Google Drive folder containing the photos:")

# File upload widget
uploaded_file = st.file_uploader(label="Upload a CSV file for processing", type=['csv'])


label_button = st.button("Process Tags")

if uploaded_file is not None and slides_temp and upload_folder_id and images_folder_id and label_button and st.session_state['final_auth']:
    with st.spinner("Processing tags (may take a few minutes)..."):
        slides_temp = extract_id_from_url(slides_temp)
        upload_folder_id = extract_id_from_url(upload_folder_id)
        images_folder_id = extract_id_from_url(images_folder_id)


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

        # Build the Google Drive service
        drive_service = build('drive', 'v3', credentials=creds)

        # Set sharing permissions for slides_temp, upload_folder_id, and images_folder_id
        for file_id in [slides_temp, upload_folder_id, images_folder_id]:
            set_link_sharing(file_id, drive_service)

        # Get the list of images in the Google Drive folder
        images_request = drive_service.files().list(q=f"'{images_folder_id}' in parents").execute()
        images_files = images_request.get('files', [])

        # Map each image to the person's name
        images_map = {}
        for image_file in images_files:
            image_name = os.path.splitext(image_file['name'].replace('_', ' '))[0]
            images_map[image_name] = image_file['id']

        image_paths = []

        generate_pdf(slides_temp, images_folder_id, upload_folder_id, uploaded_file)

def read_google_sheet_to_df(sheet_id):
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

    # Initialize the Sheets API client
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    # Read the sheet into a DataFrame
    result = sheet.values().get(spreadsheetId=sheet_id, range="Sheet1").execute()
    values = result.get('values', [])

    if not values:
        return pd.DataFrame()

    # Ensure each row has the same length as the header
    num_columns = len(values[0])
    for i in range(1, len(values)):
        len_row = len(values[i])
        if len_row < num_columns:
            values[i].extend([''] * (num_columns - len_row))

    # Create a DataFrame
    df = pd.DataFrame(values[1:], columns=values[0])
    return df
    
st.header("Image Generation from a Template")
col1, col2 = st.columns(2)
with col1:
    template_url = st.text_input("Google Slides Template URL:")

with col2:
    output_url = st.text_input("Output Google Drive folder URL:")

csv_url = st.text_input("Input data Google Sheets URL")


if st.button("Generate Images") and st.session_state['final_auth'] and template_url and csv_url:
    with st.spinner("Generating images (may take a few minutes)..."):
        csv_id = extract_id_from_url(csv_url)
        df = read_google_sheet_to_df(csv_id) 
        merge_fields = df.columns.tolist()
        template_id = extract_id_from_url(template_url)
        output_id = extract_id_from_url(output_url)
        print(generate_images(template_id, output_id, merge_fields, df, csv_id))
    st.success("Images successfully generated!")

    