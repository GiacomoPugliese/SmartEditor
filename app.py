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
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import json


if 'restart' not in st.session_state:
    st.session_state['restart'] = True
    st.session_state['download'] = False
    st.session_state['creds'] = ""

if st.session_state['restart']:
    # Remove the 'Videos' directory if it exists
    if os.path.exists('Videos'):
        shutil.rmtree('Videos')
    st.session_state['restart'] = False
    # Create the 'Videos' directory
    os.makedirs('Videos')

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

# Get the ID of the Google Drive folder to upload the videos to
folder_id = st.text_input("Enter the ID of the Google Drive folder to upload the videos to:")

# Title of the app
st.title("Automatic Video and Image Editor")

# Text input for the program name
program_name = st.text_input("Enter the Program Name:")

# Introduction text
st.write("Please upload a CSV file for processing:")

# File upload widget
uploaded_file = st.file_uploader(label="Upload a CSV file", type=['csv'])

# Configure the Shotstack API
configuration = shotstack.Configuration(host = "https://api.shotstack.io/v1")
configuration.api_key['DeveloperKey'] = "ymfTz2fdKw58Oog3dxg5haeUtTOMDfXH4Qp9zlx2"

if uploaded_file is not None and program_name:
    # Load the CSV file into a dataframe
    dataframe = pd.read_csv(uploaded_file)

    # Create API client
    with shotstack.ApiClient(configuration) as api_client:
        api_instance = edit_api.EditApi(api_client)

        progress_report = st.empty()
        i = 1
        # Loop over the rows of the dataframe
        for _, row in dataframe.iterrows():

            # Create the merge fields for this row
            merge_fields = [
                MergeField(find="program_name", replace=program_name),
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

# try:
#     with open(f'Videos/video_{id}.mp4', 'wb') as f:
#         f.write(video_response.content)
#         st.write(f"Video has been saved locally.")
#         program_name = ""

#     with zipfile.ZipFile('Videos.zip', 'w') as zipf:
#         for file in os.listdir('Videos'):
#             zipf.write(os.path.join('Videos', file), arcname=file)
#         st.download_button(label="Download Videos", data=open('Videos.zip', 'rb'), file_name='Videos.zip', mime='application/zip')
# except:
#     pass

