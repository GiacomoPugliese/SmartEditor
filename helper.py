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
from concurrent.futures import ProcessPoolExecutor, as_completed
import moviepy.editor as mp
from moviepy.editor import VideoFileClip, concatenate_videoclips
import os
import subprocess
# import pyheif

# create s3 client
s3_client = boto3.client('s3', 
                         aws_access_key_id='AKIARK3QQWNWXGIGOFOH',
                         aws_secret_access_key='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')
bucket_name = 'li-general-tasks'


def process_row(args):
    try:

        row, images_map, drive_service, dataframe, program_name, intern_status = args

        print(row)
        # Configure the Shotstack API
        configuration = shotstack.Configuration(host="https://api.shotstack.io/v1")
        configuration.api_key['DeveloperKey'] = "ymfTz2fdKw58Oog3dxg5haeUtTOMDfXH4Qp9zlx2"

        # Initialize the image_id
        image_id = None

        for name in images_map:
            if row['name'] in name:
                image_id = images_map[name]
                break

        if image_id is None:
            print(f"No image found for {row['name']}")
            return

        # Get the image file from Google Drive
        image_file = drive_service.files().get(fileId=image_id, fields='webContentLink').execute()
        image_url = image_file['webContentLink']

        # Generate a unique local image path
        local_image_name = f"{row['name']}-{str(uuid.uuid4())}.jpg"
        local_image_path = f"{local_image_name}"

        response = requests.get(image_url, stream=True)

        # Create API client
        with shotstack.ApiClient(configuration) as api_client:
            api_instance = edit_api.EditApi(api_client)

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
            image_path_in_s3 = f"images/{local_image_name}"
            s3_client.upload_file(local_image_path, bucket_name, image_path_in_s3)

            # Create the URL for the image file in S3
            image_url_s3 = f"https://{bucket_name}.s3.amazonaws.com/{image_path_in_s3}"

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

            if intern_status == False:
                template_id = "de584bc7-7e4c-423a-9350-e938af11494f"
            else:
                template_id = "99815af5-fa50-4360-a5be-cca5ae2f5ea2"

            # Create the template render object
            template = TemplateRender(
                id=template_id,
                merge=merge_fields
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
                while status != 'done':
                    time.sleep(0.5)
                    status_response = api_instance.get_render(id)
                    status = status_response.response.status

                # Construct the image URL
                image_url = f"https://cdn.shotstack.io/au/v1/yn3e0zspth/{id}.jpg"

                # Generate a unique local image name for Shotstack
                local_shotstack_image_name = f"{row['name']}-{str(uuid.uuid4())}.jpg"

                # Download the image and save it to the 'Images' directory
                image_response = requests.get(image_url, stream=True)
  
                with open(f'Images/{local_shotstack_image_name}', 'wb') as file:
                    for chunk in image_response.iter_content(chunk_size=8192):
                        file.write(chunk)

                print(image_url)

                # Download the image and save it to the 'Images' directory
                image_data = requests.get(image_url).content
                with open(f'Images/{local_shotstack_image_name}', 'wb') as handler:
                    handler.write(image_data)

                

            except Exception as e:
                print(f"Unable to resolve API call: {e}")
                # Delete the local image file\
                try:
                    os.remove(local_image_path)
                except:
                    pass
                return image_url

        try:
            os.remove(local_image_path)
        except:
            pass
        return f'Images/{local_shotstack_image_name}'
    
    except Exception as e:
        print(e)
        try:
            os.remove(local_image_path)
        except:
            pass
        return None
    
import os
import subprocess
from moviepy.editor import VideoFileClip
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2.credentials import Credentials
import os
import boto3
from botocore.exceptions import NoCredentialsError

def resize_video(input_file, output_file, target_resolution):
    clip = VideoFileClip(input_file)
    clip.resize(target_resolution).write_videofile(output_file, codec='libx264', audio_codec='aac')

def concatenate_videos_aws(intro_resized_filename, main_filename, outro_resized_filename, output_filename):
    # Set AWS details (replace with your own details)
    AWS_REGION_NAME = 'us-east-2'
    AWS_ACCESS_KEY = 'AKIARK3QQWNWXGIGOFOH'
    AWS_SECRET_KEY = 'ClAUaloRIp3ebj9atw07u/o3joULLY41ghDiDc2a'

    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # Initialize boto3 client for AWS Rekognition
    client = boto3.client('mediaconvert',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    
    def get_end_timecode(filename):
        with VideoFileClip(filename) as clip:
            duration = int(clip.duration)
            return f'00:{duration//60:02d}:{duration%60:02d}:00'
    
    # Define the input files and their roles
    inputs = [
        {'FileInput': intro_resized_filename, 'InputClippings': [{'StartTimecode': '00:00:00:00', 'EndTimecode': get_end_timecode(intro_resized_filename)}]},
        {'FileInput': main_filename},
        {'FileInput': outro_resized_filename, 'InputClippings': [{'StartTimecode': '00:00:00:00', 'EndTimecode': get_end_timecode(outro_resized_filename)}]}
    ]
    
    # Define the output file settings
    output = {
        'Extension': 'mp4',
        'ContainerSettings': {
            'Container': 'MP4',
            'Mp4Settings': {
                'CslgAtom': 'INCLUDE',
                'FreeSpaceBox': 'EXCLUDE',
                'MoovPlacement': 'PROGRESSIVE_DOWNLOAD'
            }
        },
        'VideoDescription': {
            'CodecSettings': {
                'Codec': 'H_264',
                'H264Settings': {
                    'CodecProfile': 'MAIN',
                    'CodecLevel': 'AUTO'
                }
            }
        }

    }
    
    # Create the job settings
    job_settings = {
        'Inputs': inputs,
        'OutputGroups': [{
            'Name': 'File Group',
            'Outputs': [output],
            'OutputGroupSettings': {
                'Type': 'FILE_GROUP_SETTINGS',
                'FileGroupSettings': {
                    'Destination': output_filename
                }
            }
        }]
    }
    
    # Submit the job
    try:
        response = client.create_job(Role='arn:aws:iam::123456789012:role/MediaConvert_Default_Role', Settings=job_settings)
        print('Job created:', response['Job']['Id'])
    except NoCredentialsError:
        print('Credentials not available')

def download_video(file_id, filename, service):
    request = service.files().get_media(fileId=file_id)
    with open(filename, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

def upload_video(filename, folder_id, service):
    file_metadata = {'name': os.path.basename(filename), 'parents': [folder_id]}
    media = MediaFileUpload(filename, mimetype='video/mp4')
    return service.files().create(body=file_metadata, media_body=media, fields='id').execute()

def process_video(data):
    row, videos_directory, creds_dict, stitch_folder = data

    creds = Credentials.from_authorized_user_info(creds_dict)
    service = build('drive', 'v3', credentials=creds)

    # Download intro video
    intro_file_id = row['intro'].split("/file/d/")[1].split("/view")[0]
    intro_filename = os.path.join(videos_directory, f"{row['name']}_intro.mp4")
    download_video(intro_file_id, intro_filename, service)

    # Download main video
    main_file_id = row['main'].split("/file/d/")[1].split("/view")[0]
    main_filename = os.path.join(videos_directory, f"{row['name']}_main.mp4")
    download_video(main_file_id, main_filename, service)

    # Get the resolution of the main video
    main_clip = VideoFileClip(main_filename)
    target_resolution = main_clip.size

    # Resize intro and outro videos
    intro_resized_filename = os.path.join(videos_directory, f"{row['name']}_intro_resized.mp4")
    outro_resized_filename = os.path.join(videos_directory, f"{row['name']}_outro_resized.mp4")
    resize_video(intro_filename, intro_resized_filename, target_resolution)
    resize_video("outro_li.mp4", outro_resized_filename, target_resolution)

    # Concatenate video clips
    output_filename = os.path.join(videos_directory, f"{row['name']}_final.mp4")
    concatenate_videos_aws(intro_resized_filename, main_filename, outro_resized_filename, output_filename)

    # Upload stitched video to Google Drive
    upload_video(output_filename, stitch_folder, service)

    # Optionally remove temporary files
    os.remove(intro_resized_filename)
    os.remove(outro_resized_filename)

    return row['name']