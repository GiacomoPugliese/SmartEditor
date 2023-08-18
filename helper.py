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

import boto3
from moviepy.editor import VideoFileClip
from botocore.exceptions import NoCredentialsError
from retry import retry
import time
from googleapiclient.errors import ResumableUploadError
import ssl

def wait_for_s3_object(s3, bucket, key, local_filepath):
    """
    Wait for an S3 object to exist and have the same size as the local file.
    """
    local_filesize = os.path.getsize(local_filepath)
    while True:
        try:
            response = s3.head_object(Bucket=bucket, Key=key)
            if 'ContentLength' in response and response['ContentLength'] == local_filesize:
                return True
        except Exception as e:
            pass
        ("waited")
        time.sleep(1)
        
def concatenate_videos_aws(intro_resized_filename, main_filename, outro_resized_filename, output_filename):
    # Set AWS details (replace with your own details)
    AWS_REGION_NAME = 'us-east-2'
    AWS_ACCESS_KEY = 'AKIARK3QQWNWXGIGOFOH'
    AWS_SECRET_KEY = 'ClAUaloRIp3ebj9atw07u/o3joULLY41ghDiDc2a'
    BUCKET_NAME = 'li-general-tasks'
    S3_INPUT_PREFIX = 'input_videos/'
    S3_OUTPUT_PREFIX = 'output_videos/'

    # Create a unique identifier based on the main file name
    unique_id = os.path.basename(main_filename).rsplit('.', 1)[0]

    # Initialize the S3 client
    s3 = boto3.client('s3',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # Initialize boto3 client for AWS MediaConvert
    client = boto3.client('mediaconvert',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url='https://fkuulejsc.mediaconvert.us-east-2.amazonaws.com'
    )

    def get_end_timecode(filename):
        with VideoFileClip(filename) as clip:
            duration = int(clip.duration)
            return f'00:{duration//60:02d}:{duration%60:02d}:00'

    def wait_for_job_completion(client, job_id):
        while True:
            response = client.get_job(Id=job_id)
            status = response['Job']['Status']
            if status in ['COMPLETE', 'ERROR', 'CANCELED']:
                return status
            time.sleep(5)

    # Upload videos to S3 with unique names
    s3.upload_file(intro_resized_filename, BUCKET_NAME, S3_INPUT_PREFIX + f'{unique_id}_intro.mp4')
    wait_for_s3_object(s3, BUCKET_NAME, S3_INPUT_PREFIX + f'{unique_id}_intro.mp4', intro_resized_filename)

    s3.upload_file(main_filename, BUCKET_NAME, S3_INPUT_PREFIX + f'{unique_id}_main.mp4')
    wait_for_s3_object(s3, BUCKET_NAME, S3_INPUT_PREFIX + f'{unique_id}_main.mp4', main_filename)

    s3.upload_file(outro_resized_filename, BUCKET_NAME, S3_INPUT_PREFIX + f'{unique_id}_outro.mp4')
    wait_for_s3_object(s3, BUCKET_NAME, S3_INPUT_PREFIX + f'{unique_id}_outro.mp4', outro_resized_filename)

    # Define the input files and their roles
    inputs = [
        {
            'FileInput': f's3://{BUCKET_NAME}/{S3_INPUT_PREFIX}{unique_id}_intro.mp4',
            'AudioSelectors': {
                'Audio Selector 1': {
                    'DefaultSelection': 'DEFAULT',
                    'SelectorType': 'TRACK',
                    'Offset': 0,
                    'ProgramSelection': 1,
                }
            }
        },
        {
            'FileInput': f's3://{BUCKET_NAME}/{S3_INPUT_PREFIX}{unique_id}_main.mp4',
            'AudioSelectors': {
                'Audio Selector 2': {
                    'DefaultSelection': 'DEFAULT',
                    'SelectorType': 'TRACK',
                    'Offset': 0,
                    'ProgramSelection': 1,
                }
            }
        },
        {
            'FileInput': f's3://{BUCKET_NAME}/{S3_INPUT_PREFIX}{unique_id}_outro.mp4',
            'AudioSelectors': {
                'Audio Selector 3': {
                    'DefaultSelection': 'DEFAULT',
                    'SelectorType': 'TRACK',
                    'Offset': 0,
                    'ProgramSelection': 1,
                }
            }
        }
    ]

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
                    'CodecLevel': 'AUTO',
                    'RateControlMode': 'QVBR',
                    'MaxBitrate': 5000000  # Example value, adjust as needed
                }
            }
        },
        'AudioDescriptions': [{
            'AudioSourceName': 'Audio Selector 1', # Specify the name of the audio selector
            'CodecSettings': {
                'Codec': 'AAC',
                'AacSettings': {
                    'AudioDescriptionBroadcasterMix': 'NORMAL',
                    'RateControlMode': 'CBR',
                    'CodecProfile': 'LC',
                    'CodingMode': 'CODING_MODE_2_0',
                    'SampleRate': 48000,
                    'Bitrate': 96000
                }
            }
        }]
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
                    'Destination': f's3://{BUCKET_NAME}/{S3_OUTPUT_PREFIX}{output_filename.rsplit(".", 1)[0]}'
                }
            }
        }]
    }


    try:
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Submit the job
                response = client.create_job(Role='arn:aws:iam::092040901485:role/media_convert', Settings=job_settings)
                job_id = response['Job']['Id']
                print('Job created:', job_id)

                # Wait for the job to finish
                job_status = wait_for_job_completion(client, job_id)
                if job_status == 'COMPLETE':
                    print('Job completed successfully.')
                    break
                elif job_status == 'ERROR':
                    response = client.get_job(Id=job_id)
                    error_message = response['Job'].get('ErrorMessage', 'No error message provided.')
                    print('Job failed with error:', error_message)
                    retry_count += 1
                    print(f"Retrying job submission... (Attempt {retry_count}/{max_retries})")
                else:
                    print(f'Job failed with status: {job_status}')
                    return
            except Exception as e:
                print('Error:', e)
                retry_count += 1
                print(f"Retrying job submission... (Attempt {retry_count}/{max_retries})")

        if retry_count == max_retries:
            print('Maximum number of retries reached. Job submission failed.')
            return


        # Use a waiter to wait for the object to be available in the bucket
        waiter = s3.get_waiter('object_exists')
        waiter.wait(Bucket=BUCKET_NAME, Key=S3_OUTPUT_PREFIX + output_filename)
        s3.download_file(BUCKET_NAME, S3_OUTPUT_PREFIX + output_filename, "Videos/" + output_filename)


    except Exception as e:
        print('Error:', e)

    return

@retry((ssl.SSLEOFError, ResumableUploadError), tries=5, delay=2, backoff=2)
def download_video(file_id, filename, service):
    try:
        request = service.files().get_media(fileId=file_id)
        with open(filename, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
    except:
        print("retried!")
    
@retry((ssl.SSLEOFError, ResumableUploadError), tries=5, delay=2, backoff=2)
def upload_video(filename, folder_id, service):
    file_metadata = {'name': os.path.basename(filename), 'parents': [folder_id]}
    media = MediaFileUpload(filename, mimetype='video/mp4', resumable=True)
    try:
        return service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    except (ssl.SSLEOFError, ResumableUploadError) as e:
        print(f"{e.__class__.__name__} encountered, retrying...")
        raise

def process_video(data):
    row_number, row, videos_directory, creds_dict, stitch_folder = data


    creds = Credentials.from_authorized_user_info(creds_dict)
    service = build('drive', 'v3', credentials=creds)

    # Download main video
    main_file_id = row['main'].split("/file/d/")[1].split("/view")[0]
    main_filename = os.path.join(videos_directory, f"{row['name']}_main.mp4")
    download_video(main_file_id, main_filename, service)

    # Download intro video
    intro_file_id = row['intro'].split("/file/d/")[1].split("/view")[0]
    intro_filename = os.path.join(videos_directory, f"{row['name']}_intro.mp4")
    download_video(intro_file_id, intro_filename, service)

    # Concatenate video clips
    output_filename = f"{row['name']}_final.mp4"
    concatenate_videos_aws(intro_filename, main_filename,"outro_li.mp4", output_filename)

    # Upload stitched video to Google Drive
    upload_video("Videos/" + output_filename, stitch_folder, service)

    del service

    # Optionally remove temporary files
    os.remove(intro_filename)
    os.remove(main_filename)
    os.remove("Videos/" + output_filename)

    return row['name']