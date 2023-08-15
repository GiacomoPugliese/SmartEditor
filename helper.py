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
    
