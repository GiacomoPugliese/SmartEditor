import pandas as pd
import requests
from PIL import Image
from io import BytesIO
from shotstack_sdk.model.template_render import TemplateRender
from shotstack_sdk.model.merge_field import MergeField
import os
import time
from multiprocessing import Pool
import boto3

# create s3 client
s3_client = boto3.client('s3', 
                         aws_access_key_id='AKIARK3QQWNWXGIGOFOH',
                         aws_secret_access_key='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')

bucket_name = 'li-general-tasks'

def process_row(args):
    row, images_map, drive_service, bucket_name, program_name, is_intern, is_id = args
    image_paths = []
    
    # Find the image for this person
    image_id = None
    for name in images_map:
        if row['name'] in name:
            image_id = images_map[name]
            break

    if image_id is None:
        print(f"No image found for {row['name']}")

    # Get the image file from Google Drive
    image_file = drive_service.files().get(fileId=image_id, fields='webContentLink').execute()
    image_url = image_file['webContentLink']

    local_image_path = f"{row['name']}.jpg"
    response = requests.get(image_url, stream=True)

    if response.status_code == 200:
        # Open the image from the response content
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


    if is_intern == False:
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
        print(f"{message}")

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
        print(f"Unable to resolve API call: {e}")

    return f'Images/{row["name"]}.jpg'
