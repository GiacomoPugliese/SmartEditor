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

import subprocess

from moviepy.editor import VideoFileClip, concatenate_videoclips
import os

# Load the clips
intro_clip = VideoFileClip('Videos/Sophia_intro_resized.mp4')
main_clip = VideoFileClip('Videos/Sophia_main.ts')
outro_clip = VideoFileClip('Videos/Sophia_outro_resized.ts')

# Concatenate the clips
final_clip = concatenate_videoclips([intro_clip, main_clip, outro_clip])

# Export the final video
final_clip.write_videofile('output.mp4', codec='libx264', audio_codec='aac')


