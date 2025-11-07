import boto3
import traceback
import os
from src.main.utility.logging_config import *

class S3FileDownloader:
    def __init__(self,s3_client, bucket_name, local_directory):
        self.bucket_name = bucket_name
        self.local_directory = local_directory
        self.s3_client = s3_client

    def download_files(self, file_paths, file_names):
        for file_path, name in zip(file_paths, file_names):                             # zip() iterates both lists at the same time and matches items index-by-index
            logger.info(f"Running download files for file path: {file_path}")
            logger.info(f"Downloading...{name}")
            key = file_path
            download_file_path = os.path.join(self.local_directory, name)
            try:
                self.s3_client.download_file(self.bucket_name, key, download_file_path)
            except Exception as e:
                error_message = f"Error downloading file '{key}': {str(e)}"
                traceback_message = traceback.format_exc()
                print(error_message)
                print(traceback_message)
                raise e

