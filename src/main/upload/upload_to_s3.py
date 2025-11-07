from resources.dev import config
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.logging_config import *
import datetime
import os

from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.staging_table import add_to_staging_table

s3_client_provider = S3ClientProvider(decrypt(config.aws_access_key), decrypt(config.aws_secret_key))
s3_client = s3_client_provider.get_client()

class UploadToS3:
    def __init__(self,s3_client):
        self.s3_client = s3_client

    def upload_to_s3(self,s3_bucket,s3_prefix, local_file_path):
        current_date = datetime.datetime.now().strftime("%Y_%m_%d")
        s3_upload_prefix = f"{s3_prefix}{current_date}"
        try:
            for root, dirs, files in os.walk(local_file_path):                      # os.walk walks through all subfolders and files in the given local directory.
                for file_name in files:
                    file = file_name.strip()
                    local_file_path = os.path.join(root, file_name)
                    s3_key = f"{s3_upload_prefix}/{file_name}"
                    self.s3_client.upload_file(local_file_path, s3_bucket, s3_key)
                    print(f"Uploaded {file_name}")
                    add_to_staging_table(s3_bucket, s3_upload_prefix, file)
            return f"Data Successfully uploaded in {s3_bucket}/{s3_prefix}"
        except Exception as e:
            logger.error(f"Error uploading file : {str(e)}")
            raise e

s3_prefix = config.s3_source_s3prefix
s3_bucket = config.bucket_name
local_path = config.from_local_to_s3

uploader = UploadToS3(s3_client)
result = uploader.upload_to_s3(s3_bucket,s3_prefix,local_path)

