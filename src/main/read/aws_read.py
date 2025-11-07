from resources.dev import config
from src.main.move.move_files import MoveFiles
from src.main.utility.logging_config import logger

class S3Reader:

    def list_files(self, s3_client, bucket_name, folder_path):
        try:
            # Listing all objects from the specified bucket and folder path
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
            #logger.info(response)

            # Check if there are files present under 'Contents' key of response dictionary
            if 'Contents' not in response:
                logger.info(f"No files present in {bucket_name}/{folder_path}")
                return []

            logger.info(f"Looking for Files in {bucket_name}/{folder_path}")

            # Comprehension
            files = [f"s3://{bucket_name}/{obj.get('Key', '')}" for obj in response.get('Contents', [])
                     if obj.get('Key', '').endswith('csv')]                            # Filtered to only create a list of CSV files

                     # Here we did not use response['Contents'] instead used response.get('Contents', [])
                     # get() is a safe dictionary lookup.
                     # response['Contents'] would cause a KeyError if "Contents" doesn’t exist.
                     # response.get('Contents', []) returns the value if it exists, otherwise returns the default value ([] here). Safe behavior.

            # full version (for reference)
            # for obj in response.get('Contents', []):
            #     if obj.get('Key', '').endswith('csv'):
            #         files.append(f"s3://{bucket_name}/{obj['Key']}")

            corrupt_files = [f"s3://{bucket_name}/{obj.get('Key', '')}" for obj in response.get('Contents', [])
                             if not obj.get('Key', '').endswith('csv') and not obj.get('Key', '').endswith('/')]
            # Without this "and not" condition it will take all keys
            # including the ones that are just folder paths like this sales_data/2025_11_01/, sales_data/ along with file paths in the list

            mover = MoveFiles(s3_client)
            mover.move_s3_to_s3(bucket_name, folder_path, config.mismatched_filetype_path, corrupt_files)

            if not files:
                logger.warning(f"No CSV files present in {bucket_name}/{folder_path}")
            return files

        except Exception as err:
            logger.error("Got this error : %s", err)
            raise err