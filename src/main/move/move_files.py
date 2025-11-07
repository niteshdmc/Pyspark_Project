import os
from src.main.utility.logging_config import *


class MoveFiles:
    def __init__(self, s3_client):
        self.s3_client = s3_client

    def move_s3_to_s3(self, bucket_name, source_prefix, destination_prefix, corrupt_files_paths):
        try:
                                                                    # Absolute_path = s3://relmart-sales-project/sales_data/2025_11_01/Customers.csv
            for path in corrupt_files_paths:
                filename = os.path.basename(path)
                source_key = path.split(f"{bucket_name}/")[1]           #'sales_data/2025_10_30/Customers.csv'
                destination_key = path.split(f"s3://{bucket_name}/")[1].split(f"{filename}")[0] + destination_prefix + filename

                # Destination_key = sales_data/2025_10_30/ + schema_check/schema_mismatched_csv/ + Customers.csv
                # we split Absolute_path = s3://relmart-sales-project/sales_data/2025_11_01/Customers.csv
                # using split. split(f"{bucket_name}/")[1] we make it : ['', 'sales_data/2025_11_01/Customers.csv']
                # Then we choose index [1] : 'sales_data/2025_11_01/Customers.csv' and split it again by filename
                # which makes it : ['sales_data/2025_11_01/', ''] and then we choose index [0]




                self.s3_client.copy_object(Bucket=bucket_name,
                                      CopySource={'Bucket': bucket_name,
                                                  'Key': source_key}, Key=destination_key)

                self.s3_client.delete_object(Bucket=bucket_name, Key=source_key)
            return f"Data Moved successfully from {source_prefix} to {destination_prefix}"
        except Exception as err:
            logger.error(f"Error moving file : {str(err)}")
            raise err