#Imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

#Static Variables
environment = "dev"

key = "relmart_project"
iv = "proj_rel_encrypt"
salt = "martproject_AesEncryption"

#AWS Access And Secret key
aws_access_key = "aws_access_key_id"
aws_secret_key = "aws_secret_access_key"

#AWS S3 Bucket and directories
bucket_name = "relmart-sales-project"
s3_customer_datamart_s3prefix = "customer_data_mart/"
s3_sales_datamart_s3prefix = "sales_data_mart/"
s3_source_s3prefix = "sales_data/"
s3_error_s3prefix = "sales_data_error/"
s3_processed_s3prefix = "sales_data_processed/"


#Database credential
# MySQL database connection properties
database_name = "relmart_db"
url = f"jdbc:mysql://localhost:3306/{database_name}"            # Found using this command in cmd after connecting to mysql >> SHOW VARIABLES LIKE 'port';
properties = {
    "user": "root",
    "password": "pFzUT/IlDwrg2VxoEvMz60GQlUaXjpdoYg1QubHukDg=",
    "driver": "com.mysql.cj.jdbc.Driver"
}

mysql_creds = {
    "host": "localhost",
    "user": "root",
    "password": "pFzUT/IlDwrg2VxoEvMz60GQlUaXjpdoYg1QubHukDg=",
    "database": "relmart_db"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Incoming Data Schema
schema_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]

# Expected schema for sales data

final_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", DoubleType(), True)
])

expected_schema = {
    "columns": {
    "customer_id": { "type": "integer", "nullable": False },
    "customer_name": { "type": "string","nullable": True },
    "customer_address": { "type": "string","nullable": True },
    "store_id": { "type": "string", "nullable": False },
    "sales_person_id": {"type": "integer", "nullable": False},
    "sales_person_name": {"type": "string", "nullable": True},
    "product_name": { "type": "string", "nullable": False },
    "product_id": { "type": "integer", "nullable": False },
    "sales_date": { "type": "date", "nullable": False },
    "price": { "type": "double", "nullable": False },
    "quantity": { "type": "integer", "nullable": False },
    "total_cost": { "type": "double", "nullable": False }
},
    "required":
        [ "customer_id",
          "store_id",
          "product_name",
          "sales_date",
          "sales_person_id",
          "price",
          "quantity",
          "total_cost" ]
}


#File Upload Location
from_local_to_s3 = "Z:\\Projects\\relmart-project\\rawdata\\upload_to_s3"


#File Segregation
destination_prefix = "s3://relmart-sales-project/"
corrupt_file_path = "schema_check/schema_mismatched_csv/"
mismatched_filetype_path = "schema_check/type_mismatched_files/"
valid_schema_file_path = "s3://relmart-sales-project/schema_check/schema_validated/"

# File Download location
local_directory = "Z:\\Projects\\relmart-project\\spark_data\\file_from_s3\\"
customer_datamart_local_file = "Z:\\Projects\\relmart-project\\spark_data\\customer_datamart\\"
sales_team_datamart_local_file = "Z:\\Projects\\relmart-project\\spark_data\\file_from_s3\\"
sales_team_datamart_partitioned_local_file = "Z:\\Projects\\relmart-project\\spark_data\\sales_team_datamart_partitioned\\"
