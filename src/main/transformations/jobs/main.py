import os
#import sys

from resources.dev import config
from src.main.move.move_files import MoveFiles
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.dimension_tables_join import dimensions_table_join
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.mysql_session import MySqlSession
from src.main.utility.read_s3_metadata import ReadS3Metadata
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.logging_config import logger
from src.main.utility.spark_session import spark_session
from pyspark.sql.functions import *
from src.main.utility.handling_dataframe import DataframeOperation
from src.main.write.parquet_writer import write_dataframe


def main():


    ############### Get S3 Client ###################
    aws_access_key = config.aws_access_key
    aws_secret_key = config.aws_secret_key

    s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
    s3_client = s3_client_provider.get_client()
    # Now we can use s3_client for S3 related operations and won't need to reauthorize

    env = config.environment
    mysql_session_obj = MySqlSession()
    connection = mysql_session_obj.get_mysql_connection()
    cursor = connection.cursor()

    dataframe_operation = DataframeOperation()

    try:

    ######################## READING S3_METADATA TABLE ON MYSQL DATABASE ########################

        # We will read s3_metadata table and find bucket and prefixes to scan
        # If file is present then check if the same file is present in the staging area with status 'Unprocessed'.
        # If so then don't delete and try to re-run
        # Else give an error and don't process the next file.

    # Step 1: Get bucket details dynamically from s3_metadata table inside our mysql database using the get_bucket_details method of ReadS3Metadata() class.


        crud_obj = ReadS3Metadata(connection)  # defining a crud object to access and use methods under ReadS3Metadata class to access s3_metadata table.

        buckets_info = crud_obj.get_bucket_details(env)  # Using the crud object we created to call get_bucket_details and fetch bucket_name and folder_path from s3_metadata table
        #logger.info(f"Bucket details: {buckets_info}")

        # If there are no active S3 metadata records found for the given environment, raise an exception.
        if not buckets_info:
            raise Exception(f"No active S3 metadata found for environment: {env}")

        spark = spark_session()

        # Step 2: Fetching Bucket Name and Bucket Path from buckets_info (has info from s3_metadata)
        for bucket in buckets_info:
            bucket_name = bucket["bucket_name"]
            folder_path = bucket["folder_path"]
            logger.info(f"Processing S3 bucket: {bucket_name}, folder: {folder_path}")


            ######################## CHECKING S3 BUCKETS AND PREFIXES FOR CSV FILES ########################

            # Step 1: Initialize S3Reader
            s3_reader = S3Reader()

            # Step 4: Get a List that contains all the absolute paths to files found on S3 within the specified bucket and folder_path.
            s3_absolute_file_path = s3_reader.list_files(s3_client, bucket_name, folder_path)


            # Step 5: Check if any CSV files were found and log accordingly
            if not s3_absolute_file_path:
                logger.error(f"No files found in {bucket_name}, folder: {folder_path} ")
                continue

            #logger.info(f"List of absolute file paths for CSVs found on S3 bucket: {s3_absolute_file_path}")

            #file_paths = list(url.split(f"{bucket_name}/")[1] for url in s3_absolute_file_path)        #No longer needed as I'm not downloading files on local
            #print(f"File Paths: {file_paths}")

            file_names = [os.path.basename(key) for key in s3_absolute_file_path]
            #print(f"ABS PATH : {s3_absolute_file_path}")
            #print(f"File Names: {file_names}")


            ######################## MATCHING FILE NAMES FROM S3 WITH FILE NAMES IN STAGING TABLE ########################


            try:
                if not file_names:
                    logger.error(f"No files found in {bucket_name}, folder: {folder_path} ")
                    raise Exception

                placeholders = ', '.join(['%s'] * len(file_names))
                staging_query = f""" select distinct file_name from
                                {config.mysql_creds["database"]}.{config.product_staging_table}
                                where file_name in ({placeholders}) AND status = 'U' """     #Query to check if the files in the file_names list is present in the staging_table

                cursor.execute(staging_query, file_names)
                #logger.info(f"Query unprocessed file: {staging_query}")
                data = cursor.fetchall()

                if not data:
                    logger.info(f"No records in {file_names}")
                else:
                    logger.info(f"Found {len(data)} matching records in staging for {bucket_name}")

            except Exception as err:
                logger.error(f"Error while matching filenames in Staging : {err}")




            ######################## CREATING SPARK SESSION AND VALIDATING SCHEMA ########################

            # Step 1: Creating Spark Session


            # Step 2: Reading CSV files and Inferring Schema

            logger.info(f"Running loop for Abs Path: {s3_absolute_file_path}")

            final_df_to_process = spark.createDataFrame([], config.final_schema)
            corrupt_files = []

            for file_path in s3_absolute_file_path:
                if file_path:                           #example: file_path = 's3://relmart-sales-project/sales_data/2025_10_30/Customers.csv'
                    data_df = spark.read.format("csv")\
                        .option("header", True)\
                        .option("inferSchema", True)\
                        .load(file_path.replace("s3://", "s3a://"))
                    #logger.info("Original DataFrame:")
                    #data_df.show(5)


            # Step 3: Normalizing Inferred Schema

                    actual_data_dict = dataframe_operation.make_dict(data_df)

            # Step 4: Extracting Columns

                    expected_columns = set(config.expected_schema["columns"].keys())
                    required_columns = set(config.expected_schema["required"])
                    actual_data_columns = set(actual_data_dict.keys())

                    # logger.info(f"Expected data columns: {expected_columns}")
                    # logger.info(f"Actual data columns: {actual_data_columns}")
                    # logger.info(f"Required data columns: {required_columns}")

                    """
                    Segregate files that passes th
                    1. Check if actual data has missing columns from expected columns
                    2. Check if there are any extra columns in comparison to expected columns
                    3. Check all required columns set are in actual data columns
                    4. Check if all the columns in actual data has the same DataType as expected schema's DataT
                    5. Check if any of the required fields are nullable in actual data columns and handle the null values in it by default value.
                            lit values will be decided based on the DataType a column 
                    6. Order the set into a ordered list of columns based on expected_schema before writing dataframe for consistent schema.
                            However, if there are any missing columns in between then add missing columns first with None or default values.
                    """

            # Step 4: Validating Column Names, DataTypes, Nullability




                    # Condition 1: Check if actual data has missing columns from expected.
                                # Check if all required columns are present
                                        # Add missing columns for the actual data
                                # If all required columns are not present
                                        # Move to Corrupt Files

                    missing_columns = (expected_columns - actual_data_columns)
                    missing_required = list(missing_columns & required_columns)



                    if not missing_required:
                        for colm in missing_columns:
                            colm_data_type = config.expected_schema["columns"][colm]["type"]
                            colm_nullable = config.expected_schema["columns"][colm]["nullable"]

                            data_df = data_df.withColumn(colm, dataframe_operation.add_default_value(colm_data_type,colm_nullable))
                        logger.info("Missing columns added")
                        #logger.info("Dataframe after adding missing columns:")
                        #data_df.show(5)

                    else:
                        corrupt_files.append(file_path)
                        logger.error(f"File {corrupt_files} has missing required columns: {missing_required}")
                        mover = MoveFiles(s3_client)
                        mover.move_s3_to_s3(bucket_name, folder_path, config.corrupt_file_path, corrupt_files)
                        continue





                            #This sub-condition below is not required anymore as we have used the nullable as False for required column in the schema itself and wrote the add_default_value accordingly.
                                    # Sub-Condition 1.1: Check all required columns are present in actual data columns.
                                    # Set the values of all records in those missing columns to a default value so that they are no longer null
                                    #missing_required = required_columns - actual_data_columns


                    # Condition 2: Check if there are any extra columns in comparison to expected.
                    #Extra columns data is combined and add to an extra column

                    extra_columns = list(set(data_df.columns) - expected_columns)

                    if extra_columns:
                        #data_df = data_df.withColumn("extra_data", concat_ws(", ",*extra_columns)).drop(*extra_columns)
                        data_df = data_df.withColumn("extra_data", to_json(struct(*extra_columns))).drop(*extra_columns)
                        data_df = data_df.select(*list(config.expected_schema["columns"].keys()), "extra_data")
                        #data_df.show(5)
                        #data_df.printSchema()



                    #Condition 3: Check if DataType matches for same column name
                    mismatch = False
                    for key, value in config.expected_schema["columns"].items():

                        if key in data_df.columns:
                            expected_type = value["type"].lower()
                            actual_type = data_df.schema[key].dataType.simpleString().lower()

                            if expected_type != actual_type:
                                mismatch = True

                                #logger.error(f"DataType mismatch for {key} column in {file_names} : Expected: {expected_type} vs Actual: {actual_type}")
                                data_df = dataframe_operation.convert_type(data_df,key, expected_type,actual_type)

                    if mismatch:
                        logger.info("Enforcing Correct DataType...")

                    final_df_to_process = final_df_to_process.unionByName(data_df, allowMissingColumns=True)

            logger.info(f"Writing final dataframe:")
            #final_df_to_process.show(5)
            #final_df_to_process.printSchema()


            ######################## CONNECTING TO DATABASE TO ACCESS DIM TABLES ########################

            db_reader = DatabaseReader(config.url, config.properties)

            #customer_table
            customer_df = db_reader.create_dataframe(spark, config.customer_table_name)
            #customer_df.printSchema(5)

            # product_table
            product_table_df = db_reader.create_dataframe(spark, config.product_table)

            # product_staging_table
            product_staging_table_df = db_reader.create_dataframe(spark, config.product_staging_table)

            # sales_team table
            sales_team_df = db_reader.create_dataframe(spark, config.sales_team_table)

            # store_table
            store_df = db_reader.create_dataframe(spark, config.store_table)

            logger.info("Dimension tables loaded in spark successfully")

        ######################## DATA ENRICHMENT / NULL HANDLING ########################

        #Join all dataframes

            enriched_df = dimensions_table_join(final_df_to_process, customer_df, store_df, sales_team_df)
            logger.info("Dataframe after data enrichment:")
            enriched_df.show(5)
            #print(enriched_df.columns)

        ######################## CUSTOMER DATA MART ########################

            customer_datamart_df = enriched_df.select("customer_id", "customer_name","customer_address",
                                                      "phone_number", "sales_date", "total_cost")


            write_dataframe(enriched_df, config.customer_datamart_local_file)
            logger.info("Parquet file written successfully")




    except Exception as err:
        logger.error(f"Job failed, exited with error: {err}")
        raise err
    finally:
        mysql_session_obj.disconnect()

if __name__ == "__main__":
    main()