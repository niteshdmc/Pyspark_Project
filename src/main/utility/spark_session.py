import os
import findspark
findspark.init()
from pyspark.sql import SparkSession
from src.main.utility.encrypt_decrypt import decrypt
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import logger
from resources.dev import config

aws_access_key = decrypt(config.aws_access_key)
aws_secret_key = decrypt(config.aws_secret_key)
def spark_session():
    try:
        spark = (
            SparkSession.builder.master("local[*]").appName("relmart_app")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.extraClassPath", "C:\\mysql-connector\\mysql-connector-j-9.5.0.jar")     # Include MySQL JDBC driver
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262") # Include AWS + Hadoop libraries for S3 access
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") #S3 connection configuration
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.native.lib", "false")
            .config("spark.hadoop.native.lib", "false")
            .config("spark.hadoop.io.nativeio.disable.cache", "true")
            .getOrCreate()
        )

        logger.info(f"Spark session created")
        return spark

    except Exception as e:
        logger.error(f"Initiating Spark session failed: {e}")
        raise