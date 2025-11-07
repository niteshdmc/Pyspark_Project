from src.main.utility.logging_config import logger
from src.main.utility.mysql_session import MySqlSession

class ReadS3Metadata:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = self.connection.cursor(dictionary=True)           #Set dictionary = True so that the cursor returns a dictionary instead of tuple.

    def get_bucket_details(self,environment):

        #Fetch all active (status='A') S3 bucket configurations for the given environment.
        try:
            query = """
            SELECT bucket_name, folder_path
            FROM s3_metadata
            WHERE status = 'A' AND environment = %s
            """
            self.cursor.execute(query, (environment,))
            result = self.cursor.fetchall()
            logger.info(f"Fetched {len(result)} active sources for environment: {environment}")
            return result
        except Exception as e:
            logger.error(f"Error fetching bucket details: {e}")
            raise e