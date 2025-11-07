from src.main.utility.logging_config import logger
import mysql.connector
from src.main.utility.encrypt_decrypt import decrypt
from resources.dev import config

class MySqlSession:
    def __init__(self):
        self.host = config.mysql_creds["host"]
        self.user = config.mysql_creds["user"]
        self.password = decrypt(config.mysql_creds["password"])
        self.database = config.mysql_creds["database"]
        self.connection = None

    def get_mysql_connection(self):
        try:
            logger.info("Connecting to mysql database...")
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            logger.info("Connection with mysql established.")
            return self.connection
        except mysql.connector.Error as err:
            logger.error(f"Connection with mysql database failed: {err}")
            raise err

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("🔌 MySQL connection closed.")
