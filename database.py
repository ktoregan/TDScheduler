import mysql.connector
import os
import base64
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Function to get database connection
def get_db_connection():
    db_config = {
        "host": os.getenv("MYSQL_HOST"),
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "database": os.getenv("MYSQL_DB"),
        "port": os.getenv("MYSQL_PORT"),
    }

    # Use SSL certificate content directly from environment variable
    ssl_ca = base64.b64decode(os.getenv("SSL_CERT_CONTENT")).decode("utf-8")

    # Add SSL config to the database connection
    db_config["ssl_ca"] = ssl_ca

    try:
        connection = mysql.connector.connect(**db_config)
        print("Successfully connected to the database")
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to the database: {err}")
        raise