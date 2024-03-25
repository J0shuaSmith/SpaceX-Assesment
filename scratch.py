import prefect
import pymysql
import boto3
from prefect import task, Flow
import pandas as pd
from io import StringIO
from datetime import datetime
import requests
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from tenacity import retry, stop_after_attempt, wait_exponential
import os


# AWS Configuration
AWS_REGION = 'us-east-1'
S3_BUCKET_NAME = 'spacex-data-pipeline'
AWS_ACCESS_KEY_ID = 'AKIA6GBMH67UKMEWGIU3'  # Replace with your access key
AWS_SECRET_ACCESS_KEY = 'gRj2ZAZvKTscy+6X/he/wazkNx/deJbIyaBQ2Ta5'  # Replace with your secret access key
rds_host = 'space-x-database.c54ommimud2t.us-east-1.rds.amazonaws.com'
db_username = 'admin'
db_password = 'spacexdatabase'
db_name = 'spacexdb'


# Initialize Boto3 clients
s3 = boto3.client('s3', region_name=AWS_REGION,
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


@task
def extract_data():
    response = requests.get('https://api.spacexdata.com/v5/launches/latest')
    response.raise_for_status()
    return response.json()


@task
def process_data(raw_data):
    relevant_keys = ['rocket', 'success', 'failures', 'crew', 'payloads', 'launchpad', 'flight_number', 'name',
                     'date_utc', 'date_unix', 'date_local', 'date_precision', 'upcoming', 'cores']
    processed_data = {key: raw_data[key] for key in relevant_keys if key in raw_data}

    # Convert 'crew' list of dictionaries to a comma-separated string
    processed_data['crew'] = ', '.join([crew['crew'] for crew in processed_data.get('crew', [])])

    # Convert 'payloads' list to a comma-separated string
    processed_data['payloads'] = ', '.join(processed_data.get('payloads', []))

    # Extract relevant information from nested dictionaries
    processed_data['cores'] = [core['core'] for core in processed_data.get('cores', [])]

    # Convert UTC date string to datetime
    processed_data['date_utc'] = pd.to_datetime(processed_data['date_utc'])

    # Construct DataFrame
    processed_df = pd.DataFrame([processed_data])

    # Set pandas display options to show all columns without truncation
    pd.set_option('display.max_columns', None)

    # Display the DataFrame
    print("Processed DataFrame:")
    print(processed_df)

    # Reset pandas display options to default after displaying the DataFrame
    pd.reset_option('display.max_columns')

    return processed_df


@task
def upload_to_s3(processed_df):
    try:
        # Convert DataFrame to Parquet format
        parquet_file = "processed_data.parquet"
        processed_df.to_parquet(parquet_file)

        # Get the year from the date field
        year_from_date = processed_df['date_utc'].dt.year.iloc[0]

        # Define S3 key with the desired structure
        s3_key = f"spacex-data/{year_from_date}/launch_data.parquet"

        # Upload to S3
        s3.upload_file(parquet_file, S3_BUCKET_NAME, s3_key)
        print(f"Uploaded {parquet_file} to S3 bucket {S3_BUCKET_NAME} with key {s3_key}")

    except Exception as e:
        print("Error uploading to S3:", e)




@task
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def load_data():
    try:
        # Connect to MySQL RDS
        conn = pymysql.connect(
            host=rds_host,
            user=db_username,
            password=db_password,
            database=db_name,
            port=3306,
            local_infile=True
        )
        print("Connected to the database")

        # Create a cursor object to execute queries
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute("SHOW TABLES LIKE 'spacex_sample_data'")
        table_exists = cursor.fetchone()

        if not table_exists:
            # Table doesn't exist, create it
            cursor.execute("""
                CREATE TABLE spacex_sample_data (
                    rocket VARCHAR(255),
                    success BOOLEAN,
                    failures VARCHAR(255),
                    crew VARCHAR(255),
                    payloads VARCHAR(255),
                    launchpad VARCHAR(255),
                    flight_number INT,
                    name VARCHAR(255),
                    date_utc DATETIME,
                    date_unix BIGINT,
                    date_local DATETIME,
                    date_precision VARCHAR(255),
                    upcoming BOOLEAN,
                    cores VARCHAR(255)
                )
            """)
            print("Created table 'spacex_sample_data'")

        # Download data file from S3
        s3_object_key = "processed_data.parquet"
        local_directory = "C:\\temp\\"

        local_file_path = os.path.join(local_directory, s3_object_key)
        # Replace backslashes with forward slashes in the file path
        local_file_path = local_file_path.replace("\\", "/")

        # Load data from the local file into the MySQL table
        cursor.execute(
            f"LOAD DATA LOCAL INFILE '{local_file_path}' INTO TABLE spacex_sample_data FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")

        print(local_file_path)

        print("Local file path:", repr(local_file_path))
        print("S3 Bucket Name:", S3_BUCKET_NAME)
        print("S3 Object Key:", s3_object_key)

        with open(local_file_path, 'wb') as f:
            s3.download_fileobj(S3_BUCKET_NAME, s3_object_key, f)

        print(f"Downloaded {s3_object_key} from S3 bucket {S3_BUCKET_NAME}")

        # Load data from the local file into the MySQL table
        cursor.execute(f"LOAD DATA LOCAL INFILE '{local_file_path}' INTO TABLE spacex_sample_data FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
        conn.commit()

        print("Data loaded successfully from S3 to MySQL")

    except pymysql.Error as e:
        print("Error: Could not connect to MySQL or load data")
        print(e)

    finally:
        # Close the cursor and connection
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            print("Connection closed")



with Flow("SpaceX_Data_Pipeline") as flow:
    raw_data = extract_data()
    processed_data = process_data(raw_data)
    upload_to_s3(processed_data)
    load_data()

# Run the Prefect Flow
flow.run()
