
from minio import Minio
from minio.error import S3Error
import os
from minio import error 

def write_data_to_minio(data_directory, minio_server, access_key, secret_key, minio_bucket):
    """
    Uploads all Parquet files from a local directory to Minio.

    Parameters:
    - data_directory: Local directory containing Parquet files.
    - minio_server: Minio server URL (e.g., "localhost:9000").
    - access_key: Minio access key.
    - secret_key: Minio secret key.
    - minio_bucket: Name of the Minio bucket.

    Returns:
    - None
    """
    client = Minio(minio_server, access_key=access_key, secret_key=secret_key, secure=False)
    
    # Check if the bucket already exists, if not, create it
    found = client.bucket_exists(minio_bucket)
    if not found:
        try:
            client.make_bucket(minio_bucket)
            print(f"Bucket '{minio_bucket}' created successfully.")
        except S3Error as e:
            print(f"Error creating bucket: {e}")
    else:
        print(f"Bucket '{minio_bucket}' already exists.")

    # Upload each Parquet file to the Minio bucket
    for file_name in os.listdir(data_directory):
        file_path = os.path.join(data_directory, file_name)
        try:
            with open(file_path, 'rb') as data_file:
                client.put_object(minio_bucket, file_name, data_file, length=os.stat(file_path).st_size)
            print(f"Data file {file_name} uploaded to Minio.")
        except Exception as e:
            print(f"Failed to upload data file {file_name} to Minio: {str(e)}")

if __name__ == '__main__':
    # Minio server details
    minio_server = "localhost:9000"
    access_key = "minio"
    secret_key = "minio123"

    # Minio bucket details
    minio_bucket = "tarikul"

    # Directory containing Parquet files
    parquet_directory = r'C:\Users\Tarikul\Desktop\M1\Atelier – Architecture Décisionnel Rakib\ATL-Datamart\data\raw'

    # Upload data to Minio
    write_data_to_minio(parquet_directory, minio_server, access_key, secret_key, minio_bucket)