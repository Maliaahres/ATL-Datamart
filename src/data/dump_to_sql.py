import gc
import os
import sys

import pandas as pd
from sqlalchemy import create_engine


def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    """
    Dumps a Dataframe to the DBMS engine

    Parameters:
        - dataframe (pd.Dataframe) : The dataframe to dump into the DBMS engine

    Returns:
        - bool : True if the connection to the DBMS and the dump to the DBMS is successful, False if either
        execution is failed
    """
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            success: bool = True
            print("Connection successful! Processing parquet file")
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')

    except Exception as e:
        success: bool = False
        print(f"Error connection to the database: {e}")
        return success

    return success


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite it columns into a lowercase format.
    Parameters:
        - dataframe (pd.DataFrame) : The dataframe columns to change

    Returns:
        - pd.Dataframe : The changed Dataframe into lowercase format
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


import os
import sys
import pandas as pd
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import gc
import logging

def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite its columns into a lowercase format.

    Parameters:
        - dataframe (pd.DataFrame): The dataframe columns to change

    Returns:
        - pd.DataFrame: The changed Dataframe in lowercase format
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe

def get_data_from_minio(minio_server, access_key, secret_key, minio_bucket, folder_path):
    client = Minio(minio_server, access_key=access_key, secret_key=secret_key, secure=False)

    parquet_files = [obj.object_name for obj in client.list_objects(minio_bucket)]

    for parquet_file in parquet_files:
        try:
            response = client.get_object(minio_bucket, parquet_file)
            parquet_content = BytesIO(response.read())
            parquet_df = pd.read_parquet(parquet_content, engine='pyarrow')

            clean_column_name(parquet_df)

            if not write_data_postgres(parquet_df, engine):
                return False

        except Exception as e:
            logging.error(f"Error processing {parquet_file}: {str(e)}")
            return False

    return True

def main() -> None:
    minio_server = "localhost:9000"
    access_key = "minio"
    secret_key = "minio123"
    minio_bucket = "tarikul"  # Replace with your actual Minio bucket name
    folder_path: str = "C:\Users\Tarikul\Desktop\M1\Atelier – Architecture Décisionnel Rakib\ATL-Datamart\data\raw"

    if not get_data_from_minio(minio_server, access_key, secret_key, minio_bucket, folder_path):
        sys.exit("Failed to retrieve and process data from Minio.")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())

