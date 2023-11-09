import os
import requests
from datetime import datetime, timedelta


def download_yellow_taxi_data(start_year, end_year, save_directory):
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
   
    # Create the directory if it doesn't exist
    os.makedirs(save_directory, exist_ok=True)


    current_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)


    while current_date <= end_date:
        # Construct the URL for the current month and year
        file_url = base_url + f"yellow_tripdata_{current_date.strftime('%Y-%m')}.parquet"


        # Define the local file path
        local_file_path = os.path.join(save_directory, f"yellow_tripdata_{current_date.strftime('%Y-%m')}.parquet")


        # Download the file
        try:
            response = requests.get(file_url)
            if response.status_code == 200:
                with open(local_file_path, 'wb') as f:
                    f.write(response.content)
                print(f"Data for {current_date.strftime('%Y-%m')} downloaded and saved.")
            else:
                print(f"Failed to download data for {current_date.strftime('%Y-%m')}. Status code: {response.status_code}")
        except Exception as e:
            print(f"An error occurred while downloading data for {current_date.strftime('%Y-%m')}: {str(e)}")


        # Move to the next month
        current_date += timedelta(days=30)  # Assuming an average month length of 30 days


# Example usage:
start_year = 2018
end_year = 2023
download_directory = r'C:\Users\malia.ahres\ATL-Datamart\data\raw'  # Replace with your desired save directory
download_yellow_taxi_data(start_year, end_year, download_directory)


