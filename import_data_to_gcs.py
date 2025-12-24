from google.cloud import storage
from pathlib import Path

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # Initialize a storage client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob object from the bucket
    blob = bucket.blob(destination_blob_name)

    # Upload the file to GCS
    blob.upload_from_filename(source_file_name)

    return  print(f"File {source_file_name} uploaded to {destination_blob_name}.")  

if __name__ == "__main__":
    data_dir = Path("data/")
    for csv_file in data_dir.glob("*.csv"):
        # Define GCS bucket name and destination path
        bucket_name = "secops_storage_mouna"
        destination_blob_name = f"data/{csv_file.name}"

        # Upload the CSV file to GCS
        upload_to_gcs(bucket_name, str(csv_file), destination_blob_name)