import yaml
from google.cloud import storage
import os

def upload_file(local_path, bucket, prefix):
    file_name = os.path.basename(local_path)
    blob = bucket.blob(f"{prefix}{file_name}")
    blob.upload_from_filename(local_path)
    print(f"Uploaded {file_name} to gs://{bucket.name}/{prefix}{file_name}")

def main():
    config = yaml.safe_load(open("config/gcs_config.yaml"))
    client = storage.Client.from_service_account_json(config["credentials_path"])
    bucket = client.bucket(config["bucket_name"])

    for file_path in config["input_files"]:
        upload_file(file_path, bucket, config["synthetic_output_prefix"])

if __name__ == "__main__":
    main()