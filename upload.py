from GoogleDriveWrapper import *
import argparse
from pathlib import Path
from pathlib import PurePath
from google.cloud import storage

def get_args():
    parser = argparse.ArgumentParser(description='upload {file} {google_drive_dir}')
    parser.add_argument('local_path', type=str, help='path to the file to be uploaded')
    parser.add_argument('remote_path', nargs='?', default='/', type=str, help='path to google drive location of the file')
    parser.add_argument('bucket', type=str, help='optional, gcs bucket, if you like to upload to gcs instead of gdrive')
    args = parser.parse_args()
    return args

def upload_gdrive(local_path, remote_path):
    wrapped_remote_path = PurePath(remote_path)
    if remote_path == '/':
        wrapped_remote_path = PurePath()
    google_folder = Folder(wrapped_remote_path, createIfNotExists=True)
    google_folder.upload(local_path)

def upload_gcs(bucket, local_path, remote_path):
    storage_client = storage.Client()
    bucket: storage.Bucket = storage_client.bucket(bucket)
    blob = bucket.blob(remote_path)
    blob.upload_from_filename(local_path)
    return blob.public_url

def main():
    args = get_args()
    local_path = args.local_path
    remote_path = args.remote_path
    bucket = args.bucket
    if not Path(local_path).exists():
        print(f"path {local_path} does not exist")
        exit(1)

    if bucket is not None:
        print(f"uploading local file: {local_path} into GCS bucket {bucket}: {remote_path}")
        url = upload_gcs(bucket, local_path, remote_path)
        print(f"public url: {url}")
    else:
        print(f"uploading local file: {local_path} into Google Drive dir: {remote_path}")
        upload_gdrive(local_path, remote_path)

if __name__ == '__main__':
    main()
