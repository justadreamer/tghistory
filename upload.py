from GoogleDriveWrapper import *
import argparse
from pathlib import Path
from pathlib import PurePath

def get_args():
    parser = argparse.ArgumentParser(description='upload {file} {google_drive_dir}')
    parser.add_argument('local_path', type=str, help='path to the file to be uploaded')
    parser.add_argument('remote_path', nargs='?', default='/', type=str, help='path to google drive location of the file')
    args = parser.parse_args()
    return args

def upload(local_path, remote_path):
    google_folder = Folder(remote_path, createIfNotExists=True)
    google_folder.upload(local_path)

def main():
    args = get_args()
    local_path = args.local_path
    remote_path = args.remote_path

    if not Path(local_path).exists():
        print(f"path {local_path} does not exist")
        exit(1)

    print(f"uploading local file: {local_path} into Google Drive dir: {remote_path}")
    wrapped_remote_path = PurePath(remote_path)
    if remote_path == '/':
        wrapped_remote_path = PurePath()
    upload(local_path, wrapped_remote_path)


if __name__ == '__main__':
    main()
