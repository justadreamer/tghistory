from config import *
from GoogleDriveWrapper import *
import asyncio
from google.cloud import storage
from google.auth import credentials

RETRIES = 3 #put into config #deprecated we are now uploading to gcloud bucket

# Set environment variable GOOGLE_APPLICATION_CREDENTIALS with the file of the service account key for gcloud bucket

class ChannelMediaUploader:
    def __init__(self, db, download_dir, upload_dir, channel_id, subdir, bucket_name):
        self.db = db
        self.download_dir = download_dir
        self.upload_dir = upload_dir
        self.channel_id = channel_id
        self.subdir = subdir
        self.bucket_name = bucket_name

    async def upload_channel(self):
        messages = self.db.get_messages(chat_id=self.channel_id, has_no_uploaded=True)
        if messages is None:
            return
        print(f"processing {len(messages)} for channel {self.channel_id}")
        for message in messages:
            message_id = message[0]
            filename = message[len(message) - 2]
            if filename is None:
                continue
            filepath = os.path.join(self.download_dir, self.subdir, filename)
            if not os.path.exists(filepath):
                continue

            link = await asyncio.to_thread(self.upload_file_bucket, filepath, message_id) #self.upload_file_gdrive(filepath)

            if link is not None:
                print(f'updating message id={message_id}, channel_id={self.channel_id}, with uploaded={link}')
                self.db.update_message_upload(message_id=message_id, chat_id=self.channel_id, uploaded=link)
                os.unlink(filepath) #delete file after uploading, to conserve space

    def upload_file_bucket(self, filepath, message_id):
        storage_client = storage.Client()
        bucket: storage.Bucket = storage_client.bucket(self.bucket_name)

        #rename using message_id - to avoid long filenames, need to download using message_id as well,
        # preserving ext however as a hint to mime-type
        filenameext = os.path.basename(filepath)
        filename, ext = os.path.splitext(filenameext)
        filename = f"{message_id}" + ext
        #rename

        destination_blob_name = f"{self.channel_id}/{filename}"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(filepath)
        return blob.public_url

    #DEPRECATED
    def upload_file_gdrive(self, filepath, message_id):  #deprecated, we are not uploading to gcloud bucket
        link = None
        for i in range(RETRIES):
            try:
                print(f"uploading {filepath}")
                folder = Folder(PurePath(f'{self.upload_dir}/{self.channel_id}'), createIfNotExists=True)
                gfile = folder.upload(filepath)
                if gfile is not None:
                    link = gfile['alternateLink']
                break
            except Exception as e:
                print(f"retrying upload {filepath}, error: {e}")
        return link

async def main():
    # load config:
    config = Config()
    db = config.get_db()
    chats = db.get_chats()

    print(f"uploading media for {len(chats)} channels to {config.bucket_name} GCS bucket")
    uploaders = []
    for chat in chats:
        try:
            channel_id = chat[0]
            subdir = f"{channel_id}"
            uploader = ChannelMediaUploader(db, config.download_dir, config.upload_dir, channel_id, subdir, config.bucket_name)
            uploaders.append(uploader.upload_channel())
        except:
            continue

    await asyncio.wait(uploaders)

asyncio.run(main())

