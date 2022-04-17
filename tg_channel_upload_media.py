from config import *
from GoogleDriveWrapper import *
import asyncio
from google.cloud import storage
from google.auth import credentials
from db import *

# Set environment variable GOOGLE_APPLICATION_CREDENTIALS with the file of the service account key for gcloud bucket

class ChannelMediaUploader:
    def __init__(self, sessionfactory, download_dir, upload_dir, channel_id, subdir, bucket_name):
        self.sessionfactory = sessionfactory
        self.download_dir = download_dir
        self.upload_dir = upload_dir
        self.channel_id = channel_id
        self.subdir = subdir
        self.bucket_name = bucket_name

    async def upload_channel(self):
        Session = scoped_session(self.sessionfactory)
        session = Session()

        channel = session.query(Chat).where(Chat.id == self.channel_id).first()
        messages = session.query(Message).where(Message.chat_id == self.channel_id,
                                                Message.content_type != 'web',
                                                Message.content_type != 'text',
                                                Message.uploaded == None,
                                                Message._metadata != None).all()

        if messages is None or len(messages) == 0:
            return

        print(f"processing {len(messages)} for channel {channel.username} ({channel.id})")
        for message in messages:
            filename = message._metadata
            if filename is None or len(filename)==0:
                continue
            filepath = os.path.join(self.download_dir, self.subdir, filename)
            if not os.path.exists(filepath):
                #print(f"{channel.username} m.id={message.id}, {filepath} does not exist")
                continue

            link = await asyncio.to_thread(self.upload_file_bucket, filepath, message.id)

            if link is not None:
                message.uploaded = link
                session.commit() #store uploaded into db
                os.unlink(filepath) #delete file after uploading, to conserve space
            else:
                print(f"failed to upload message id={message.id} ({message.send_date}), channel={channel.username}")

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

async def main():
    # load config:
    config = Config()
    engine = create_sqlachemy_engine(config.sqlalchemy_connection_string())
    sessionfactory = sessionmaker(engine)
    session = scoped_session(sessionfactory)

    chats = session.query(Chat).all()

    print(f"uploading media for {len(chats)} channels to {config.bucket_name} GCS bucket")
    uploaders = []
    for chat in chats:
        try:
            subdir = f"{chat.id}"
            uploader = ChannelMediaUploader(sessionfactory, config.download_dir, config.upload_dir, chat.id, subdir, config.bucket_name)
            uploaders.append(uploader.upload_channel())
        except:
            continue

    await asyncio.wait(uploaders)

asyncio.run(main())

