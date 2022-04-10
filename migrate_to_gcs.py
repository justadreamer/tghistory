from config import *
from db import *
import os
from pydrive2.auth import *
from pydrive2.drive import *
from GoogleDriveWrapper import *
from google.cloud import storage
from google.auth import credentials

class MessageProcessor:
    def __init__(self):
        self.config = Config()
        engine = create_sqlachemy_engine(self.config.sqlalchemy_connection_string())
        sessionfactory = session_factory(engine)
        self.session = scoped_session(sessionfactory)()


    def process_message(self, message):
        ext = os.path.splitext(message._metadata)[1]
        filename = f"{message.id}{ext}"
        filepath = os.path.join(self.config.download_dir, f"{message.chat_id}", filename)
        print(f"{message.uploaded} downloading to {filepath}")

        components = message.uploaded.split('/')
        id = components[len(components) - 2]

        #download
        gdriveFile = self.drive.CreateFile(metadata={'id':id})
        gdriveFile.GetContentFile(filepath)

        # upload it to gcs bucket with the corresponding path of chat_id/message_id.ext
        storage_client = storage.Client()
        bucket: storage.Bucket = storage_client.bucket(self.config.bucket_name)
        destination_blob_name = f"{message.chat_id}/{filename}"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(filepath)

        # rewrite the uploaded field in message with public link
        chat = self.session.query(Chat).where(Chat.id == message.chat_id).first()
        message.uploaded = blob.public_url
        self.session.commit()
        print(f"updated {chat.username}, message date {message.send_date}, uploaded={message.uploaded}")
        # delete the uploaded file
        os.unlink(filepath)

    def main(self):
        messages = self.session.query(Message).filter(Message.uploaded.like('%drive.google.com%')).all()
        self.drive = GoogleDrive()

        print(f"processing {len(messages)} messages")
        for message in messages[:10]:
            self.process_message(message)

MessageProcessor().main()