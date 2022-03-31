from config import *
from GoogleDriveWrapper import *
import asyncio

RETRIES = 3 # put into config

class ChannelMediaUploader:
    def __init__(self, db, download_dir, upload_dir, channel_id, subdir):
        self.db = db
        self.download_dir = download_dir
        self.upload_dir = upload_dir
        self.channel_id = channel_id
        self.subdir = subdir

    async def upload_channel(self):
        folder = Folder(PurePath(f'{self.upload_dir}/{self.channel_id}'), createIfNotExists=True)
        messages = self.db.get_messages(chat_id=self.channel_id, has_no_uploaded=True)
        if messages is not None:
            for message in messages:
                message_id = message[0]
                filename = message[len(message) - 2]
                if filename is not None:
                    filepath = os.path.join(self.download_dir, self.subdir, filename)
                    if os.path.exists(filepath):
                        gfile = None
                        for i in range(RETRIES):
                            try:
                                print(f"uploading {filepath}")
                                gfile = folder.upload(filepath)
                                break
                            except Exception as e:
                                print(f"retying upload {filepath}, error: {e}")
                        if gfile is not None:
                            self.db.update_message_upload(message_id=message_id, chat_id=self.channel_id, uploaded=gfile['alternateLink'])

async def main():
    # load config:
    config = get_config()
    db = get_db()
    uploads = []
    K_DOWNLOAD_DIR = 'download_dir'
    K_UPLOAD_DIR = 'gdrive_upload_dir'
    download_dir = config[K_DOWNLOAD_DIR] if K_DOWNLOAD_DIR in config else 'download'
    upload_dir = config[K_UPLOAD_DIR] if K_UPLOAD_DIR in config else 'tghistory'
    print(f"uploading to {upload_dir}")
    for subdir in os.listdir(download_dir):
        try:
            channel_id = int(subdir)
            uploader = ChannelMediaUploader(db, download_dir, upload_dir, channel_id, subdir)
            uploads.append(uploader.upload_channel())
        except:
            continue
    print(f"uploading media for {len(uploads)} channels in parallel")
    await asyncio.wait(uploads)

asyncio.run(main())

