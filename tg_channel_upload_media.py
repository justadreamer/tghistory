from config import *
from GoogleDriveWrapper import *

def upload():
    # load config:
    config = get_config()
    db = get_db()

    download_dir = config['download'] if 'download' in config else 'download'

    for subdir in os.listdir(download_dir):
        try:
            channel_id = int(subdir)
        except:
            continue

        folder = Folder(PurePath(f'tghistory/{channel_id}'), createIfNotExists=True)
        messages = db.get_messages(chat_id=channel_id)
        if messages is not None:
            for message in messages:
                message_id = message[0]
                filename = message[len(message)-2]
                if filename is not None:
                    filepath = os.path.join(download_dir, subdir, filename)
                    if os.path.exists(filepath):
                        gfile = folder.upload(filepath)
                        db.update_message_upload(message_id=message_id, chat_id=channel_id, uploaded=gfile['alternateLink'])

upload()
