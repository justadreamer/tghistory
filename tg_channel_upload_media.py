import asyncio
import yaml
from yaml import Loader
import os
from postgres_connection import *
from GoogleDriveWrapper import *

#load config:
with open('config.yml','r') as f:
    config = yaml.load(f, Loader)['default']
    print(config)

# global service objects:
connection_string = "dbname={} user={} password={} host={} port={}".format(config['dbname'],
                                                               config['dbuser'],
                                                               config['dbpassword'],
                                                               config['dbhost'],
                                                               config['dbport'])
db = DB(connection_string=connection_string)

download_dir = config['download'] if 'download' in config else 'download'

def upload():
    for subdir in os.listdir(download_dir):
        try:
            channel_id = int(subdir)
        except:
            continue

        folder = Folder(PurePath(f'tghistory/{channel_id}'), createIfNotExists=True)
        messages = db.get_messages(chat_id=channel_id)
        if messages is not None:
            for message in messages:
                print(message)
                message_id = message[0]
                filename = message[len(message)-2]
                if filename is not None:
                    filepath = os.path.join(download_dir, subdir, filename)
                    if os.path.exists(filepath):
                        gfile = folder.upload(filepath)
                        db.update_message_upload(message_id=message_id, chat_id=channel_id, uploaded=gfile['alternateLink'])

upload()
