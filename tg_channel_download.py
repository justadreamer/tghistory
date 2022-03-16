from telethon import TelegramClient
import pprint
from postgres_connection import DB
import yaml
import asyncio
from telethon.tl.functions.messages import GetHistoryRequest

from telethon.tl.types import MessageMediaPhoto
from telethon.tl.types import MessageMediaDocument
from telethon.tl.types import MessageMediaWebPage
from telethon.tl.types import DocumentAttributeFilename
from telethon.tl.types import DocumentAttributeVideo
from telethon.tl.types import WebPage

import os
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from yaml import Loader

class Downloader:
    def __init__(self, db, client, debug = False):
        self.db = db
        self.client = client
        self.debug = debug

    async def download_history_batch(self, channel, offset_id):
        history = await self.client(GetHistoryRequest(
            peer=channel,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=100 if not self.debug else 10,
            max_id=0,
            min_id=0,
            hash=0
        ))
        min_date = datetime.now().replace(tzinfo=timezone(offset=timedelta()))
        offset_id = 0

        messages = history.messages

        for message in messages:
            print(message)
            message_dict = dict()
            offset_id = message.id
            date = message.date
            if date < min_date:
                min_date = date

            media = message.media
            filename = None  #extract either filename or url as metadata to store with the message record in the DB
            url = None
            content_type = 'text'
            if isinstance(media, MessageMediaDocument):
                attributes: [DocumentAttributeFilename] = message.media.document.attributes

                for attribute in attributes:
                    if isinstance(attribute, DocumentAttributeFilename):
                        filename = attribute.file_name
                        break
                    elif isinstance(attribute, DocumentAttributeVideo):
                        filename = f"{message.id}.mp4"
                        content_type = 'video'
            elif isinstance(media, MessageMediaPhoto):
                filename = f"{message.id}.jpeg"
                content_type = 'photo'
            elif isinstance(media, MessageMediaWebPage):
                if isinstance(media.webpage, WebPage):
                    url = print(media.webpage.url)
                content_type = 'web'


            # download media
            metadata = None
            if filename is not None:
                filepath = f'download/{channel.id}/{filename}'
                if not os.path.exists(filepath):
                    print(f"downloading {filepath}")
                    await self.client.download_media(message, filepath)
                else:
                    print(f"{filepath} already downloaded")
                metadata = filename
            elif url is not None:
                metadata = url
            #save message into DB:
            self.db.store_message({ 'id': message.id,
                               'chat_id': channel.id,
                               'date': message.date,
                               'sender_user_id': channel.id,
                               'content': {
                                   'text': message.message,
                                   '@type': content_type
                               },
                               'metadata': metadata
                               })

        return min_date, offset_id

    async def download_chat(self, chat_name, redownload = False):
        print(chat_name)
        await self.client.start()
        dialogs = await self.client.get_dialogs()
        dialog = list(filter(lambda dialog: dialog.name == chat_name, dialogs))[0]
        channel = dialog.entity
        self.db.store_chat( { 'id':channel.id, 'title':channel.title } )

        if redownload:
            lower_bound_date = None
        else:
            lower_bound_date = self.db.get_last_stored_message_date(channel.id)

        if lower_bound_date is None:
            lower_bound_date = datetime.fromisoformat("2022-02-24 00:00:00").replace(tzinfo=timezone(offset=timedelta()))

        date = datetime.now().replace(tzinfo=timezone(offset=timedelta()))
        offset_id = 0
        while date > lower_bound_date:
            date, offset_id = await self.download_history_batch(channel, offset_id)
            print(date, offset_id)
            if self.debug:
                break

async def main():
    # load config:
    with open('config.yml', 'r') as f:
        config = yaml.load(f, Loader)['default']
        print(config)

    connection_string = "dbname={} user={} password={} host={} port={}".format(config['dbname'],
                                                                               config['dbuser'],
                                                                               config['dbpassword'],
                                                                               config['dbhost'],
                                                                               config['dbport'])
    db = DB(connection_string=connection_string)
    client = TelegramClient(config['user_phone'], config['app_id'], config['api_hash'])
    debug = False
    if 'debug' in config:
        debug = config['debug']
    downloader = Downloader(db, client, debug = debug)
    for chat in config['chats']:
        await downloader.download_chat(chat, redownload = config['redownload'])

asyncio.run(main())

