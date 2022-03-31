from telethon import TelegramClient
from config import *
import asyncio
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.channels import GetChannelsRequest

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
from pprint import pprint

class ChannelHistoryDownloader:
    def __init__(self, db, tgclient, username, debug = False):
        self.db = db
        self.tgclient = tgclient
        self.username = username
        self.debug = debug


    async def download_history_batch(self, channel, offset_id):
        history = await self.tgclient(GetHistoryRequest(
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

        for (i,message) in enumerate(messages):
            print(f"{channel.username} {i}/{len(messages)} d={message.date.isoformat()}")

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
                    dbmessage = self.db.get_message(message_id=message.id, chat_id=channel.id)
                    uploaded = None
                    if dbmessage is not None:
                        uploaded = dbmessage[len(dbmessage) - 1]
                    if uploaded is None:
                        print(f"downloading {filepath}")
                        await self.tgclient.download_media(message, filepath)
                    else:
                        print(f"media {filepath} already uploaded to gdrive")
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

    async def resolve_channel(self, username):
        channels = await self.tgclient(GetChannelsRequest(id=[username]))
        channel = channels.chats[0]
        if channel is not None:
            self.db.store_chat({'id': channel.id, 'title': channel.title, 'username': channel.username})
        return channel

    async def download_channel_history(self, redownload = False):
        channel = await self.resolve_channel(self.username)
        if channel is None:
            print(f"could not find channel for {self.username}")
        else:
            print(f"channel {channel.username} resolved")

        if redownload:
            lower_bound_date = None
        else:
            lower_bound_date = self.db.get_last_stored_message_date(channel.id)

        if lower_bound_date is None:
            lower_bound_date = datetime.fromisoformat("2022-02-24 00:00:00").replace(tzinfo=timezone(offset=timedelta()))

        date = datetime.now().replace(tzinfo=timezone(offset=timedelta()))
        offset_id = 0
        while date > lower_bound_date:
            print(f"BATCH for {channel.username} from {date}, offset={offset_id}")
            date, offset_id = await self.download_history_batch(channel, offset_id)

            if self.debug:
                break

async def main():
    config = get_config()
    pprint(config)

    db = get_db()

    tgclient = TelegramClient(config['user_phone'], config['app_id'], config['api_hash'])
    await tgclient.start()

    debug = False
    if 'debug' in config:
        debug = config['debug']

    downloads = []
    for username in config['chats']:
        downloader = ChannelHistoryDownloader(db, tgclient, username, debug=debug)
        downloads.append(downloader.download_channel_history(redownload = config['redownload']))

    await asyncio.wait(downloads)

asyncio.run(main())

