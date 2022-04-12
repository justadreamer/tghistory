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

from sqlalchemy.orm import session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from db import create_sqlachemy_engine
from db import session_factory
from db import Chat
from db import Message



class ChannelHistoryDownloader:
    def __init__(self, sessionfactory, tgclient, username, debug = False):
        self.sessionfactory = sessionfactory
        self.tgclient = tgclient
        self.username = username
        self.debug = debug

    def get_scoped_session(self):
        Session = scoped_session(self.sessionfactory)
        session = Session()
        return session

    async def download_history_batch(self, channel, date):
        session = self.get_scoped_session()
        history = await self.tgclient(GetHistoryRequest(
            peer=channel,
            offset_id=0,
            offset_date=date,
            add_offset=0,
            limit=100,  # batch should be of significant size, so that we don't have this amount of messages per single date - f.e. a bunch of photos sent simultaneously, otherwise we'll stop fetching channel early
            max_id=0,
            min_id=0,
            hash=0
        ))
        min_date = date
        messages = history.messages

        for (i,message) in enumerate(messages):
            print(f"{channel.username} {i}/{len(messages)} id={message.id} d={message.date.isoformat()}")
            dbmessage = session.query(Message).where(Message.id == message.id, Message.chat_id == channel.id).first()

            date = message.date
            if date < min_date:
                min_date = date

            # figure out content type and metadata for media (filename, url etc.)
            media = message.media
            filename = None  #extract either filename or url as metadata to store with the message record in the DB
            url = None
            content_type = 'text'
            if isinstance(media, MessageMediaDocument):
                attributes: [DocumentAttributeFilename] = message.media.document.attributes
                for attribute in attributes:
                    if isinstance(attribute, DocumentAttributeFilename):
                        ext = os.path.splitext(attribute.file_name)[1]
                        filename = f"{message.id}{ext}"
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

            # download media if needed
            metadata = None
            if filename is not None:
                filepath = f'download/{channel.id}/{filename}'
                if os.path.exists(filepath):
                    print(f"media already downloaded: {filepath}")
                    continue
                else:
                    if dbmessage is not None and dbmessage.uploaded is not None and len(dbmessage.uploaded)>0:
                        print(f"media already uploaded: {filepath}")
                        continue
                    else:
                        print(f"downloading {filepath}")
                        if not self.debug:
                            await self.tgclient.download_media(message, filepath)
                metadata = filename
            elif url is not None:
                metadata = url


            #save message into DB:
            if dbmessage is None:
                dbmessage = Message(id=message.id, chat_id=channel.id, send_date=message.date, sender_user_id=channel.id,
                                content_type=content_type, _metadata=metadata, message_text=message.message)
                session.add(dbmessage)
            else:
                dbmessage._metadata = metadata #update metadata
            session.commit()

        return min_date

    async def resolve_channel(self, username):
        session = self.get_scoped_session()
        channels = await self.tgclient(GetChannelsRequest(id=[username]))
        channel = channels.chats[0]
        if channel is not None:
            chat = session.query(Chat).where(Chat.id == channel.id).first()
            if chat is not None:
                chat.title = channel.title
                chat.username = channel.username
            else:
                chat = Chat.from_tg_channel(channel)
                session.add(chat)
            session.commit()
        return channel

    async def download_channel_history(self, redownload = False):
        session = self.get_scoped_session()
        channel = await self.resolve_channel(self.username)
        if channel is None:
            print(f"could not find channel for {self.username}")
        else:
            print(f"channel {channel.username} resolved")


        lower_bound_date = None
        if not redownload:
            message = session\
                .query(Message)\
                .where(Message.chat_id == channel.id)\
                .order_by(Message.send_date.desc())\
                .limit(1)\
                .first()
            if message is not None:
                lower_bound_date = message.send_date

        if lower_bound_date is None:
            lower_bound_date = datetime.fromisoformat("2022-02-24 00:00:00").replace(tzinfo=timezone(offset=timedelta()))

        date = datetime.now().replace(tzinfo=timezone(offset=timedelta()))
        while date > lower_bound_date:
            print(f"BATCH for {channel.username} from {date}")
            new_date = await self.download_history_batch(channel, date)
            print(new_date)
            if new_date >= date:
                break
            date = new_date
            #if self.debug:
             #   break

async def main():
    config = Config()
    engine = create_sqlachemy_engine(config.sqlalchemy_connection_string())
    sessionfactory = sessionmaker(engine)

    tgclient = TelegramClient(config.user_phone, config.app_id, config.api_hash)
    await tgclient.start()

    debug = config.debug

    downloads = list(map(lambda username: ChannelHistoryDownloader(sessionfactory, tgclient, username, debug=debug)
                         .download_channel_history(redownload = config.redownload), config.chats)
                     )

    await asyncio.wait(downloads)

asyncio.run(main())
