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


from datetime import datetime
from datetime import timezone
from datetime import timedelta

#load config:
with open('config.yml','r') as f:
    config = yaml.load(f)['default']
    print(config)

# global service objects:

client = TelegramClient(config['user_phone'], config['app_id'], config['api_hash'])
client.start()

pp = pprint.PrettyPrinter(indent=2)
connection_string = "dbname={} user={} password={} host={} port={}".format(config['dbname'],
                                                               config['dbuser'],
                                                               config['dbpassword'],
                                                               config['dbhost'],
                                                               config['dbport'])
print(connection_string)
db = DB(connection_string=connection_string)

async def download_history_batch(channel, offset_id):
    history = await client(GetHistoryRequest(
        peer=channel,
        offset_id=offset_id,
        offset_date=None,
        add_offset=0,
        limit=100,
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

            print(filename)
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
            await client.download_media(message, f'download/{filename}')
            metadata = filename
        elif url is not None:
            metadata = url
        #save message into DB:
        db.store_message({ 'id': message.id,
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

async def main(chat_name):
    print(chat_name)
    dialogs = await client.get_dialogs()
    dialog = list(filter(lambda dialog: dialog.name == chat_name, dialogs))[0]
    channel = dialog.entity
    print(channel)
    db.store_chat( { 'id':channel.id, 'title':channel.title } )
    first_date = db.get_last_stored_message_date(channel.id)
    if first_date is None:
        first_date = datetime.fromisoformat("2022-02-24 00:00:00").replace(tzinfo=timezone(offset=timedelta()))
    date = datetime.now().replace(tzinfo=timezone(offset=timedelta()))
    offset_id = 0
    while date > first_date:
        date, offset_id = await download_history_batch(channel, offset_id)
        print(date, offset_id)


loop = asyncio.get_event_loop()
loop.run_until_complete(main(config['chats'][0]))
