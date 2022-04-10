from config import *
import os

lines = []
lines.append("#generated content\n")
lines.append("python tg_channel_download.py\n")
lines.append("python tg_channel_upload_media.py\n")
from db import *

psqllines = []
uploadlines = []

config = Config()
engine = create_sqlachemy_engine(config.sqlalchemy_connection_string())
sessionfactory = session_factory(engine)
session = scoped_session(sessionfactory)()

chats = session.query(Chat).all()

for chat in chats:
    filename = f'{chat.username}.csv'
    psqllines.append(f'echo "\copy (select * from messages where chat_id={chat.id} order by send_date) to \'{filename}\' delimiter \',\' csv header;" | psql -U tgclient tghistory\n')
    uploadlines.append(f'python upload.py "{filename}" "{filename}" tghistory\n')

with open('update.sh', 'w+') as f:
    f.writelines(lines)
    f.writelines(psqllines)
    f.writelines(uploadlines)

os.system("bash update.sh")