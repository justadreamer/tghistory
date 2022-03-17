from config import *
import os

lines = []
lines.append("#generated content\n")
lines.append("python tg_channel_download.py\n")
lines.append("python tg_channel_upload_media.py\n")

psqllines = []
uploadlines = []
db = get_db()
chats = db.get_chats()
for chat in chats:
    id = chat[0]
    title = chat[1]
    filename = f'{title}.csv'
    psqllines.append(f'echo "\copy (select * from messages where chat_id={id} order by send_date) to \'{filename}\' delimiter \',\' csv header;" | psql -U tgclient tghistory\n')
    uploadlines.append(f'python upload.py "{filename}" tghistory\n')

with open('update.sh', 'w+') as f:
    f.writelines(lines)
    f.writelines(psqllines)
    f.writelines(uploadlines)

os.system("bash update.sh")