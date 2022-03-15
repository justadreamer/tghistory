py3.9 #alias
python tg_channel_download.py
python tg_channel_upload_media.py
echo "\copy (select * from messages where chat_id=1722167948 order by send_date) to PravdaGerashchenko.csv delimiter ',' csv header;" | psql -U tgclient tghistory 
python upload.py PravdaGerashchenko.csv tghistory
