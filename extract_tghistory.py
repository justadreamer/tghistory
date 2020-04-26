from telegram.client import Telegram
import pprint
from postgres_connection import DB
import yaml

#load config:
with open('config.yml','r') as f:
    config = yaml.load(f)['default']
    print(config)

# global service objects:

tg = Telegram(
    api_id=config['app_id'],
    api_hash=config['api_hash'],
    phone=config['user_phone'],
    database_encryption_key='',
    use_message_database=False
)

pp = pprint.PrettyPrinter(indent=2)
connection_string = "dbname={} user={} password={} host={} port={}".format(config['dbname'],
                                                               config['dbuser'],
                                                               config['dbpassword'],
                                                               config['dbhost'],
                                                               config['dbport'])
print(connection_string)
db = DB(connection_string=connection_string)

# end global service objects definitions


def fetch_and_store_chats(chat_ids):
    chats = []
    for id in chat_ids:
        chat_result = tg.get_chat(id)
        chat_result.wait()
        chat = chat_result.update
        chats += [chat]
        db.store_chat(chat)
    return chats


def get_chats_by_titles(chats, titles):
    filtered = filter(lambda chat: chat['title'] in titles, chats)
    return list(filtered)


def fetch_and_store_users(user_ids):
    for user_id in user_ids:
        res = tg.get_user(user_id)
        res.wait()
        user = res.update
        db.store_user(user)


def get_last_stored_message_id(chat_id):
    return db.get_last_stored_message_id(chat_id)


def fetch_and_store_chats_history(chat_ids):
    user_ids = set() # we accumulate user_ids while fetching chat messages to later fetch users

    for chat_id in chat_ids:
        print('Fetching messages for chat_id', chat_id)
        last_stored_message_id = get_last_stored_message_id(chat_id)
        print("chat_id =", chat_id, "last_stored_message_id =", last_stored_message_id);
        last_message_id = 0
        count = 0
        while True:
            offset = -99 if last_message_id == 0 else 0
            history_result = tg.get_chat_history(chat_id, from_message_id=last_message_id, offset=offset)
            history_result.wait()
            messages = history_result.update['messages']
            count += len(messages)
            db.store_messages(messages)
            sender_user_ids = list(map(lambda x: x['sender_user_id'], messages))
            for sender_user_id in sender_user_ids:
                user_ids.add(sender_user_id)

            if len(messages) == 0 or (last_message_id > 0 and last_stored_message_id > last_message_id):
                break
            else:
                last_message_id = messages[len(messages) - 1]['id']
        print('Total fetched for chat_id=', chat_id, count, 'messages')

    fetch_and_store_users(list(user_ids))

# import datetime
# message = {'id':11647582208, 'date': datetime.datetime.fromisoformat("2020-03-05 21:38:35").timestamp(),
#            'chat_id':-1001198134786, 'sender_user_id': 574136325, 'content':
#                {'@type':'messageText', 'text': { 'text': """Арнольд Шварценеггер подал в суд на российскую компанию. Она создала робота-няню с его лицом 🤖 — Meduza
# https://meduza.io/shapito/2020/03/05/arnold-shvartsenegger-podal-v-sud-na-rossiyskuyu-kompaniyu-ona-sozdala-robota-nyanyu-s-ego-litsom"""} } }
#
# db.store_message(message)


tg.login()
chats_result = tg.get_chats(offset_order=9223372036854775807)
chats_result.wait()
chat_ids = chats_result.update['chat_ids']
chats = fetch_and_store_chats(chat_ids)
chats = get_chats_by_titles(chats, config['chats'])
chat_ids = list(map(lambda chat: chat['id'], chats))
fetch_and_store_chats_history(chat_ids)
