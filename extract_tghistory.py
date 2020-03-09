"""
App configuration
App api_id:
1209350
App api_hash:
05af397f1571f3e439f4d2eafe0d809a
App title:
MyChatsStats
Short name:
mychatstats
alphanumeric, 5-32 characters

Available MTProto servers
Test configuration:
149.154.167.40:443
DC 2

Production configuration:
149.154.167.50:443
DC 2

Public keys:
-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAwVACPi9w23mF3tBkdZz+zwrzKOaaQdr01vAbU4E1pvkfj4sqDsm6
lyDONS789sVoD/xCS9Y0hkkC3gtL1tSfTlgCMOOul9lcixlEKzwKENj1Yz/s7daS
an9tqw3bfUV/nqgbhGX81v/+7RFAEd+RwFnK7a+XYl9sluzHRyVVaTTveB2GazTw
Efzk2DWgkBluml8OREmvfraX3bkHZJTKX4EQSjBbbdJ2ZXIsRrYOXfaA+xayEGB+
8hdlLmAjbCVfaigxX0CDqWeR1yFL9kwd9P0NsZRPsmoqVwMbMu7mStFai6aIhc3n
Slv8kg9qv1m6XHVQY3PnEw+QQtqSIXklHwIDAQAB
-----END RSA PUBLIC KEY-----

-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAruw2yP/BCcsJliRoW5eB
VBVle9dtjJw+OYED160Wybum9SXtBBLXriwt4rROd9csv0t0OHCaTmRqBcQ0J8fx
hN6/cpR1GWgOZRUAiQxoMnlt0R93LCX/j1dnVa/gVbCjdSxpbrfY2g2L4frzjJvd
l84Kd9ORYjDEAyFnEA7dD556OptgLQQ2e2iVNq8NZLYTzLp5YpOdO1doK+ttrltg
gTCy5SrKeLoCPPbOgGsdxJxyz5KKcZnSLj16yE5HvJQn0CNpRdENvRUXe6tBP78O
39oJ8BTHp9oIjd6XWXAsp2CvK45Ol8wFXGF710w9lwCGNbmNxNYhtIkdqfsEcwR5
JwIDAQAB
-----END PUBLIC KEY-----

-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvfLHfYH2r9R70w8prHbl
Wt/nDkh+XkgpflqQVcnAfSuTtO05lNPspQmL8Y2XjVT4t8cT6xAkdgfmmvnvRPOO
KPi0OfJXoRVylFzAQG/j83u5K3kRLbae7fLccVhKZhY46lvsueI1hQdLgNV9n1cQ
3TDS2pQOCtovG4eDl9wacrXOJTG2990VjgnIKNA0UMoP+KF03qzryqIt3oTvZq03
DyWdGK+AZjgBLaDKSnC6qD2cFY81UryRWOab8zKkWAnhw2kFpcqhI0jdV5QaSCEx
vnsjVaX0Y1N0870931/5Jb9ICe4nweZ9kSDF/gip3kWLG0o8XQpChDfyvsqB9OLV
/wIDAQAB
-----END PUBLIC KEY-----

-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAs/ditzm+mPND6xkhzwFI
z6J/968CtkcSE/7Z2qAJiXbmZ3UDJPGrzqTDHkO30R8VeRM/Kz2f4nR05GIFiITl
4bEjvpy7xqRDspJcCFIOcyXm8abVDhF+th6knSU0yLtNKuQVP6voMrnt9MV1X92L
GZQLgdHZbPQz0Z5qIpaKhdyA8DEvWWvSUwwc+yi1/gGaybwlzZwqXYoPOhwMebzK
Uk0xW14htcJrRrq+PXXQbRzTMynseCoPIoke0dtCodbA3qQxQovE16q9zz4Otv2k
4j63cz53J+mhkVWAeWxVGI0lltJmWtEYK6er8VqqWot3nqmWMXogrgRLggv/Nbbo
oQIDAQAB
-----END PUBLIC KEY-----

-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvmpxVY7ld/8DAjz6F6q0
5shjg8/4p6047bn6/m8yPy1RBsvIyvuDuGnP/RzPEhzXQ9UJ5Ynmh2XJZgHoE9xb
nfxL5BXHplJhMtADXKM9bWB11PU1Eioc3+AXBB8QiNFBn2XI5UkO5hPhbb9mJpjA
9Uhw8EdfqJP8QetVsI/xrCEbwEXe0xvifRLJbY08/Gp66KpQvy7g8w7VB8wlgePe
xW3pT13Ap6vuC+mQuJPyiHvSxjEKHgqePji9NP3tJUFQjcECqcm0yV7/2d0t/pbC
m+ZH1sadZspQCEPPrtbkQBlvHb4OLiIWPGHKSMeRFvp3IWcmdJqXahxLCUS1Eh6M
AQIDAQAB
-----END PUBLIC KEY-----
"""

from telegram.client import Telegram
import pprint
from postgres_connection import DB
from config import *

# global service objects:

tg = Telegram(
    api_id=API_ID,
    api_hash=API_HASH,
    phone=USER_PHONE,
    database_encryption_key='',
    use_message_database=True
)

pp = pprint.PrettyPrinter(indent=2)

db = DB(DB_CONNECTION_STRING)

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
chats = get_chats_by_titles(chats, CHAT_TITLES)
chat_ids = list(map(lambda chat: chat['id'], chats))
fetch_and_store_chats_history(chat_ids)
