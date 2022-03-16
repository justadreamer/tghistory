import psycopg2
import datetime

class DB:
    def __init__(self, connection_string):
        self.connection = psycopg2.connect(connection_string)
        recreate = False
        with self.connection:
            try:
                with self.connection.cursor() as cur:
                    cur.execute("select count(*) from messages")
                    print("DB has",cur.fetchone()[0], "messages")
            except psycopg2.ProgrammingError as e:
                print("could not get messages count recreating schema")
                recreate = True
        if recreate:
            self.recreate_schema()


    def __del__(self):
        self.connection.close()

    def recreate_schema(self):
        with open('schema.sql') as f:
            schema = f.read(-1)
            with self.connection:
                with self.connection.cursor() as cur:
                    cur.execute(schema)

    def store_message(self, message):
        text = ""
        if 'text' in message['content']:
            text = message['content']['text']

        content_type = message['content']['@type']
        #print('inserting:', message['id'], send_date, message['chat_id'], message['sender_user_id'], content_type, text)

        with self.connection:
            with self.connection.cursor() as cur:
                cur.execute(
                    """insert into messages 
                    (id, send_date, chat_id, sender_user_id, content_type, message_text, metadata) 
                    values (%s, %s, %s, %s, %s, %s, %s)
                    on conflict(id) do nothing""",
                    (message['id'], message['date'], message['chat_id'], message['sender_user_id'], content_type, text, message['metadata']))

    def store_messages(self, messages):
        for message in messages:
            self.store_message(message)


    def store_chat(self, chat):
        with self.connection:
            with self.connection.cursor() as cur:
                cur.execute("insert into chats (id, title) values (%s, %s) on conflict do nothing", (chat['id'], chat['title']))


    def store_user(self, user):
        with self.connection:
            with self.connection.cursor() as cur:
                cur.execute("insert into users (id, first_name, last_name, username, type) "
                        "values (%s, %s, %s, %s, %s) on conflict do nothing",
                        (user['id'], user['first_name'], user['last_name'], user['username'], user['type']['@type']))

    def get_last_stored_message_id(self, chat_id):
        with self.connection.cursor() as cur:
            cur.execute("select max(id) id from messages where chat_id = "+ str(chat_id))
            record = cur.fetchone()
            max_id = record[0]
            if max_id is None:
                max_id = 0
            return max_id

    def get_last_stored_message_date(self, chat_id):
        with self.connection.cursor() as cur:
            cur.execute("select max(send_date) date from messages where chat_id = "+ str(chat_id))
            record = cur.fetchone()
            max_date = record[0]
            return max_date

    def get_messages(self, chat_id):
        with self.connection.cursor() as cur:
            cur.execute("select * from messages where chat_id = "+str(chat_id))
            records = cur.fetchall()
            return records

    def get_message(self, message_id):
        with self.connection:
            with self.connection.cursor() as cur:
                cur.execute("select * from messages where id = %s", [message_id])
                record = cur.fetchone()
                return record

    def update_message_upload(self, message_id, uploaded):
        with self.connection:
            with self.connection.cursor() as cur:
                print(f'updating message id={message_id} with uploaded={uploaded}')
                cur.execute('update messages set uploaded=%s where id=%s', (uploaded, message_id))