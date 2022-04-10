import psycopg2
import datetime

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, Date, DECIMAL, TIMESTAMP, BigInteger
from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session

from sqlalchemy.orm import declarative_base
Base = declarative_base()

def create_sqlachemy_engine(connection_string):
    engine = create_engine(connection_string, pool_size = 64)
    return engine

def session_factory(engine):
    return sessionmaker(bind=engine)

class Chat(Base):
    __tablename__ = 'chats'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    username = Column(String)

    @staticmethod
    def from_tg_channel(channel):
        return Chat(id=channel.id, title=channel.title, username=channel.username)

class Message(Base):
    __tablename__ = 'messages'
    id = Column(Integer, primary_key=True)
    send_date = Column(TIMESTAMP, nullable=False)
    chat_id = Column(BigInteger, primary_key = True)
    sender_user_id = Column(BigInteger, nullable=False)
    content_type = Column(String)
    message_text = Column(String)
    _metadata = Column('metadata', String)
    uploaded = Column(String)
