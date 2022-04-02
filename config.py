from db import DB
import yaml
from yaml import Loader

class Config:
    def __init__(self):
        with open('config.yml', 'r') as f:
            self.config = yaml.load(f, Loader)['default']
        config = self.config
        self.download_dir = config.get('download_dir', "download")
        self.upload_dir = config.get('gdrive_upload_dir', "tghistory")
        self.bucket_name = config.get('bucket_name', "tghistory")
        self.user_phone = config.get('user_phone')
        self.api_hash = config.get('api_hash')
        self.app_id = config.get('app_id')
        self.redownload = config.get('redownload')
        self.debug = config.get('debug')
        self.chats = config.get('chats')
        self.download_dir = config.get('download_dir')
        self.upload_dir = config.get('upload_dir')
        self.bucket_name = config.get('bucket_name')

    def get_db(self):
        config = self.config
        connection_string = "dbname={} user={} password={} host={} port={}".format(config['dbname'],
                                                                                   config['dbuser'],
                                                                                   config['dbpassword'],
                                                                                   config['dbhost'],
                                                                                   config['dbport'])
        db = DB(connection_string=connection_string)
        return db
