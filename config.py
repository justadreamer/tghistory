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

    def sqlalchemy_connection_string(self):
        host = self.config['dbhost']
        port = self.config['dbport']
        if host is None or len(host) == 0: #use unix domain sockets
            connection_string=f"postgresql://{self.config['dbuser']}:{self.config['dbpassword']}@/{self.config['dbname']}"
        else:
            connection_string=f"postgresql://{self.config['dbuser']}:{self.config['dbpassword']}@{self.config['dbhost']}:{self.config['dbport']}/{self.config['dbname']}"
        if self.debug:
            print(connection_string)
        return connection_string