from db import DB
import yaml
from yaml import Loader

def get_config():
    with open('config.yml', 'r') as f:
        config = yaml.load(f, Loader)['default']
    return config

def get_db():
    config = get_config()
    connection_string = "dbname={} user={} password={} host={} port={}".format(config['dbname'],
                                                                               config['dbuser'],
                                                                               config['dbpassword'],
                                                                               config['dbhost'],
                                                                               config['dbport'])
    db = DB(connection_string=connection_string)
    return db