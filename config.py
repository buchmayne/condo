from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


def create_connection_obj():
    """Creates engine object from database.ini configuration"""
    filename = "database.ini"
    parser = ConfigParser()
    parser.read(filename)

    params = {k: v for k, v in parser.items("postgresql")}
    conn = create_engine(URL.create(**params))
    return conn
