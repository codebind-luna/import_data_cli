import sqlalchemy
from sqlalchemy import create_engine, exc
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import sqlite3
import MySQLdb
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, String

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'

    id          =   Column(Integer, primary_key=True)
    name        =   Column(String(36))
    age         =   Column(Integer)
    address     =   Column(String(36))


class DbConnector(object):
    def __init__(self, val):
        self.database_uri = val
        self.database_engine()
        self.create_table()
        self.create_session()

    @property
    def database_uri(self):
        return self._database_uri

    @database_uri.setter
    def database_uri(self, val):
        db_type, db, username, password, host = val
        if db_type == 'mysql' or db_type == 'postgres':
            self._database_uri = '{}://{}:{}@{}/{}'.format(db_type, username, password, host or 'localhost', db)
        elif db_type == 'sqlite':
            self._database_uri = "sqlite:///{}.db".format(db)
        else:
            print('Database not supported')

    def database_engine(self):
        try:
            if not database_exists(self.database_uri):
                create_database(self.database_uri)
            self.engine = create_engine(self.database_uri, echo=True)
        except sqlalchemy.exc.OperationalError:
            print('Database Authentication Error')
            exit()

    def create_table(self):
        if not self.engine.dialect.has_table(self.engine, 'users'):
            Base.metadata.create_all(self.engine)

    def create_session(self):
        session = sessionmaker()
        session.configure(bind=self.engine)
        self.s = session()

    def close_db(self):
        self.s.close()

    def rollback_db(self):
        self.s.rollback()
