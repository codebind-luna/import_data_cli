import sqlalchemy
from sqlalchemy import create_engine, exc
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
from models import User


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
            self._database_uri = '{}://{}:{}@{}/{}'.format(
             db_type, username, password, host or 'localhost', db)
        elif db_type == 'sqlite':
            self._database_uri = "sqlite:///{}.db".format(db)
        else:
            raise SystemExit('Database not supported')

    def database_engine(self):
        try:
            if not database_exists(self.database_uri):
                create_database(self.database_uri)
            self.engine = create_engine(self.database_uri, echo=True)
        except sqlalchemy.exc.OperationalError:
            raise SystemExit('Database authentication error')

    def create_table(self):
        if not self.engine.dialect.has_table(self.engine, 'users'):
            User.metadata.create_all(self.engine)

    def create_session(self):
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def save_bulk_record_db(self, record_set):
        try:
            self.session.bulk_save_objects(record_set)
            self.session.commit()
        except sqlalchemy.exc.DBAPIError:
            self.session.rollback()
            raise SystemExit("Database operation failed due to")
