import models

import sqlalchemy
import sqlalchemy.orm

import sqlalchemy_utils


class DbConnector(object):
    def __init__(self, db_type, db_name, host, username, password):
        self.db_type = db_type
        self.db_name = db_name
        self.host = host
        self.username = username
        self.password = password

    def build_db_url(self):
        if self.db_type in('mysql', 'postgres'):
            self.database_uri = '{}://{}:{}@{}/{}'.format(
                self.db_type, self.username,
                self.password, self.host, self.db_name)
        elif self.db_type == 'sqlite':
            self.database_uri = "sqlite:///{}.db".format(self.db_name)
        else:
            raise SystemExit('Database not supported')

    def create_db(self):
        try:
            if not sqlalchemy_utils.database_exists(self.database_uri):
                sqlalchemy_utils.create_database(self.database_uri)
        except sqlalchemy.exc.OperationalError:
            raise SystemExit('Database authentication error')

    def create_db_engine(self):
        self.engine = sqlalchemy.create_engine(self.database_uri)

    def create_db_table(self):
        models.User.metadata.create_all(self.engine)

    def create_session(self):
        Session = sqlalchemy.orm.sessionmaker(self.engine)
        self.session = Session()

    def check_proper_setup(self):
        self.build_db_url()
        self.create_db()
        self.create_db_engine()
        self.create_db_table()
        self.create_session()

    def save_bulk_record_db(self, record_set):
        try:
            self.session.bulk_save_objects(record_set)
            self.session.commit()
        except sqlalchemy.exc.DBAPIError:
            self.session.rollback()
            raise SystemExit("Database operation failed due to")
