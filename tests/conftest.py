from sqlalchemy_utils import drop_database
from db_connector import DbConnector
import pytest


@pytest.fixture(scope='function')
def db_connection(request):

    def make_db_connection(db_type, db, username, password, hostname):
        db_conn = DbConnector((db_type, db, username, password, hostname))

        def tear_down():
            drop_database(db_conn.database_uri)
        request.addfinalizer(tear_down)
        return db_conn
    return make_db_connection
