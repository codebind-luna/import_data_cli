from models import User
import pytest
from db_connector import DbConnector


@pytest.fixture(scope='module')
def users_list():
    return [User(name='Henry', age=20, address='hyderabad'),
            User(name='James', age=30, address='Bengaluru')]


def test_Database_not_supported_wrong_database_type():
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        DbConnector(('DB2', 'test_pg_database',
                     'root', 'root', 'localhost'))
        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 42
        assert pytest_wrapped_e.value.message == 'Database not supported'


def test_database_authentication_error_wrong_username_password():
    with pytest.raises(SystemExit) as e:
        DbConnector(('postgres', 'test_pg_database',
                     'root', 'root', 'localhost'))
        assert e.type == SystemExit
        assert e.value.code == 42
        assert e.value.message == 'Database Authentication Error'


def test_save_bulk_record_postgres_db(db_connection, users_list):
    db_conn = db_connection('postgres',
                            'test_pg_database',
                            'sammy',
                            'sammy',
                            'localhost')
    db_conn.save_bulk_record_db(users_list)
    assert db_conn.session.query(User).count() == 2


def test_save_bulk_record_sqlite_db(db_connection, users_list):
    db_conn = db_connection('sqlite',
                            'test_sqlite_database',
                            '',
                            '',
                            'localhost')
    db_conn.save_bulk_record_db(users_list)
    assert db_conn.session.query(User).count() == 2
