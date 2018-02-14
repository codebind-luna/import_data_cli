from db_connector import DbConnector
import mock
from mock import Mock
import sqlalchemy
import pytest
import sqlalchemy.orm


def test_build_db_url_postgres():
    connector = DbConnector('postgres', 'test_pg_db', 'localhost',
                            'my_user', 'my_pass')
    connector.build_db_url()
    assert connector.database_uri \
        == 'postgres://my_user:my_pass@localhost/test_pg_db'


def test_build_db_url_mysql():
    connector = DbConnector('mysql', 'test_mysql_db', 'localhost',
                            'my_user', 'my_pass')
    connector.build_db_url()
    assert connector.database_uri \
        == 'mysql://my_user:my_pass@localhost/test_mysql_db'


def test_build_db_url_sqlite():
    connector = DbConnector('sqlite', 'test_sqlite', '', '', '')
    connector.build_db_url()
    assert connector.database_uri == 'sqlite:///test_sqlite.db'


def test_build_db_url_anything_arbitrary():
    with pytest.raises(SystemExit) as e:
        connector = DbConnector('DB2', 'test_DB2_db', 'localhost',
                                'my_user', 'my_pass')
        connector.build_db_url()
        assert e.type == SystemExit
        assert e.value.code == 42
        assert e.value.message == 'Database not supported'


@mock.patch('db_connector.sqlalchemy_utils')
def test_create_db(mock_sqlalchemy_utils):
    mock_sqlalchemy_utils.database_exists = Mock(return_value=False)
    mock_sqlalchemy_utils.create_database = \
        Mock(side_effect=sqlalchemy.exc.OperationalError(None, None,
             None, False, None))
    with pytest.raises(SystemExit) as e:
        connector = DbConnector('mysql', 'test_mysql_db', 'localhost',
                                'my_user', 'my_pass')
        connector.database_uri = 'any_path'
        connector.create_db()
        assert e.type == SystemExit
        assert e.value.code == 42
        assert e.value.message == 'Database Authentication Error'


@mock.patch('db_connector.sqlalchemy_utils')
def test_create_db_called(mock_sqlalchemy_utils):
    mock_sqlalchemy_utils.database_exists = Mock(return_value=False)
    mock_sqlalchemy_utils.create_database = Mock(return_value=True)
    connector = DbConnector('mysql', 'test_mysql_db', 'localhost',
                            'my_user', 'my_pass')
    connector.database_uri = 'any_path'
    connector.create_db()
    mock_sqlalchemy_utils.create_database.assert_called_once_with('any_path')


@mock.patch('db_connector.sqlalchemy_utils')
def test_create_db_when_already_exists(mock_sqlalchemy_utils):
    mock_sqlalchemy_utils.database_exists = Mock(return_value=True)
    connector = DbConnector('mysql', 'test_mysql_db', 'localhost',
                            'my_user', 'my_pass')
    connector.database_uri = 'any_path'
    connector.create_db()
    mock_sqlalchemy_utils.create_database.assert_not_called


@mock.patch('db_connector.sqlalchemy')
def test_create_db_engine(mock_sqlalchemy):
    mock_sqlalchemy.create_engine = Mock(return_value=True)
    connector = DbConnector('mysql', 'test_mysql_db', 'localhost',
                            'my_user', 'my_pass')
    connector.database_uri = 'any_path'
    connector.create_db_engine()
    mock_sqlalchemy.create_engine.assert_called_once_with('any_path')
    assert str(connector.engine) == 'True'


@mock.patch('db_connector.models.User.metadata')
def test_create_db_table(mock_user_model_metadata):
    mock_user_model_metadata.create_all = Mock(return_value=True)
    connector = DbConnector('mysql', 'test_mysql_db', 'localhost',
                            'my_user', 'my_pass')
    connector.engine = 'any_thing'
    connector.create_db_table()
    mock_user_model_metadata.create_all.assert_called_once_with('any_thing')


@mock.patch('db_connector.sqlalchemy.orm')
def test_create_session_sqlalchemy_sessionmaker(mock_sqlalchemy_orm):
    MockSessionCls = Mock(return_value=True)
    mock_sqlalchemy_orm.sessionmaker = Mock(return_value=MockSessionCls)
    connector = DbConnector('mysql', 'test_mysql_db', 'localhost',
                            'my_user', 'my_pass')
    connector.engine = 'any_thing'
    connector.create_session()
    mock_sqlalchemy_orm.sessionmaker.assert_called_once_with('any_thing')


@mock.patch('db_connector.sqlalchemy.orm')
def test_create_session_sqlalchemy_sessionmaker_returns(mock_sqlalchemy_orm):
    MockSessionCls = Mock(return_value=True)
    mock_sqlalchemy_orm.sessionmaker = Mock(return_value=MockSessionCls)
    connector = DbConnector('mysql', 'test_mysql_db', 'localhost',
                            'my_user', 'my_pass')
    connector.engine = 'any_thing'
    connector.create_session()
    MockSessionCls.assert_called_with()


@mock.patch('db_connector.sqlalchemy.orm')
def test_create_session_returns_properly(mock_sqlalchemy_orm):
    MockSessionCls = Mock(return_value=True)
    mock_sqlalchemy_orm.sessionmaker = Mock(return_value=MockSessionCls)
    connector = DbConnector('mysql', 'test_mysql_db', 'localhost',
                            'my_user', 'my_pass')
    connector.engine = 'any_thing'
    connector.create_session()
    assert str(connector.session) == 'True'


def test_bulk_record_db_called_with_proper_parameters():
    connector = DbConnector('mysql', 'test_mysql_db', 'localhost',
                            'my_user', 'my_pass')
    connector.session = sqlalchemy.orm.session.Session()
    connector.session.bulk_save_objects = Mock(return_value=True)
    connector.session.commit = Mock(return_value=True)
    users_record_set = []
    connector.save_bulk_record_db(users_record_set)
    connector.session.bulk_save_objects.assert_called_once_with(
        users_record_set)


def test_commit_is_called():
    connector = DbConnector('mysql', 'test_mysql_db', 'localhost',
                            'my_user', 'my_pass')
    connector.session = sqlalchemy.orm.session.Session()
    connector.session.bulk_save_objects = Mock(return_value=True)
    connector.session.commit = Mock(return_value=True)
    users_record_set = []
    connector.save_bulk_record_db(users_record_set)
    connector.session.commit.assert_called_with()


def test_check_proper_setup():
    connector = DbConnector('mysql', 'test_mysql_db', 'localhost',
                            'my_user', 'my_pass')
    connector.build_db_url = Mock(return_value=None)
    connector.create_db = Mock(return_value=None)
    connector.create_db_engine = Mock(return_value=None)
    connector.create_db_table = Mock(return_value=None)
    connector.create_session = Mock(return_value=None)
    connector.check_proper_setup()
    connector.build_db_url.assert_called_with()
    connector.create_db.assert_called_with()
    connector.create_db_engine.assert_called_with()
    connector.create_db_table.assert_called_with()
    connector.create_session.assert_called_with()
