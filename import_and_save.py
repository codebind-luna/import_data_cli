import click

from db_connector import DbConnector

from file_loader import FileLoader


@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.argument('database_type')
@click.argument('db')
@click.option('--username', default='', help='Username database')
@click.option('--hostname', default='', help='Username database')
@click.password_option(default='')
def import_and_save(file_path, db, database_type,
                    username='', password='', hostname='localhost'):
    """Simple program that imports a csv file and save it to database."""
    db_conn = DbConnector(database_type, db, hostname, username, password)
    db_conn.check_proper_setup()
    file_importer = FileLoader.factory(file_path)
    file_importer.load_data(db_conn)


if __name__ == '__main__':
    import_and_save()
