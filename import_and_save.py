import click
from file_importer import FileLoader
from db_connector import DbConnector

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.argument('database_type')
@click.argument('db')
@click.option('--username', default='', help='Username database')
@click.option('--hostname', default='', help='Username database')
@click.password_option(default='')

def import_and_save(file_path, db, database_type, username='', password='', hostname=''):
    """Simple program that imports a csv file and save it to database."""

    db_conn = DbConnector((database_type, db, username, password, hostname))
    file_importer = FileLoader(file_path)
    file_importer.load_data(db_conn)

if __name__ == '__main__':
    import_and_save()
