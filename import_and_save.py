import click
import csv
from importlib import import_module

def save_into_mysql(filename, username, password):
    try:
        module = import_module('MySQLdb')
        db_name = 'cli_test'
        mydb_conn = module.connect(user=username, passwd=password)
        cur = mydb_conn.cursor()
        cur.execute("""CREATE DATABASE IF NOT EXISTS """ + db_name)
        mydb_conn = module.connect(user=username, passwd=password, db=db_name)
        cur = mydb_conn.cursor()
        csv_data = csv.reader(file(filename))
        lst = ['Name', 'Age', 'Address']
        table_name = "People"
        createsqltable = """CREATE TABLE IF NOT EXISTS """ + table_name + " (" + " VARCHAR(250),".join(lst) + " VARCHAR(250))"
        cur.execute(createsqltable)
        for row in csv_data:
            cur.execute('INSERT INTO People(Name,Age,Address)''VALUES(%s, %s, %s)',row)
        mydb_conn.commit()
        cur.close()
    except:
        print('Sorry Something went wrong')

def save_into_sqlite(filename):
    try:
        import sqlite3
        con = sqlite3.connect("my_test.db")
        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS t(Name, Age, Address);") # use your column names here
        with open(filename,'rb') as fin: # `with` statement available in 2.5+
        # csv.DictReader uses first line in file for column headings by default
            dr = csv.DictReader(fin) # comma is default delimiter
            to_db = [(i['Name'], i['Age'], i['Address']) for i in dr]
        cur.executemany("INSERT INTO t (Name, Age, Address) VALUES (?, ?, ?);", to_db)
        con.commit()
        con.close()
    except:
        print('Sorry Something went wrong')

def save_into_postgres(filename, username, password):
    try:
        module = import_module('psycopg2')
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
        db_name = 'cli_test'
        mydb_conn = module.connect("host=localhost user={} password={}".format(username, password))
        mydb_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = mydb_conn.cursor()
        cur.execute("SELECT COUNT(*) = 0 FROM pg_catalog.pg_database WHERE datname = 'cli_test'")
        not_exists_row = cur.fetchone()
        not_exists = not_exists_row[0]
        if not_exists:
            cur.execute('CREATE DATABASE cli_test')
        mydb_conn = module.connect("host=localhost dbname={} user={} password={}".format(db_name, username, password))
        mydb_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = mydb_conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS people(Name text, Age integer, Address text)")
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            next(reader)  
            for row in reader:
                cur.execute("INSERT INTO people VALUES (%s, %s, %s)", row)
        cur.close()
    except:
        print('Sorry Something went wrong')


@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.argument('database_type')
@click.option('--username', default='', help='Username database')
@click.password_option(default='')

def import_and_save(file_path, database_type=None, username=None, password=None):
    """Simple program that imports a csv file and save it to database."""
    if database_type == 'mysql':
        save_into_mysql(file_path, username, password)
    elif database_type == 'sqlite':
        save_into_sqlite(file_path)
    elif database_type == 'postgres':
        save_into_postgres(file_path, username, password)

if __name__ == '__main__':
    import_and_save()
