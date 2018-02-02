import csv
from db_connector import User

class FileLoader(object):
    def __init__(self, val):
        self.file = val

    def load_data(self, db_conn):
        try:
            with open(self.file,'rb') as input_file:
                csv_data = csv.reader(input_file)
                next(csv_data, None)
                for i in csv_data:
                    record = User(**{
                          'name' : i[0],
                          'age' : i[1],
                          'address' : i[2]
                          })
                    db_conn.s.add(record)
                    db_conn.s.commit() 
        except:
            db_conn.rollback_db()
        finally:
            db_conn.close_db()
