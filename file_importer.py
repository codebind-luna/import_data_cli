import csv
import os
import pandas as pd
import xlrd
from db_connector import User

class FileLoader(object):
    def __init__(self, path_to_file):
        self.filename, self.file_extension = os.path.splitext(path_to_file)

    def xlsx_data_import_db(self, file_buffer, db_conn):
        df = pd.read_excel(file_buffer, sheet_name='Sample-spreadsheet-file')

        for i in df.index:
            record = User(**{'name' : df['Name'][i],'age' : df['Age'][i],'address' : df['Address'][i]})
            db_conn.save_record_db(record)

    def csv_data_import_db(self, file_buffer, db_conn):
        csv_data = csv.reader(file_buffer)
        next(csv_data, None)

        for i in csv_data:
            record = User(**{'name' : i[0],'age' : i[1],'address' : i[2]})
            db_conn.save_record_db(record)

    def xls_data_import_db(self, file_buffer, db_conn):
        book = xlrd.open_workbook(file_contents=file_buffer.read())

        for sheet_i in range(book.nsheets):
            sheet = book.sheet_by_index(sheet_i)
            for r in range(1, sheet.nrows):
                cells = sheet.row_slice(rowx=r, start_colx=0, end_colx=3)
                record = User(**{'name' : str(cells[0].value),'age' : str(cells[1].value),'address' : str(cells[2].value)})
                db_conn.save_record_db(record)

    def load_data(self, db_conn):
        try:
            with open(self.filename + self.file_extension,'rb') as input_file:
                if self.file_extension == '.csv':
                    self.csv_data_import_db(input_file, db_conn)
                elif self.file_extension == '.xlsx':
                    self.xlsx_data_import_db(input_file, db_conn)
                elif self.file_extension == '.xls':
                    self.xls_data_import_db(input_file, db_conn)
        except:
            db_conn.rollback_db()
        finally:
            db_conn.close_db()
