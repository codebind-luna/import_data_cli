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
            self.import_each_record([df['Name'][i],
                                     df['Age'][i], df['Address'][i]], db_conn)

    def csv_data_import_db(self, file_buffer, db_conn):
        csv_data = csv.reader(file_buffer)
        next(csv_data, None)

        for i in csv_data:
            self.import_each_record([i[j] for j in range(len(i))], db_conn)

    def xls_data_import_db(self, file_buffer, db_conn):
        book = xlrd.open_workbook(file_contents=file_buffer.read())

        for sheet_i in range(book.nsheets):
            sheet = book.sheet_by_index(sheet_i)
            for r in range(1, sheet.nrows):
                cells = sheet.row_slice(rowx=r, start_colx=0, end_colx=3)
                self.import_each_record([str(cells[j].value)
                                         for j in range(sheet.ncols)], db_conn)

    def import_each_record(self, data, db_conn):
        db_conn.save_record_db(User(**{'name': data[0],
                                       'age': data[1], 'address': data[2]}))

    def load_data(self, db_conn):
        with open(self.filename + self.file_extension, 'rb') as input_file:
            if self.file_extension == '.csv':
                self.csv_data_import_db(input_file, db_conn)
            elif self.file_extension == '.xlsx':
                self.xlsx_data_import_db(input_file, db_conn)
            elif self.file_extension == '.xls':
                self.xls_data_import_db(input_file, db_conn)
            db_conn.close_db()
