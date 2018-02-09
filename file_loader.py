import os
import csv
from db_connector import User
import xlrd
import pandas as pd


class FileLoader(object):
    def __init__(self, filename, file_extension):
        self.filename = filename
        self.file_extension = file_extension

    # Create based on file extension :
    @staticmethod
    def factory(path_to_file):
        filename, file_extension = os.path.splitext(path_to_file)
        try:
            return eval(file_extension[1:].title() + "FileLoader")(
             filename, file_extension
            )
        except NameError:
            print "File extension not supported"
            exit()

    def create_user_instance(self, data):
        return User(**{'name': data[0], 'age': data[1], 'address': data[2]})


class CsvFileLoader(FileLoader):
    def load_data(self, db_conn):
        with open(self.filename + self.file_extension, 'rb') as input_file:
            csv_data = csv.reader(input_file)
            next(csv_data, None)

            record_set = [self.create_user_instance([i[j]
                          for j in range(len(i))])
                          for i in csv_data]

            db_conn.save_bulk_record_db(record_set)


class XlsFileLoader(FileLoader):
    def load_data(self, db_conn):
        with open(self.filename + self.file_extension, 'rb') as input_file:
            book = xlrd.open_workbook(file_contents=input_file.read())
            record_set = []

            for sheet_i in range(book.nsheets):
                sheet = book.sheet_by_index(sheet_i)
                for r in range(1, sheet.nrows):
                    cells = sheet.row_slice(rowx=r, start_colx=0, end_colx=3)
                    record_set.append(self.create_user_instance(
                                      [str(cells[j].value)
                                       for j in range(sheet.ncols)])
                                      )

        db_conn.save_bulk_record_db(record_set)


class XlsxFileLoader(FileLoader):
    def load_data(self, db_conn):
        with open(self.filename + self.file_extension, 'rb') as input_file:
            rows = pd.read_excel(input_file,
                                 sheet_name='Sample-spreadsheet-file')
            record_set = [self.create_user_instance(
                          [rows['Name'][i],
                           rows['Age'][i],
                           rows['Address'][i]]) for i in rows.index]

        db_conn.save_bulk_record_db(record_set)
