import file_loader
from file_loader import FileLoader

import pytest


def test_factory_check_returns_csv_file_loader():
    file_loader_obj = FileLoader.factory('test_csv_file.csv')
    assert str(isinstance(
        file_loader_obj, file_loader.CsvFileLoader)) == 'True'
    assert file_loader_obj.filename == 'test_csv_file'
    assert file_loader_obj.file_extension == '.csv'


def test_factory_check_returns_xls_file_loader():
    file_loader_obj = FileLoader.factory('test_xls_file.xls')
    assert str(isinstance(
        file_loader_obj, file_loader.XlsFileLoader)) == 'True'
    assert file_loader_obj.filename == 'test_xls_file'
    assert file_loader_obj.file_extension == '.xls'


def test_factory_check_returns_xlsx_file_loader():
    file_loader_obj = FileLoader.factory('test_xlsx_file.xlsx')
    assert str(isinstance(
        file_loader_obj, file_loader.XlsxFileLoader)) == 'True'
    assert file_loader_obj.filename == 'test_xlsx_file'
    assert file_loader_obj.file_extension == '.xlsx'


def test_factory_check_returns_invalid_extension():
    with pytest.raises(SystemExit) as e:
        FileLoader.factory('test_png_file.png')
        assert e.type == SystemExit
        assert e.value.code == 42
        assert e.value.message == 'File extension not supported'
