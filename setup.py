from setuptools import setup

setup(
    name='import_and_save',
    version='0.1',
    py_modules=['import_and_save'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        import_and_save=import_and_save:import_and_save
    ''',
)
