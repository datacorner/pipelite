from setuptools import setup, find_packages
# LAUNCH --> python setup.py bdist_wheel
import pathlib
import pkg_resources
import os

VERSION = "0.5.2"
DEPENDENCIES = ["pandas==2.0.3",
                "openpyxl==3.1.2",
                "pyodbc==4.0.39",
                "pyrfc==3.1",
                "requests==2.31.0",
                "xmltodict==0.13.0",
                ]
# LAUNCH --> python setup.py bdist_wheel

setup(
    name = 'dataplumber', 
    version=VERSION, 
    license = 'GPL V3',
    url = 'https://datacorner.fr/',
    download_url = 'https://datacorner.fr/',
    description = 'This solution builds a bridge between two different or same datasources',
    long_description = open(os.path.join(os.path.dirname(__file__), 'README.md')).read(),
    data_files = [('dossier_config',['config-samples/bplogs.sql', 
                                     'config-samples/sqlite/config.ddl', 
                                     'config-samples/ini/config.ini-template'])],
    packages=find_packages(),
    install_requires=DEPENDENCIES,
)