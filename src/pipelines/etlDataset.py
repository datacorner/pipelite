__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pandas as pd

class etlDataset:
    """ This class encapsulate the data set management 
        (here it's currently managed by using Pandas DataFrame)
    """
    def __init__(self):
        self.content = pd.DataFrame()

    @property
    def count(self):
        """Return the Data rows count
        Returns:
            int: row count
        """
        return self.content.shape[0]

    def readSQL(self, odbcConnection, query):
        """ Launch a SQL statement and get the resultset

        Args:
            odbcConnection (ODBC connection): ODBC connection via pyodbc
            query (str): SQL statement
        """
        self.content = pd.read_sql(query, odbcConnection)

    def readCSV(self, filename, separator, encoding):
        """ Read the Data from a CSV file by using Pandas
        Args:
            filename (str): filename and path
            separator (str): CSV delimiter
            encoding (str): encoding
        """
        self.content = pd.read_csv(filepath_or_buffer=filename, 
                                    encoding=encoding, 
                                    delimiter=separator)

    def writeCSV(self, filename, separator, encoding):
        """ Write the Data into a CSV file by using Pandas
        Args:
            filename (str): filename and path
            separator (str): CSV delimiter
            encoding (str): encoding
        """
        self.content.to_csv(path_or_buf=filename, 
                            encoding=encoding, 
                            index=False, 
                            sep=separator)
    
    def concatWith(self, etlDatasetB, keys=None):
        self.content = pd.concat([self.content, etlDatasetB], 
                                 keys=keys)
    
    def __getitem__(self, item):
        """ Makes the Data column accessible via [] array
            example: df['colName']
        Args:
            item (str): attribute/column name
        Returns:
            object: data
        """
        return self.content.__getitem__(item)

    def __getattr__(self, name):
        """ Makes the Data accessible via attribute name
            example: df.colName
        Args:
            item (str): attribute/column name
        Returns:
            object: data
        """
        return self.content.__getattr__(name)