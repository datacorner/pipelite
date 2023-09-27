__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pandas as pd
import pipelite.utils.constants as C

class etlDataset:
    """ This class encapsulate the data set management (currently Pandas DataFrames) 
        (here it's currently managed by using Pandas DataFrame)
    """
    def __init__(self):
        self.name = C.EMPTY
        self.__content__ = pd.DataFrame() # encapsulate DataFrame

    @property
    def columns(self):
        """ Returns all the dataset columns names
        Returns:
            list: columns names
        """
        return self.__content__.columns

    def initFromList(self, lst, defaultype=None):
        """ initialize the content via a list
        Args:
            lst (list): data set
            defaultype: default type for columns
        """
        if (defaultype == None):
            self.__content__ = pd.DataFrame(lst)
        else:
            self.__content__ = pd.DataFrame(lst, dtype=defaultype)

    def copy(self):
        """Returns a copy of the Dataset (to avoid any reference issues)
        Returns:
            etlDataset: strict copy of the current dataset
        """
        newDS = etlDataset()
        newDS.name = self.name
        newDS.__content__ = self.__content__.copy(deep=True)
        return newDS

    @property
    def count(self):
        """Return the Data rows count
        Returns:
            int: row count
        """
        return self.__content__.shape[0]

    def readSQL(self, odbcConnection, query):
        """ Launch a SQL statement and get the resultset
        Args:
            odbcConnection (ODBC connection): ODBC connection via pyodbc
            query (str): SQL statement
        """
        self.__content__ = pd.read_sql(query, odbcConnection)

    def readCSV(self, filename, separator, encoding):
        """ Read the Data from a CSV file by using Pandas
        Args:
            filename (str): filename and path
            separator (str): CSV delimiter
            encoding (str): encoding
        """
        self.__content__ = pd.read_csv(filepath_or_buffer=filename, 
                                    encoding=encoding, 
                                    delimiter=separator)

    def writeCSV(self, filename, separator, encoding):
        """ Write the Data into a CSV file by using Pandas
        Args:
            filename (str): filename and path
            separator (str): CSV delimiter
            encoding (str): encoding
        """
        self.__content__.to_csv(path_or_buf=filename, 
                            encoding=encoding, 
                            index=False, 
                            sep=separator)

    def read_excel(self, filename, sheet=0):
        # Read the Excel file and provides a DataFrame
        self.__content__ = pd.read_excel(filename, sheet_name=sheet) #, engine='openpyxl')
        
    def concatWith(self, etlDatasetB, keys=None):
        """Concatenate the current dataset with the one in arg
        Args:
            etlDatasetB (etlDataset): other dataset to concat
            keys (str, optional): key to concat. Defaults to None.
        """
        self.__content__ = pd.concat([self.__content__, etlDatasetB], 
                                 keys=keys)
    
    def lookupWith(self, dsLookup, mapColName):
        """ Makes a lookup from the current dataset and the givent one. the join is made on the mapColName which must exists in both sides
        Args:
            dsLookup (etlDataset): lookup dataset
            mapColName (str): column name on both side
        """
        self.__content__ = pd.merge(self.__content__, dsLookup, on=mapColName, how ="inner")

    def dropLineNaN(self, column):
        """ Drops all rows if the column value is NaN
        Args:
            column (str): column name
        """
        self.__content__ = self.__content__.dropna(subset=[column])

    def dropColumn(self, column):
        """Drop a column
        Args:
            column (str): column name
        """
        del self.__content__[column]

    def renameColumn(self, oldName, newName):
        """rename a column inside the dataset
        Args:
            oldName (str): Old column name
            newName (str): New column name
        """
        self.__content__.rename(columns={oldName:newName}, inplace=True)

    def __getitem__(self, item):
        """ Makes the Data column accessible via [] array
            example: df['colName']
        Args:
            item (str): attribute/column name
        Returns:
            object: data
        """
        return self.__content__.__getitem__(item)

    def __getattr__(self, name):
        """ Makes the Data accessible via attribute name
            example: df.colName
        Args:
            item (str): attribute/column name
        Returns:
            object: data
        """
        return self.__content__.__getattr__(name)