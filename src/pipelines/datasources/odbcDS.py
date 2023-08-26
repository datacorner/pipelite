__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

from .DataSource import DataSource 
import utils.constants as C
import pyodbc

class odbcDS(DataSource):

    def __init__(self, config, log):
        super().__init__(config, log)
        self.separator = C.DEFCSVSEP
        self.filename = C.EMPTY
        self.encoding = C.ENCODING

    def initialize(self, params) -> bool:
        """ initialize and check all the needed configuration parameters
            A ODBC Extractor/Loader must have:
                * A connection string
                * A query (SQL)
                ex. params['query']
        Args:
            params (json list) : params for the data source.
                example: {"connectionstring": "DRIVER={SQLite3};Database=tests/data/dbsample.sqlite",
                          "query": "SELECT * FROM Customers"}
        Returns:
            bool: False if error
        """
        try:
            self.connectionString = str(params['connectionstring'])
            self.query = str(params['query'])

            # checks
            if (len(self.connectionString) == 0):
                raise Exception("The ODBC Connection String cannot be null")
            if (len(self.query) == 0):
                raise Exception("The SQL Query cannot be null")
            
            return True
        except Exception as e:
            self.log.error("CSVFileDS.initialize() Error: {}".format(e))
            return False
    
    def extract(self) -> int:
        """ Returns all the data in a DataFrame format
        Returns:
            pd.DataFrame(): dataset read
        """
        try:
            self.log.info("Connect to the ODBC Datasource ...")
            odbcConnection = pyodbc.connect(self.connectionString)
            self.log.info("Connected to ODBC Data source")
            if (not odbcConnection.closed):
                self.log.debug("Execute the query: {}".format(self.query))
                self.content.readSQL(odbcConnection=odbcConnection, 
                                     query=self.query)
                odbcConnection.close()
                self.log.debug("Number of <{}> rows read in the ODBC Data Source".format(self.content.shape[0]))

            return self.content.count
        except pyodbc.Error as e:
            self.log.error("odbcDS.extract() -> pyodbc.Error while reading ODBC Data Source: Code: {} - Message: {}".format(e.args[0], e.args[1]))
            return False
        
        except Exception as e:
            self.log.error("odbcDS.extract(Exception) Exception while reading ODBC Data Source: ".format(e))
            try:
                odbcConnection.close()
            except:
                pass
            return False