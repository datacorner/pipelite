__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from fmk.roots.DataSource import DataSource 
import utils.constants as C
import pyodbc
from config.dpConfig import dpConfig as pc
from fmk.SqlTemplate import SqlTemplate

class odbcDS(DataSource):

    def __init__(self, config, log):
        super().__init__(config, log)
        self.query = C.EMPTY
        self.connectionString = C.EMPTY

    def initialize(self, params) -> bool:
        """ initialize and check all the needed configuration parameters
            A ODBC Extractor/Loader must have:
                * A connection string
                * A query: this field can be the SQL QUery itself or a reference to a separate file
            The QUery can use a template to manage replacement of some values base on the configuration file entries.
            For that a new entry "query-parameters" is needed:
                        "query-parameters": {
                            "tablename" : "Customers",
                            "fields" : "customer_id, first_name, birthdate"
                        }
                it contains all the values that will be replaced into the query: 
                    ex: SELECT $fields FROM $tablename;
                will be replaced by
                    SELECT customer_id, first_name, birthdate FROM Customers
        Args:
            params (json list) : params for the data source.
                example: {"connectionstring": "DRIVER={SQLite3};Database=tests/data/dbsample.sqlite",
                          "query": "SELECT * FROM Customers"}
        Returns:
            bool: False if error
        """
        try:
            self.connectionString = str(self.getValFromDict(params, C.PLJSONCFG_DS_ODBC_CN, C.EMPTY))
            template = SqlTemplate(self.log)
            self.query = template.getQuery(self.getValFromDict(params, C.PLJSONCFG_DS_ODBC_QUERY, C.EMPTY), 
                                           self.getValFromDict(params, "query-parameters", {}))
            
            # checks
            if (len(self.connectionString) == 0):
                raise Exception("The ODBC Connection String cannot be null")
            if (len(self.query) == 0):
                raise Exception("The SQL Query cannot be null")
            
            return True
        except Exception as e:
            self.log.error("odbcDS.initialize() Error: {}".format(e))
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