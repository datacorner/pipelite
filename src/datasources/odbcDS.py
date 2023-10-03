__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.interfaces.IDataSource import IDataSource 
import pipelite.constants as C
import pyodbc
from pipelite.utils.SqlTemplate import SqlTemplate
from pipelite.etlDataset import etlDataset

# json validation Configuration 
CFGFILES_DSOBJECT = "odbcDS.json"
CFGPARAMS_QUERY_PARAMETERS = "query-parameters"
CFGPARAMS_ODBC_CN = "connectionstring"
CFGPARAMS_ODBC_QUERY = "query"

class odbcDS(IDataSource):

    def __init__(self, config, log):
        super().__init__(config, log)
        self.query = C.EMPTY
        self.connectionString = C.EMPTY

    @property
    def parametersValidationFile(self):
        return self.getResourceFile(package=C.RESOURCE_PKGFOLDER_DATASOURCES, 
                                    file=CFGFILES_DSOBJECT)
    
    def initialize(self, cfg) -> bool:
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
            
            self.connectionString = str(cfg.getParameter(CFGPARAMS_ODBC_CN, C.EMPTY))
            template = SqlTemplate(self.log)
            self.query = template.getQuery(cfg.getParameter(CFGPARAMS_ODBC_QUERY, C.EMPTY), 
                                           cfg.getParameter(CFGPARAMS_QUERY_PARAMETERS, {}))
            
            # checks
            if (len(self.connectionString) == 0):
                raise Exception("The ODBC Connection String cannot be null")
            if (len(self.query) == 0):
                raise Exception("The SQL Query cannot be null")
            
            return True
        except Exception as e:
            self.log.error("{}".format(e))
            return False
    
    def extract(self) -> etlDataset:
        """ Returns all the data in a DataFrame format
        Returns:
            pd.DataFrame(): dataset read
        """
        try:
            dsExtract = etlDataset()
            self.log.info("Connect to the ODBC Datasource ...")
            odbcConnection = pyodbc.connect(self.connectionString)
            self.log.info("Connected to ODBC Data source")
            if (not odbcConnection.closed):
                self.log.debug("Execute the query: {}".format(self.query))
                dsExtract.readSQL(odbcConnection=odbcConnection, 
                                     query=self.query)
                odbcConnection.close()
                self.log.debug("Number of <{}> rows read in the ODBC Data Source".format(dsExtract.shape[0]))
            return dsExtract
        
        except pyodbc.Error as e:
            self.log.error("odbcDS.extract() -> pyodbc.Error while reading ODBC Data Source: Code: {} - Message: {}".format(e.args[0], e.args[1]))
            return etlDataset()
        except Exception as e:
            self.log.error("Exception while reading ODBC Data Source: ".format(e))
            try:
                odbcConnection.close()
            except:
                pass
            return etlDataset()