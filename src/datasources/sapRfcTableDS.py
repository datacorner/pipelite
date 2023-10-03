__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.interfaces.IDataSource import IDataSource 
import pipelite.constants as C
from pipelite.etlDataset import etlDataset
from pyrfc import Connection, ABAPApplicationError, ABAPRuntimeError, LogonError, CommunicationError
from pipelite.etlDataset import etlDataset

# json validation Configuration 
CFGFILES_DSOBJECT = "sapRfcTableDS.json"
# Parameters to have in the config file under /parameters
SAPPARAM_AHOST = "ahost"
SAPPARAM_CLIENT = "client"
SAPPARAM_SYSNR = "sysnr"
SAPPARAM_USER = "user"
SAPPARAM_PWD = "pwd"
SAPPARAM_ROUTER = "router"
SAPPARAM_TABLE = "table"
SAPPARAM_FIELDS = "fields"  # list of fields / column names
SAPPARAM_ROWCOUNT = "rowcount"

CFGFILES_DSOBJECT = C.CFG_PARAMETER_DEF_FOLDER + "/datasources/sapRfcTableDS.json"

class sapRfcTableDS(IDataSource):
    def __init__(self, config, log):
        super().__init__(config, log)
        self.ahost = C.EMPTY
        self.client = C.EMPTY
        self.sysnr = C.EMPTY
        self.user = C.EMPTY
        self.pwd = C.EMPTY
        self.router = C.EMPTY
        self.table = C.EMPTY
        self.fields = []
        self.rowcount = 0

    @property
    def parametersValidationFile(self):
        return self.getResourceFile(package=C.RESOURCE_PKGFOLDER_DATASOURCES, 
                                    file=CFGFILES_DSOBJECT)
    
    def initialize(self, cfg) -> bool:
        """ initialize and check all the needed configuration parameters
        Args:
            params (json list) : params for the data source.
        Returns:
            bool: False if error
        """
        try:
            # Get params
            self.ahost = cfg.getParameter(SAPPARAM_AHOST, C.EMPTY)
            self.client = cfg.getParameter(SAPPARAM_CLIENT, C.EMPTY)
            self.sysnr = cfg.getParameter(SAPPARAM_SYSNR, C.EMPTY)
            self.user = cfg.getParameter(SAPPARAM_USER, C.EMPTY)
            self.pwd = cfg.getParameter(SAPPARAM_PWD, C.EMPTY)
            self.router = cfg.getParameter(SAPPARAM_ROUTER, C.EMPTY)
            self.table = cfg.getParameter(SAPPARAM_TABLE, C.EMPTY)
            self.fields = cfg.getParameter(SAPPARAM_FIELDS, [])
            self.rowcount = cfg.getParameter(SAPPARAM_ROWCOUNT, 0)
            return True
        except Exception as e:
            self.log.error("{}".format(e))
            return False

    def __connectToSAP__(self) -> Connection:
        """ Connect to the SAP instance via RFC
        Returns:
            connection: SAP Connection
        """
        try:
            # Get the SAP parmaters first
            self.log.info("Connect to SAP via RFC")
            conn = Connection(ashost=self.ahost, 
                              sysnr=self.sysnr, 
                              client=self.client, 
                              user=self.user, 
                              passwd=self.pwd, 
                              saprouter=self.router)
            return conn
        except CommunicationError:
            self.log.error("CommunicationError: Could not connect to server.")
        except LogonError:
            self.log.error("LogonError: Could not log in. Wrong credentials?")
            print("Could not log in. Wrong credentials?")
        except (ABAPApplicationError, ABAPRuntimeError):
            self.log.error("ABAPApplicationError / ABAPRuntimeError")
        return None

    def __callRFCReadTable__(self, conn) -> etlDataset:
        """ Call the RFC_READ_TABLE BAPI and get the dataset as result
        Args:
            conn (_type_): SAP Connection via pyrfc
        Returns:
            etlDataset: dataset 
        """
        try:
            # Get the list of fields to gather
            # Call RFC_READ_TABLE
            self.log.info("Gather data from the SAP Table")
            result = conn.call("RFC_READ_TABLE",
                                ROWCOUNT=self.rowcount,
                                QUERY_TABLE=self.table,
                                FIELDS=self.fields)

            # Get the data & create the dataFrame
            data = result["DATA"]
            self.log.info("<{}> rows has been read from SAP".format(len(data)))
            fields = result["FIELDS"]

            records = []
            for entry in data:
                record = {}
                for i, field in enumerate(fields):
                    field_name = field["FIELDNAME"]
                    idx = int(field["OFFSET"])
                    length = int(field["LENGTH"])
                    field_value = str(entry["WA"][idx:idx+length])
                    record[field_name] = field_value
                records.append(record)
            res = etlDataset()
            res.initFromList(records, defaultype=str)
            return res

        except Exception as e:
            self.log.error("{}".format(e))
            return etlDataset()

    def extract(self) -> etlDataset:
        """ flaten the XES (XML format) and returns all the data in a etlDataset format
        Returns:
            etlDataset: data set
        """
        try:
            dsExtract = etlDataset()
            sapConn = self.__connectToSAP__()
            if (sapConn != None):
                dsExtract = self.__callRFCReadTable(sapConn)
            return dsExtract
        except Exception as e:
            self.log.error("sapRfcDS.extract() Error while accessing the SAP RFC Table: ".format(e))
            return etlDataset()