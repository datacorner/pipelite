__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.parents.DataSource import DataSource 
import pipelite.utils.constants as C
import os

class excelFileDS(DataSource):

    def __init__(self, config, log):
        super().__init__(config, log)
        self.sheet = 0
        self.filename = C.EMPTY

    def initialize(self, params) -> bool:
        """ initialize and check all the needed configuration parameters
        Args:
            params (json list) : params for the data source.
                example: {'separator': ',', 'filename': 'test2.csv', 'path': '/tests/data/', 'encoding': 'utf-8'}
        Returns:
            bool: False if error
        """
        try:
            self.sheet = self.getValFromDict(params, 'sheet', 0)
            self.filename = os.path.join(self.getValFromDict(params, 'path', C.EMPTY), 
                                         self.getValFromDict(params, 'filename', C.EMPTY))

            # Checks ...
            if (self.ojbType == C.PLJSONCFG_LOADER):
                if (not os.path.isfile(self.filename)):
                    raise Exception("The file {} does not exist or is not accessible.".format(self.filename))
            
            return True
        except Exception as e:
            self.log.error("excelFileDS.initialize() Error: {}".format(e))
            return False
    
    def extract(self) -> int:
        """ Returns all the data in a DataFrame format
        Returns:
            pd.DataFrame(): dataset read
        """
        try:
            self.log.info("Extract the Dataset from the file: {}".format(self.filename))
            self.content.read_excel(self.filename, 
                         sheet_name=self.sheet)
            return self.content.count
        except Exception as e:
            self.log.error("excelFileDS.extract() Error while reading the file: ".format(e))
            return False