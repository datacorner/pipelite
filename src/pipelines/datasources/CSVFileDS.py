__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import pandas as pd
from .DataSource import DataSource 
import utils.constants as C
import os

class CSVFileDS(DataSource):

    def __init__(self, config, log):
        super().__init__(config, log)
        self.separator = C.DEFCSVSEP
        self.filename = C.EMPTY
        self.encoding = C.ENCODING

    def initialize(self, params) -> bool:
        """ initialize and check all the needed configuration parameters
            A CSV Extractor/Loader must have:
                * A filename
                * A separator
                * A path name
                * An Encoding type
                params['separator']
        Args:
            params (json list) : params for the data source.
                example: {'separator': ',', 'filename': 'test2.csv', 'path': '/tests/data/', 'encoding': 'utf-8'}
        Returns:
            bool: False if error
        """
        try:
            self.separator = str(params['separator'])
            self.filename = os.path.join(params['path'], params['filename'])
            self.encoding = str(params['encoding'])

            # Checks ...
            if (self.ojbType == C.PLJSONCFG_LOADER):
                if (not os.path.isfile(self.filename)):
                    raise Exception("The file {} does not exist or is not accessible.".format(self.filename))
            
            return (self.filename)
        except Exception as e:
            self.log.error("CSVFileDS.initialize() Error: {}".format(e))
            return False
    
    def extract(self) -> bool:
        """ Returns all the data in a DataFrame format
        Returns:
            pd.DataFrame(): dataset read
        """
        try:
            self.content = pd.read_csv(self.filename, 
                                       encoding=self.encoding, 
                                       delimiter=self.separator)
            return True
        except Exception as e:
            self.log.error("CSVFileDS.extract() Error while reading the file: ".format(e))
            return False

    def load(self) -> int:
        """ write the dataset in the datasource
        Returns:
            int: Number of data rows loaded
        """
        try:
            self.content.to_csv(self.filename, encoding=C.ENCODING, index=False)
            return self.count
        except Exception as e:
            self.log.error("CSVFileDS.extract() Error while writing the file: ".format(e))
            return 0