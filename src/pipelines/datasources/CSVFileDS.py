__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import pandas as pd
from .DataSource import DataSource 
import utils.constants as C

class CSVFileDS(DataSource):

    def __init__(self, config, log):
        super().__init__(config, log)
        self.separator = C.DEFCSVSEP
        self.filename = C.EMPTY
        self.path = C.EMPTY
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
        self.separator = params['separator']
        self.filename = params['filename']
        self.path  = params['path'] 
        self.encoding = params['encoding']
        return True

    def extract(self) -> pd.DataFrame():
        """ Returns all the data in a DataFrame format
        Returns:
            pd.DataFrame(): dataset read
        """
        try:
            content = pd.read_csv(self.filename, 
                                  encoding=self.encoding, 
                                  delimiter=self.separator)
            return content
        except Exception as e:
            self.log.error("CSVFileDS.extract() Error: " + str(e))
            return super().extract()

    def load(self, dfDataSet) -> bool:
        """ write the dataset in the datasource
        Returns:
            bool: False is any trouble when loading
        """
        return True