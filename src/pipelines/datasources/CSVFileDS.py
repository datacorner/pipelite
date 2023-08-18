__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import pandas as pd
from .DataSource import DataSource 
import utils.constants as C

class CSVFileDS(DataSource):

    def __init__(self, log = None):
        super().__init__(log)
        self.separator = C.DEFCSVSEP

    @property
    def filename(self):
        return self.__filename
    @filename.setter   
    def filename(self, value):
        self.__filename = value

    def extract(self) -> pd.DataFrame():
        """ Returns all the data in a DataFrame format
        Returns:
            pd.DataFrame(): dataset read
        """
        try:
            content = pd.read_csv(self.filename, 
                                    encoding=C.ENCODING, 
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