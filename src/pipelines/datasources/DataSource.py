__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import pandas as pd
from utils.log import log

class DataSource:
    def __init__(self, log = None):
        self.__log = log

    @property
    def log(self) -> log:
        return self.__log

    def checkIntegrity(self) -> bool:
        """ check if the Data source is readable and is correct
        Returns:
            bool: True if readable
        """
        return True

    def extract(self) -> pd.DataFrame():
        """ Returns all the data in a DataFrame format
        Returns:
            pd.DataFrame(): dataset read
        """
        return True

    def load(self, dfDataSet) -> bool:
        """ write the dataset in the datasource
        Returns:
            bool: False is any trouble when loading
        """
        return True
