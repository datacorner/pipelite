__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import pandas as pd
from utils.log import log

class Transformer:
    def __init__(self, log = None):
        self.__log = log

    @property
    def log(self) -> log:
        return self.__log

    def transform(self, dfSource) -> pd.DataFrame:
        """ Returns all the data in a DataFrame format
        Returns:
            bool: False is any trouble when reading
        """
        return pd.DataFrame()
