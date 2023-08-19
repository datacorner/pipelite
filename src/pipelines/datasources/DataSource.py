__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import pandas as pd
from utils.log import log
from pipelines.etlObject import etlObject

class DataSource(etlObject):
    def __init__(self, config, log):
        super().__init__(config, log)

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
