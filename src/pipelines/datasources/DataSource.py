__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import pandas as pd
from utils.log import log
from pipelines.etlObject import etlObject

class DataSource(etlObject):
    def __init__(self, config, log):
        self.content = pd.DataFrame()
        super().__init__(config, log)

    @property
    def count(self):
        try:
            return self.content.shape[0]
        except:
            return 0

    def extract(self) -> bool:
        """ Returns all the data in a DataFrame format
        Returns:
            pd.DataFrame(): dataset read
        """
        return True

    def load(self) -> int:
        """ write the dataset in the datasource
        Returns:
            int: Number of data rows loaded
        """
        return 0
