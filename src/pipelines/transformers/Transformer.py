__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import pandas as pd
from utils.log import log
from pipelines.etlObject import etlObject

class Transformer(etlObject):

    def transform(self, dfSource) -> pd.DataFrame:
        """ Returns all the data in a DataFrame format
        Returns:
            bool: False is any trouble when reading
        """
        return pd.DataFrame()
