__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import pandas as pd
from utils.log import log
from pipelines.dpInstantiableObj import dpInstantiableObj

class DataSource(dpInstantiableObj):
    def __init__(self, config, log):
        super().__init__(config, log)

    def initialize(self) -> bool:
        """Sets all the needed parameters comong from the configuration ()
        Args:
            config (_type_): COnfiguration focused on the parameters needed for this datasource
        Returns:
            bool: False if error
        """
        return True

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
