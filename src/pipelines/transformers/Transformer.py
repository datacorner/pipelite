__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import pandas as pd
from utils.log import log
from pipelines.dpInstantiableObj import dpInstantiableObj

class Transformer(dpInstantiableObj):

    def initialize(self) -> bool:
        """Sets all the needed parameters comong from the configuration ()
        Args:
            config (_type_): COnfiguration focused on the parameters needed for this datasource
        Returns:
            bool: False if error
        """
        return True
    
    def transform(self, dfSource) -> pd.DataFrame:
        """ Returns all the data in a DataFrame format
        Returns:
            bool: False is any trouble when reading
        """
        return pd.DataFrame()
