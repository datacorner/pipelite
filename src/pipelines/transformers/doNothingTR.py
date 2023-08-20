__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import pandas as pd
import utils.constants as C
from .Transformer import Transformer

class doNothingTR(Transformer):

    def transform(self, inputDataFrames):
        """ Returns all the data in a DataFrame format
        Args:
            inputDataFrames (pd.DataFrame() []): multiple dataframes
        Returns:
            pd.DataFrame: Output Dataframe [] of the transformer(s)
            int: Number of rows transformed
        """
        return [ inputDataFrames ], 0