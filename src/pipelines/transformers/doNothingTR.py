__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import utils.constants as C
from .Transformer import Transformer

class doNothingTR(Transformer):

    def transform(self, dsStack):
        """ Returns all the data in a etlDataset format
        Args:
            inputDataFrames (etlDataset() []): multiple dataframes
        Returns:
            etlDataset: Output etlDataset [] of the transformer(s)
            int: Number of rows transformed
        """
        return dsStack, 0