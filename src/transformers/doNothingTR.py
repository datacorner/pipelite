__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.parents.Transformer import Transformer

class doNothingTR(Transformer):

    def transform(self, dsStack):
        """ Just do nothing !
        Args:
            inputDataFrames (etlDatasets): multiple dataset in a collection
        Returns:
            etlDatasets: same as input
            int: 0 transformed rows
        """
        return dsStack, 0