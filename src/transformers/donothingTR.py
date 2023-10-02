__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.parents.Transformer import Transformer
from pipelite.etlDatasets import etlDatasets

class donothingTR(Transformer):

    def transform(self, dsTransformerInputs) -> etlDatasets:
        """ Just do nothing !
        Args:
            inputDataFrames (etlDatasets): multiple dataset in a collection
        Returns:
            etlDatasets: same as input
        """
        return dsTransformerInputs