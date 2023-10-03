__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.interfaces.ITransformer import ITransformer
from pipelite.etlDatasets import etlDatasets

class donothingTR(ITransformer):

    def transform(self, dsTransformerInputs) -> etlDatasets:
        """ Just do nothing !
        Args:
            inputDataFrames (etlDatasets): multiple dataset in a collection
        Returns:
            etlDatasets: same as input
        """
        return dsTransformerInputs