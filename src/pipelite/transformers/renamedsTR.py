__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.baseobjs.BOTransformer import BOTransformer
from pipelite.etlDatasets import etlDatasets

class renamedsTR(BOTransformer):

    def process(self, dsTransformerInputs) -> etlDatasets:
        """ Just rename the datasource (pass through)
        Args:
            inputDataFrames (etlDatasets): multiple dataset in a collection
            outputNames (array): output names list
        Returns:
            etlDatasets: same as input
        """
        i=0
        if (len(self.dsOutputs) == dsTransformerInputs.count):
            for item in dsTransformerInputs:
                item.name = self.dsOutputs[i]
                i=+1
        return dsTransformerInputs