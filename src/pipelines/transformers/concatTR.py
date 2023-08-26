__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

from .Transformer import Transformer
from pipelines.etlDataset import etlDataset

NB_OF_DATASOURCES_MAX = 10

class concatTR(Transformer):
    @property
    def dsMaxEntryCount(self):
        return NB_OF_DATASOURCES_MAX
    
    def transform(self, inputDataFrames):
        """ Do absolutemy nothing !
        Args:
            inputDataFrames (etlDataset() []): multiple dataframes
        Returns:
            etlDataset: Output etlDataset [] of the transformer(s)
            int: Number of rows transformed
        """
        try:
            output = etlDataset()
            nbDataSetsInInput = len(inputDataFrames)
            if (nbDataSetsInInput <= 1):
                raise Exception("At least 2 datasets are needed for a concatenation transformation.")
            self.log.info("There are {} datasets to concatenate".format(nbDataSetsInInput))
            for obj in inputDataFrames:
                output.concatWith(obj)
            return [ output ], output.count
        
        except Exception as e:
            self.log.info("concatTR.transform() -> ".format(e))
            return [ inputDataFrames ], 0