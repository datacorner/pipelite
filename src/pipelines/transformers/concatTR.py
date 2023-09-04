__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from .Transformer import Transformer
from pipelines.etlDataset import etlDataset

class concatTR(Transformer):
    @property
    def dsInputNbSupported(self):
        return 2
    
    @property
    def dsOutputNbSupported(self):
        return 1
    
    def initialize(self, params) -> bool:
        """ Initialize and makes some checks (params) for that transformer
        Args:
            params (json): parameters
        Returns:
            bool: False if error
        """
        return True
    
    def transform(self, inputDataFrames):
        """ Do absolutemy nothing !
        Args:
            inputDataFrames (etlDataset []): multiple datasets in a list
        Returns:
            etlDataset []: Output etlDataset[] of the transformer(s). Only One item in the list
            int: Number of rows transformed
        """
        try:
            output = etlDataset()
            nbDataSetsInInput = len(inputDataFrames)
            if (nbDataSetsInInput <= 1):
                raise Exception("At least 2 datasets are needed for a concatenation transformation.")
            self.log.info("There are {} datasets to concatenate".format(nbDataSetsInInput))
            for obj in inputDataFrames:
                self.log.debug("Adding {} rows from the dataset {}".format(obj.count, obj.name))
                output.concatWith(obj)
            return [ output ], output.count
        
        except Exception as e:
            self.log.info("concatTR.transform() -> ".format(e))
            return [ inputDataFrames ], 0