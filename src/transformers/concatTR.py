__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from .Transformer import Transformer
from fmk.etlDataset import etlDataset
from fmk.etlDatasets import etlDatasets

class concatTR(Transformer):
    
    def initialize(self, params) -> bool:
        """ Initialize and makes some checks (params) for that transformer
        Args:
            params (json): parameters
        Returns:
            bool: False if error
        """
        return True
    
    def transform(self, dsStack):
        """ Do absolutemy nothing !
        Args:
            inputDataFrames (etlDataset []): multiple datasets in a list
        Returns:
            etlDataset []: Output etlDataset[] of the transformer(s). Only One item in the list
            int: Number of rows transformed
        """
        try:
            output = etlDataset()
            self.log.info("There are {} datasets to concatenate".format(dsStack.count))
            for obj in dsStack:
                self.log.debug("Adding {} rows from the dataset {}".format(obj.count, obj.name))
                output.concatWith(obj)

            # Return the output as a collection with only one item with the excepted name
            dsOutputs = etlDatasets()
            # Create from the source another instance of the data
            output.name = self.dsOutputs[0]
            dsOutputs.add(output)

            return dsOutputs, output.count
        
        except Exception as e:
            self.log.info("concatTR.transform() -> ".format(e))
            return dsStack, 0