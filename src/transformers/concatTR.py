__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.interfaces.ITransformer import ITransformer
from pipelite.etlDataset import etlDataset
from pipelite.etlDatasets import etlDatasets

CFGFILES_DSOBJECT = "concatTR.json"

class concatTR(ITransformer):

    @property
    def parametersValidationFile(self):
        return self.getResourceFile(package=C.RESOURCE_PKGFOLDER_TRANSFORMERS, 
                                    file=CFGFILES_DSOBJECT)
    
    def initialize(self, params) -> bool:
        """ Initialize and makes some checks (params) for that transformer
        Args:
            params (json): parameters
        Returns:
            bool: False if error
        """
        return True
    
    def transform(self, dsTransformerInputs) -> etlDatasets:
        """ Concatenate 2 or more datasets together
        Args:
            dsStack (etlDatasets): multiple datasets to concat in a collection
        Returns:
            etlDatasets: Output etlDataset collection of the transformer(s).
            int: Number of rows transformed
        """
        try:
            output = etlDataset()
            self.log.info("There are {} datasets to concatenate".format(dsTransformerInputs.count))
            for obj in dsTransformerInputs:
                self.log.debug("Adding {} rows from the dataset {}".format(obj.count, obj.name))
                output.concatWith(obj)
            # Return the output as a collection with only one item with the excepted name
            dsOutputs = etlDatasets()
            # Create from the source another instance of the data
            output.name = self.dsOutputs[0]
            dsOutputs.add(output)
            return dsOutputs
        
        except Exception as e:
            self.log.info("concatTR.transform() -> ".format(e))
            return dsTransformerInputs