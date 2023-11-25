__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.baseobjs.BOTransformer import BOTransformer
from pipelite.plDataset import plDataset
from pipelite.plDatasets import plDatasets

CFGFILES_DSOBJECT = "concatTR.json"

class concatTR(BOTransformer):

    @property
    def parametersValidationFile(self):
        return self.getResourceFile(package=C.RESOURCE_PKGFOLDER_TRANSFORMERS, 
                                    file=CFGFILES_DSOBJECT)
    
    def process(self, dsTransformerInputs) -> plDatasets:
        """ Concatenate 2 or more datasets together
        Args:
            dsStack (etlDatasets): multiple datasets to concat in a collection
        Returns:
            etlDatasets: Output etlDataset collection of the transformer(s).
            int: Number of rows transformed
        """
        try:
            output = plDataset(self.config, self.log)
            self.log.info("There are {} datasets to concatenate".format(dsTransformerInputs.count))
            for obj in dsTransformerInputs:
                self.log.debug("Adding {} rows from the dataset {}".format(obj.count, obj.id))
                output.concatWith(obj)
            # Return the output as a collection with only one item with the excepted id
            dsOutputs = plDatasets()
            # Create from the source another instance of the data
            output.id = self.dsOutputs[0]
            dsOutputs.add(output)
            return dsOutputs
        
        except Exception as e:
            self.log.info("concatTR.transform() -> ".format(e))
            return dsTransformerInputs