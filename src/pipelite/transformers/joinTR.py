__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.baseobjs.BOTransformer import BOTransformer
from pipelite.plDatasets import plDatasets

CFGFILES_DSOBJECT = "joinTR.json"
PARAM_LEFT = "left"
PARAM_RIGHT = "right"
PARAM_DS_NAME = "dsid"
PARAM_KEYS = "keys"
PARAM_JOINTYPE = "join"
JOINTYPE_ACCEPTED = ["inner", "left", "right", "outer"]

class joinTR(BOTransformer):
    """ join two datasets.
        The join is made on the keys (main-column-key, lookup-column-key)
        The column wich remains is lookup-column-keep (it replaces the main-column-key column content)
    Args:
        Transformer (class): Transformer template
    """
    def __init__(self, config, log):
        super().__init__(config, log)

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
        try:
            self.left_DS = params.getParameter(PARAM_LEFT)[PARAM_DS_NAME]
            self.left_DS_Keys = params.getParameter(PARAM_LEFT)[PARAM_KEYS]
            self.right_DS = params.getParameter(PARAM_RIGHT)[PARAM_DS_NAME]
            self.right_DS_Keys = params.getParameter(PARAM_RIGHT)[PARAM_KEYS]
            self.joinType  = params.getParameter(PARAM_JOINTYPE)
            if (self.joinType not in JOINTYPE_ACCEPTED):
                raise Exception ("The join type specified is not accepted by pipelite")
            if (len(self.left_DS_Keys) == 0 or len(self.right_DS_Keys) == 0):
                raise Exception ("At least one column join key must be specified in both sides (left and right)")
            if (len(self.left_DS_Keys) != len(self.right_DS_Keys)):
                raise Exception ("The number of column keys must equal in both sides (left and right)")
            return True
        except Exception as e:
            self.log.error("{}".format(str(e)))
            return False
    
    def process(self, dsTransformerInputs) -> plDatasets:
        """ Returns all the data in a etlDataset format
        Args:
            inputDataFrames (etlDatasets): multiple datasets (inputs)
        Returns:
            etlDatasets: Output etlDatasets of the transformer(s)
            int: Number of rows transformed
        """
        try:
            # identify the main & the lookup dataset first
            dsLeft = dsTransformerInputs.getFromId(self.left_DS)
            if (dsLeft == None):
                raise Exception("Dataset Left stream has not been identified in the flow")
            dsRight = dsTransformerInputs.getFromId(self.right_DS)
            if (dsRight == None):
                raise Exception("Dataset Right stream has not been identified in the flow")
            self.log.debug("Perform join between Left stream {} and Right Stream {}".format(self.left_DS, self.right_DS))
            
            # Change the right columns names to the main one for the lookup itself
            dsRightCopy = dsRight.copy()
            for colIdx in range(len(self.right_DS_Keys)):
                dsRightCopy.renameColumn(self.right_DS_Keys[colIdx], self.left_DS_Keys[colIdx])
            dsLeft.joinWith(dsRightCopy, on=self.left_DS_Keys, how=self.joinType)

            # Return the output as a collection with only one item with the excepted id
            dsOutputs = plDatasets()
            # Create from the source another instance of the data
            dsLeft.id = self.dsOutputs[0]
            dsOutputs.add(dsLeft)
            return dsOutputs
        
        except Exception as e:
            self.log.error("{}".format(e))
            return dsTransformerInputs