__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import utils.constants as C
from .Transformer import Transformer
from fmk.etlDatasets import etlDatasets

class lookupTR(Transformer):
    """ Transcode a column from a main dataset with data from the lookup table.
        * If data is found in the lookup table -> then the column is replace in the main dataset
        * If the data is not found -> row is dropped
        The join is made on the keys (main-column-key, lookup-column-key)
        The column wich remains is lookup-column-keep (it replaces the main-column-key column content)
    Args:
        Transformer (class): Transformer template
    """
    def __init__(self, config, log):
        super().__init__(config, log)
        self.lookupDatasetName = C.EMPTY
        self.lookupDatasetColKey = C.EMPTY
        self.lookupDatasetColKeep = C.EMPTY
        self.mainDatasetName = C.EMPTY
        self.mainColKey = C.EMPTY

    def initialize(self, params) -> bool:
        """ Initialize and makes some checks (params) for that transformer
        Args:
            params (json): parameters
        Returns:
            bool: False if error
        """
        try:
            self.lookupDatasetName = params["lookup"]["ds-name"]
            self.lookupDatasetColKey = params["lookup"]["key"]
            self.mainDatasetName = params["main"]["ds-name"]
            self.mainColKey = params["main"]["key"]
            self.lookupDatasetColKeep = params["lookup"]["keep"]
            return (len(self.lookupDatasetName) != 0 and 
                    len(self.lookupDatasetColKey) != 0 and 
                    len(self.mainDatasetName) != 0 and 
                    len(self.mainColKey) != 0 and 
                    len(self.lookupDatasetColKeep) != 0)
        except Exception as e:
            self.log.error("transcoderTR.initialize() Error -> {}".format(str(e)))
            return False
    
    def transform(self, dsStack):
        """ Returns all the data in a etlDataset format
        Args:
            inputDataFrames (etlDatasets): multiple datasets (inputs)
        Returns:
            etlDatasets: Output etlDatasets of the transformer(s)
            int: Number of rows transformed
        """

        try:
            # identify the main & the lookup dataset first
            if (dsStack[0].name == self.mainDatasetName):
                dsMain, dsLookup = dsStack[0], dsStack[1]
            else:
                dsMain, dsLookup = dsStack[1],dsStack[0]
            # Change the lookup column name to the main one for the lookup itself
            dsLookup.renameColumn(self.lookupDatasetColKey, self.mainColKey)
            originalRecCount = dsMain.count
            self.log.debug("There are {} records in the original dataset".format(originalRecCount))
            dsMain.lookupWith(dsLookup, self.mainColKey) # Effective lookup
            dsMain.dropLineNaN(self.lookupDatasetColKeep) # drop the NA values (lookup failed / no values as result)
            # Reshape the dataset (columns changes)
            dsMain.dropColumn(self.mainColKey)
            dsMain.renameColumn(self.lookupDatasetColKeep, self.mainColKey)
            # display Nb of rows removed
            iNbRemoved = originalRecCount - dsMain.count
            if (iNbRemoved != 0):
                self.log.warning("{} records have been removed by the transformation".format(iNbRemoved))
            # Return the output as a collection with only one item with the excepted name
            dsOutputs = etlDatasets()
            # Create from the source another instance of the data
            dsMain.name = self.dsOutputs[0]
            dsOutputs.add(dsMain)
            return dsOutputs, iNbRemoved
        except Exception as e:
            self.log.info("lookupTR.transform() -> ".format(e))
            return dsStack, 0