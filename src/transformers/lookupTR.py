__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.interfaces.ITransformer import ITransformer
from pipelite.etlDatasets import etlDatasets

CFGFILES_DSOBJECT = "lookupTR.json"
PARAM_LOOKUP = "lookup"
PARAM_MAIN = "main"
PARAM_DS_NAME = "ds-name"
PARAM_KEY = "key"
PARAM_LOOKUP_KEEP = "keep"

class lookupTR(ITransformer):
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
            self.lookupDatasetName = params.getParameter(PARAM_LOOKUP)[PARAM_DS_NAME]
            self.lookupDatasetColKey = params.getParameter(PARAM_LOOKUP)[PARAM_KEY]
            self.lookupDatasetColKeep = params.getParameter(PARAM_LOOKUP)[PARAM_LOOKUP_KEEP]
            self.mainDatasetName = params.getParameter(PARAM_MAIN)[PARAM_DS_NAME]
            self.mainColKey = params.getParameter(PARAM_MAIN)[PARAM_KEY]
            return (len(self.lookupDatasetName) != 0 and 
                    len(self.lookupDatasetColKey) != 0 and 
                    len(self.mainDatasetName) != 0 and 
                    len(self.mainColKey) != 0 and 
                    len(self.lookupDatasetColKeep) != 0)
        except Exception as e:
            self.log.error("{}".format(str(e)))
            return False
    
    def transform(self, dsTransformerInputs) -> etlDatasets:
        """ Returns all the data in a etlDataset format
        Args:
            inputDataFrames (etlDatasets): multiple datasets (inputs)
        Returns:
            etlDatasets: Output etlDatasets of the transformer(s)
            int: Number of rows transformed
        """

        try:
            # identify the main & the lookup dataset first
            dsMain = dsTransformerInputs.getFromName(self.mainDatasetName)
            if (dsMain == None):
                raise Exception("Main stream has not been identified in the flow")
            dsLookup = dsTransformerInputs.getFromName(self.lookupDatasetName)
            if (dsLookup == None):
                raise Exception("Lookup stream has not been identified in the flow")
            self.log.debug("Perform Lookup between Main stream {} and Lookup Stream {} on main key [{}]".format(self.mainDatasetName, self.lookupDatasetName, self.mainColKey))
            # Change the lookup column name to the main one for the lookup itself
            dsLookup.renameColumn(self.lookupDatasetColKey, self.mainColKey)
            originalRecCount = dsMain.count
            self.log.debug("There are {} records in the main dataset stream".format(originalRecCount))
            dsMain.lookupWith(dsLookup, self.mainColKey) # Effective lookup
            dsMain.dropLineNaN(self.lookupDatasetColKeep) # drop the NA values (lookup failed / no values as result)
            # Reshape the dataset (columns changes)
            dsMain.dropColumn(self.mainColKey)
            dsMain.renameColumn(self.lookupDatasetColKeep, self.mainColKey)
            # display Nb of rows removed
            iNbRemoved = originalRecCount - dsMain.count
            if (iNbRemoved != 0):
                self.log.warning("{} records have been removed by the transformation (no lookup)".format(iNbRemoved))
            # Return the output as a collection with only one item with the excepted name
            dsOutputs = etlDatasets()
            # Create from the source another instance of the data
            dsMain.name = self.dsOutputs[0]
            dsOutputs.add(dsMain)
            return dsOutputs
        
        except Exception as e:
            self.log.error("{}".format(e))
            return dsTransformerInputs