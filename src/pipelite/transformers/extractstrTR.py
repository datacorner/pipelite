__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.interfaces.ITransformer import ITransformer
from pipelite.etlDatasets import etlDatasets
import re

CFGFILES_DSOBJECT = "extractstrTR.json"
PARAM_COLUMN = "column"
PARAM_START = "start"
PARAM_LENGTH = "length"

class extractstrTR(ITransformer):

    def __init__(self, config, log):
        super().__init__(config, log)
        self.columnName = C.EMPTY
        self.start = 0
        self.length = 0

    def initialize(self, params) -> bool:
        """ Initialize and makes some checks (params) for that transformer
        Args:
            params (json): parameters
        Returns:
            bool: False if error
        """
        try:
            self.columnName = params.getParameter(PARAM_COLUMN)
            self.start = params.getParameter(PARAM_START, 0)
            self.length = params.getParameter(PARAM_LENGTH, 0)
            return True
        except Exception as e:
            self.log.error("{}".format(str(e)))
            return False
        
    @property
    def parametersValidationFile(self):
        return self.getResourceFile(package=C.RESOURCE_PKGFOLDER_TRANSFORMERS, 
                                    file=CFGFILES_DSOBJECT)
    
    def transform(self, dsTransformerInputs) -> etlDatasets:
        """ extract a string from a given column and replace the result
        Args:
            inputDataFrames (etlDatasets): multiple dataset in a collection
        Returns:
            etlDatasets: same as input
        """
        for dsItem in dsTransformerInputs:  # go through each dataset in entry
            dsItem.subString(self.columnName, self.start, self.length)
        return dsTransformerInputs