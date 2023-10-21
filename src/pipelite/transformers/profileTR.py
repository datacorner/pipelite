__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.baseobjs.BOTransformer import BOTransformer
from pipelite.plDatasets import plDatasets
import json

CFGFILES_DSOBJECT = "profileTR.json"
PARAM_PROFILEFILE = "filename"
PARAM_MAXVALUECOUNTS = "maxvaluecounts"
DEFAULT_PROFILEFILE = "pipelite.profile"

class profileTR(BOTransformer):

    @property
    def parametersValidationFile(self):
        return self.getResourceFile(package=C.RESOURCE_PKGFOLDER_TRANSFORMERS, 
                                    file=CFGFILES_DSOBJECT)

    def __init__(self, config, log):
        super().__init__(config, log)
        self.profileFile = C.EMPTY

    def initialize(self, params) -> bool:
        """ Initialize and makes some checks (params) for that transformer
        Args:
            params (json): parameters
        Returns:
            bool: False if error
        """
        try:
            self.profileFile = params.getParameter(PARAM_PROFILEFILE)
            self.maxvaluecounts = params.getParameter(PARAM_MAXVALUECOUNTS)
            if (len(self.profileFile) == 0):
                self.profileFile = DEFAULT_PROFILEFILE
            return True
        except Exception as e:
            self.log.error("{}".format(str(e)))
            return False

    def process(self, dsTransformerInputs) -> plDatasets:
        """ this transformer profiles the dataset in inputs and provides several profile datasets in output
        Args:
            inputDataFrames (etlDatasets): multiple dataset in a collection
            outputNames (array): output names list
        Returns:
            etlDatasets: same as input
        """
        try :
            for index, input in enumerate(dsTransformerInputs):
                profile = input.profile(maxvaluecounts=self.maxvaluecounts)
                filename = self.profileFile
                if (index != 0):
                    filename = self.profileFile.split(".")[0] + str(index) + "." + self.profileFile.split(".")[1]
                with open(filename, "w") as outfile: 
                    json.dump(profile, outfile)
                self.log.info("The data profile has been successfully generated in the file {}".format(self.profileFile))
            return plDatasets()
        except Exception as e:
            self.log.error("{}".format(str(e)))
            return plDatasets()