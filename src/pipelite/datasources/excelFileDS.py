__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.baseobjs.BODataSource import BODataSource 
import pipelite.constants as C
import os
from pipelite.plDataset import plDataset

CFGPARAMS_PATH = "path"
CFGPARAMS_FILENAME = "filename"
CFGPARAMS_SHEET = "sheet"
CFGFILES_DSOBJECT = "excelFileDS.json"

class excelFileDS(BODataSource):

    def __init__(self, config, log):
        super().__init__(config, log)
        self.sheet = 0
        self.filename = C.EMPTY

    @property
    def parametersValidationFile(self):
        return self.getResourceFile(package=C.RESOURCE_PKGFOLDER_DATASOURCES, 
                                    file=CFGFILES_DSOBJECT)
    
    def initialize(self, cfg) -> bool:
        """ initialize and check all the needed configuration parameters
        Args:
            params (json list) : params for the data source.
                example: {'separator': ',', 'filename': 'test2.csv', 'path': '/tests/data/', 'encoding': 'utf-8'}
        Returns:
            bool: False if error
        """
        try:
            self.sheet = cfg.getParameter(CFGPARAMS_SHEET, C.EMPTY)
            self.filename = os.path.join(cfg.getParameter(CFGPARAMS_PATH, C.EMPTY), 
                                         cfg.getParameter(CFGPARAMS_FILENAME, C.EMPTY))

            # Checks ...
            if (self.objtype == C.PLJSONCFG_EXTRACTOR):
                if (not os.path.isfile(self.filename)):
                    raise Exception("The file {} does not exist or is not accessible.".format(self.filename))
            
            return True
        except Exception as e:
            self.log.error("{}".format(e))
            return False
    
    def read(self) -> plDataset:
        """ Returns all the data in a DataFrame format
        Returns:
            pd.DataFrame(): dataset read
        """
        try:
            self.log.info("Extract the Dataset from the file: {}".format(self.filename))
            dsExtract = plDataset(self.config, self.log)
            dsExtract.read_excel(self.filename, sheet=self.sheet)
            return dsExtract
        except Exception as e:
            self.log.error("{}".format(e))
            return False