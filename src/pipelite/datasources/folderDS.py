__author__ = "Benoit CAYLA"
__email__ = "benoit@datacorner.fr"
__license__ = "MIT"

from pipelite.interfaces.IDataSource import IDataSource 
import pipelite.constants as C
from pathlib import Path
import os

from .csvFileDS import csvFileDS
from .excelFileDS import excelFileDS
from .xesFileDS import xesFileDS
from pipelite.etlDataset import etlDataset

FILE_EXT_CSV = ".CSV"
FILE_EXT_EXCEL = ".XLSX"
FILE_EXT_XES = ".XES"
CFGFILES_DSOBJECT = "folderDS.json"

class folderDS(IDataSource):
    def __init__(self, config, log):
        super().__init__(config, log)
        self.folder = C.EMPTY
        self.files = C.EMPTY

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
            self.folder = cfg.getParameter('folder', 0)
            self.filenameFilter = cfg.getParameter('files', "*")

            # Checks ...

            return True
        except Exception as e:
            self.log.error("{}".format(e))
            return False

    def extract(self) -> etlDataset:
        """ Several tasks to do in this order:
            1) List the folder content
            2) filter out the interresting files 
            3) read the content of each file anc concatenate them in one dataframe
            Note: the structure (columns) must be the same.
        Returns:
            bool: False is any trouble when reading
            Only support xlsx, csv and xes files
        """
        globaldf = etlDataset()
        try:
            # Get the whole list of files to read
            fileList = [f for f in Path(self.folder).glob(self.files)]
            self.log.info("There are {} file(s) to read and concatenate".format(len(fileList)))
            for file in fileList:
                dataset = None
                self.log.info("Reading {} ...".format(file))
                _, ext = os.path.splitext(file)
                # Instantiate the right extractor
                if (ext.upper() == FILE_EXT_CSV):
                    dataset = csvFileDS(self.config, self.log)
                    dataset.filename = str(file)
                elif (ext.upper() == FILE_EXT_EXCEL):
                    dataset = excelFileDS(self.config, self.log)
                    dataset.filename = str(file)
                elif (ext.upper() == FILE_EXT_XES):
                    dataset = xesFileDS(self.log)
                    dataset.filename = str(file)

                # Extract the data and Merge/Concat with previous
                if (dataset != None):
                    if (not dataset.read()):
                        raise Exception("Error while reading {}".format(dataset.filename))
                    self.log.debug("Concatenating {} with {} rows and {} columns...".format(file, dataset.content.count, len(dataset.content.columns)))
                    globaldf.concatWith(dataset.content)

            self.log.debug("Final dataset has {} rows and {} columns...".format(globaldf.shape[0], globaldf.shape[1]))
            return globaldf
        
        except Exception as e:
            self.log.error("{}".format(e))
            return etlDataset()
