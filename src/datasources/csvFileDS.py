__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from .DataSource import DataSource 
import utils.constants as C
import os

class csvFileDS(DataSource):

    def __init__(self, config, log):
        super().__init__(config, log)
        self.separator = C.DEFCSVSEP
        self.filename = C.EMPTY
        self.encoding = C.ENCODING

    def initialize(self, params) -> bool:
        """ initialize and check all the needed configuration parameters
            A CSV Extractor/Loader must have:
                * A filename
                * A separator
                * A path name
                * An Encoding type
                params['separator']
        Args:
            params (json list) : params for the data source.
                example: {'separator': ',', 'filename': 'test2.csv', 'path': '/tests/data/', 'encoding': 'utf-8'}
        Returns:
            bool: False if error
        """
        try:
            self.separator = self.getValFromDict(params, 'separator', C.EMPTY)
            self.filename = os.path.join(self.getValFromDict(params, 'path', C.EMPTY), 
                                         self.getValFromDict(params, 'filename', C.EMPTY))
            self.encoding = self.getValFromDict(params, 'encoding', C.ENCODING)

            # Checks ...
            if (self.ojbType == C.PLJSONCFG_LOADER):
                if (not os.path.isfile(self.filename)):
                    raise Exception("The file {} does not exist or is not accessible.".format(self.filename))
            
            return True
        except Exception as e:
            self.log.error("CSVFileDS.initialize() Error: {}".format(e))
            return False
    
    def extract(self) -> int:
        """ Returns all the data in a DataFrame format
        Returns:
            pd.DataFrame(): dataset read
        """
        try:
            self.log.info("Extract the Dataset from the file: {}".format(self.filename))
            self.content.readCSV(filename=self.filename, 
                                 encoding=self.encoding, 
                                 separator=self.separator)
            return self.content.count
        except Exception as e:
            self.log.error("CSVFileDS.extract() Error while reading the file: ".format(e))
            return False

    def load(self) -> int:
        """ write the dataset in the datasource
        Returns:
            int: Number of data rows loaded
        """
        try:
            self.log.info("Load  the Dataset into the file: {}".format(self.filename))
            self.content.writeCSV(filename=self.filename, 
                                  encoding=C.ENCODING,
                                  separator=self.separator)
            return self.content.count
        except Exception as e:
            self.log.error("CSVFileDS.extract() Error while writing the file: ".format(e))
            return 0