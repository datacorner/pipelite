__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.dpObject import dpObject
from pipelite.etlDataset import etlDataset
from abc import abstractmethod

class IDataSource(dpObject):
    def __init__(self, config, log):
        super().__init__(config, log)

    @abstractmethod
    def initialize(self, cfg) -> bool:
        """ Initialize and makes some checks (params) for that datasource
        Args:
            cfg (objConfig): parameters
        Returns:
            bool: False if error
        """
        return True

    @abstractmethod
    def read(self) -> etlDataset:
        """ MUST BE OVERRIDED !
            Returns all the data in a DataFrame format
        Returns:
            etlDataset: Number of data read
        """
        self.log.error("This Data sources does not support reading/extracting")
        return etlDataset()

    @abstractmethod
    def write(self, dataset) -> bool:
        """ MUST BE OVERRIDED !
            write the dataset in the datasource
        Returns:
            int: Number of data rows loaded
        """
        self.log.error("This Data sources does not support writing/loading")
        return False
