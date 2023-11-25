__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.plBaseObject import plBaseObject
from pipelite.plDataset import plDataset
from abc import abstractmethod

class BODataSource(plBaseObject):

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
    def read(self) -> plDataset:
        """ MUST BE OVERRIDED !
            Returns all the data in a DataFrame format
        Returns:
            etlDataset: Number of data read
        """
        self.log.error("This Data sources does not support reading/extracting")
        return plDataset(self.config, self.log)

    @abstractmethod
    def write(self, dataset) -> bool:
        """ MUST BE OVERRIDED !
            write the dataset in the datasource
        Returns:
            int: Number of data rows loaded
        """
        self.log.error("This Data sources does not support writing/loading")
        return False
