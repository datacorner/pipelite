__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

from pipelines.etlObject import etlObject
from pipelines.etlDataset import etlDataset

class DataSource(etlObject):
    def __init__(self, config, log):
        self.content = etlDataset()
        super().__init__(config, log)

    @property
    def count(self):
        try:
            return self.content.count
        except:
            return 0

    def extract(self) -> int:
        """ Returns all the data in a DataFrame format
        Returns:
            int: Number of data read
        """
        return 0

    def load(self) -> int:
        """ write the dataset in the datasource
        Returns:
            int: Number of data rows loaded
        """
        return 0
