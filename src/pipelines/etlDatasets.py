__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import utils.constants as C
from .etlObject import etlObject
from .etlDataset import etlDataset

class etlDatasets(etlObject):
    def __init__(self):
        self.dataset = [] # Array of etlDataset

    @property
    def count(self) -> int:
        return len(self.dataset)
    
    @property
    def empty(self) -> bool:
        return (len(self.dataset) == 0)

    def add(self, dataset):
        self.dataset.append(dataset)

    @property
    def names(self):
        return [ ds.name for ds in self.dataset ]

    def merge(self, etlOtherDatasets):
        for dsItem in etlOtherDatasets:
            if (not dsItem.name in self.names): 
                self.add(dsItem)

    def getFromName(self, name) -> etlDataset:
        for ds in self.dataset:
            if (ds.name == name):
                return ds
        return None

    def __getitem__(self, item):
        """ Makes the Data column accessible via [] array
            example: df['colName']
        Args:
            item (str): attribute/column name
        Returns:
            object: data
        """
        return self.dataset.__getitem__(item)