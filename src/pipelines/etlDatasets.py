__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import utils.constants as C
from .etlObject import etlObject

class etlDatasets(etlObject):
    def __init__(self):
        self.dataset = [] # Array of etlDataset

    def count(self):
        return len(self.dataset)
    
    def add(self, dataset):
        self.dataset.append(dataset)

    @property
    def names(self):
        return [ ds.name for ds in self.dataset ]

    def merge(self, etlOtherDatasets):
        for dsItem in etlOtherDatasets:
            if (dsItem.name in self.names): # replace the Dataset
                self.add(dsItem)
            else: # Add the dataset
                self.add(dsItem)

    def getFromName(self, name):
        for ds in self.dataset:
            if (self.dataset.name == name):
                return self.dataset
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