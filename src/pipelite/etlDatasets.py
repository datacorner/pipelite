__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from .dpObject import dpObject
from .etlDataset import etlDataset

class etlDatasets(dpObject):
    """ Manages a collection of etlDatasets
    Args:
        etlObject (app Object type): must be a Datasource object
    """
    def __init__(self):
        self.dataset = [] # Array of etlDataset

    @property
    def count(self) -> int:
        """Returns the Number of etlDatasets inside the collection
        Returns:
            int: dataset count
        """
        return len(self.dataset)
    
    @property
    def empty(self) -> bool:
        """ Empty dataset ?
        Returns:
            bool: True if empty
        """
        return (len(self.dataset) == 0)

    def add(self, dataset):
        self.dataset.append(dataset)

    @property
    def names(self):
        """ Returns a list with all etlDatasets names like [ "E1", ...., "En"]

        Returns:
            list: names list
        """
        return [ ds.name for ds in self.dataset ]

    def merge(self, etlOtherDatasets):
        """ merge 2 datasets together
        Args:
            etlOtherDatasets (etlDataset): dataset to merge with
        """
        for dsItem in etlOtherDatasets:
            if (not dsItem.name in self.names): 
                self.add(dsItem)

    def getFromName(self, name) -> etlDataset:
        """ Returns the dataset by searching it by id

        Args:
            name (str): dataset name/id

        Returns:
            etlDataset: dataset
        """
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