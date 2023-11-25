__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from .plBaseObject import plBaseObject
from .plDataset import plDataset

class plDatasets(plBaseObject):
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
    def totalRowCount(self) -> int:
        """Returns the Number of all the etlDataset count inside the collection
        Returns:
            int: all datasets count
        """
        count = 0
        for i in self.dataset:
            count += i.count
        return count

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
        return [ ds.id for ds in self.dataset ]

    def merge(self, etlOtherDatasets):
        """ merge 2 datasets together
        Args:
            etlOtherDatasets (etlDataset): dataset to merge with
        """
        for dsItem in etlOtherDatasets:
            if (not dsItem.id in self.names): 
                self.add(dsItem)

    def isInside(self, id) -> bool:
        """Returns True if the dataset (via id) exists in the collection
        Args:
            id (str): plDataset id

        Returns:
            bool: True is in the collection
        """
        for ds in self.dataset:
            if (ds.id == id):
                return True
        return False

    def getFromId(self, id) -> plDataset:
        """ Returns the dataset by searching it by id
        Args:
            id (str): dataset id/id
        Returns:
            etlDataset: dataset
        """
        for ds in self.dataset:
            if (ds.id == id):
                return ds
        return None

    def __getitem__(self, item):
        """ Makes the Data column accessible via [] array
            example: df['colName']
        Args:
            item (str): attribute/column id
        Returns:
            object: data
        """
        return self.dataset.__getitem__(item)
    
    def __iter__(self):
        return iter(self.dataset)