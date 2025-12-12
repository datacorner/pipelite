from __future__ import annotations

__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from typing import TYPE_CHECKING, Iterator, Optional

if TYPE_CHECKING:
    from .plDataset import plDataset

class plDatasets:
    """Manages a collection of plDataset objects.
    
    This class provides a container for multiple datasets with methods
    for adding, retrieving, and iterating over them.
    """
    def __init__(self):
        self.dataset: list[plDataset] = []  # Array of plDataset

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

    def add(self, dataset) -> None:
        """Add a dataset to the collection.
        
        Args:
            dataset: The plDataset to add (None values are ignored)
        """
        if dataset is not None:
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

    def getFromId(self, id: str) -> Optional[plDataset]:
        """Returns the dataset by searching it by id.
        
        Args:
            id: dataset id/name
            
        Returns:
            The matching plDataset or None if not found
        """
        for ds in self.dataset:
            if ds.id == id:
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
    
    def __iter__(self) -> Iterator[plDataset]:
        return iter(self.dataset)