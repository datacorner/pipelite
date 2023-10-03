__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from abc import abstractmethod
from pipelite.etlDatasets import etlDatasets
from pipelite.dpObject import dpObject

class ITransformer(dpObject):
    """ The First transformer can manage several extractors but not the next. Transformers works in stack.
        Dataframe is after the 1st the way to manage the dataset in transformation.
    Args:
        etlObject: Basic ETL object interface
    """
    def __init__(self, config, log):
        self.dsInputs = []    # Input Dataset name List
        self.dsOutputs = []   # Output Dataset name List
        super().__init__(config, log)

    @abstractmethod
    def initialize(self, params) -> bool:
        """ Initialize and makes some checks (params) for that transformer
        Args:
            params (json): parameters
        Returns:
            bool: False if error
        """
        return True

    @abstractmethod
    def transform(self, dsTransformerInputs) -> etlDatasets:
        """ MUST BE OVERRIDED !
            Returns all the data in a DataFrame format
            This must:
                1) get from the dsStack list the input datasets
                2) return the datasets (with the right name)
        Args:
            dsTransformerInputs (etlDatasets): multiple dataframes in source in input for thtis transformer
        Returns:
            etlDatasets: Output etlDatasets of the transformer(s)
        """
        return etlDatasets()
