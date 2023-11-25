__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from abc import abstractmethod
from pipelite.plDatasets import plDatasets
from pipelite.plBaseObject import plBaseObject

class BOTransformer(plBaseObject):
    """ The First transformer can manage several extractors but not the next. Transformers works in stack.
        Dataframe is after the 1st the way to manage the dataset in transformation.
    Args:
        etlObject: Basic ETL object interface
    """
    def __init__(self, config, log):
        self.dsInputs = []    # Input Dataset id List
        self.dsOutputs = []   # Output Dataset id List
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
    def process(self, dsTransformerInputs, outputNames) -> plDatasets:
        """ MUST BE OVERRIDED !
            Returns all the data in a DataFrame format
            This must:
                1) get from the dsStack list the input datasets
                2) return the datasets (with the right id)
        Args:
            dsTransformerInputs (etlDatasets): multiple dataset in input for the transformer
            outputNames (array): array of expected output names
        Returns:
            etlDatasets: Output etlDatasets of the transformer(s)
        """
        return plDatasets()
