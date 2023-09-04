__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from abc import abstractmethod
from pipelines.etlDatasets import etlDatasets
from pipelines.etlObject import etlObject

class Transformer(etlObject):
    """ The First transformer can manage several extractors but not the next. Transformers works in stack.
        Dataframe is after the 1st the way to manage the dataset in transformation.
    Args:
        etlObject: Basic ETL object interface
    """
    def __init__(self, config, log):
        self.dsInputs = None    # Input Dataset name List
        self.dsOutputs = None   # Output Dataset name List
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
    def transform(self, dsStack):
        """ MUST BE OVERRIDED !
            Returns all the data in a DataFrame format
            This must:
                1) get from the dsStack list the input datasets
                2) return the datasets (with the right name)
        Args:
            inputDataStack (etlDatasets): multiple dataframes in source
        Returns:
            etlDatasets: Output etlDatasets of the transformer(s)
            int: Number of rows transformed
        """
        return etlDatasets(), 0
