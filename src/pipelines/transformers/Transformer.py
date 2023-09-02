__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from abc import abstractmethod
from pipelines.etlDataset import etlDataset
from pipelines.etlObject import etlObject

class Transformer(etlObject):
    """ The First transformer can manage several extractors but not the next. Transformers works in stack.
        Dataframe is after the 1st the way to manage the dataset in transformation.
    Args:
        etlObject: Basic ETL object interface
    """
    def __init__(self, config, log):
        self.datasources = 1    # Number of Datasource to apply the transformer with
        self.extractors = None
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

    @property
    def dsMaxEntryCount(self):
        """ Number Max of Data Sources supported by this transformer in entry. By default it's 1.
            Note: The first Transformer can support more than 1 ds in entry
        Returns:
            int: Number max of datasources
        """
        return 1

    @abstractmethod
    def transform(self, inputDataFrames):
        """ MUST BE OVERRIDED !
            Returns all the data in a DataFrame format
        Args:
            inputDataFrames (etlDataset []): multiple dataframes
        Returns:
            pd.DataFrame: Output etlDataset [] of the transformer(s)
            int: Number of rows transformed
        """
        return [ etlDataset() ], 0
