__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import pandas as pd
from utils.log import log
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

    @property
    def dsMaxEntryCount(self):
        """ Number Max of Data Sources supported in entry. By default 1.
            Note: The first Transformer can support more than 1 ds in entry
        Returns:
            _type_: _description_
        """
        return 1

    def transform(self, inputDataFrames):
        """ Returns all the data in a DataFrame format
        Args:
            inputDataFrames (pd.DataFrame() []): multiple dataframes
        Returns:
            pd.DataFrame: Output Dataframe [] of the transformer(s)
            int: Number of rows transformed
        """
        return [ pd.DataFrame() ], 0
