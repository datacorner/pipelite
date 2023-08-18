__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

from config.pipelineConfig import pipelineConfig
import utils.constants as C
from utils.log import log
import pandas as pd
from pipelines.datasources.DataSource import DataSource
MANDATORY_PARAM_LIST = []

class pipeline:
    def __init__(self, config, log):
        self.__config = config          # All the configuration parameters
        self.__log = log                # Logger

    # Contain the Extractor
    @property
    def extractor(self) -> DataSource:
        return self.__extractor
    @extractor.setter   
    def extractor(self, value):
        self.__extractor = value

    # Contain the Loader
    @property
    def loader(self) -> DataSource:
        return self.__loader
    @loader.setter   
    def loader(self, value):
        self.__loader = value

    # Contains all the config parameters (from the INI file)
    @property
    def config(self) -> pipelineConfig:
        return self.__config
    @config.setter   
    def config(self, value):
        self.__config = value
    
    # Contains the logger
    @property
    def log(self) -> log:
        return self.__log
    @log.setter   
    def log(self, value):
        self.__log = value

    def __checkIntegrity(self) -> bool:
        """ Checking stuff like:
            * parameters
            * metadata
            * datasource integrity
        Returns:
            bool: False si at least one mandatory param is missing
        """
        try:
            self.log.info("Check Extractor integrity ...")
            if (not self.extractor.checkIntegrity()):
                self.log.info("The Extractor is not available for extraction")
            self.log.info("Check Loader integrity ...")
            if (not self.loader.checkIntegrity()):
                self.log.info("The Loader is not available for loading")
            self.log.info("Both Extractor and Loader integrity checks are successful")
            return True
        
        except Exception as e:
            self.log.error("pipeline.__checkIntegrity() -> {}".format(e))
            return False
    
    def initialize(self) -> bool:
        """Initialize the Class instance by gathering the Pipeline infos.
            * set Extractor, Loader and Transformer(s)
            * initialize the logger
            * check the mandatory parameters
            * init the API (get the Pipeline infos)
        Returns:
            bool: False if error
        """
        try:
            # Checking parameters
            self.log.info("*** Starting pipeline processing ***")
            if (not self.__checkIntegrity()):
                raise Exception("Integrity checks for Data Sources has failed")
            return True
        except Exception as e:
            self.log.error("pipeline.initialize() Error -> {}".format(e))
            return False

    def terminate(self) -> bool:
        # For surcharge
        self.log.info("*** End of Job treatment ***")
        return True
    
    def extract(self) -> pd.DataFrame: 
        """This method must be surchaged and aims to collect the data from the datasource to provides the corresponding dataframe
        Returns:
            pd.DataFrame: Dataset in a pd.Dataframe object
        """
        self.log.info("*** Extraction treatment ***")
        df = self.extractor.extract()
        return df

    def transform(self, df) -> pd.DataFrame: 
        """ Surcharge this method to enable modification in the Dataset after gathering the data and before loading
            By default just manage the event mapping.
        Args:
            df (pd.DataFrame): source dataset
        Returns:
            pd.DataFrame: altered dataset
        """
        self.log.info("*** Data Transformation treatment ***")
        return df

    def load(self, dfDataset) -> bool:
        """ Surcharge this method to upload a dataset (Pandas DataFrame)
        Args:
            dfDataset (pd.DataFrame): DataFrame with the Data to upload
        Returns:
            bool: False if error
        """
        self.log.info("*** Loading treatment ***")
        return True
    
    def afterLoad(self) -> bool:
        """ Surcharge this method to manage tasks after loading the dataset
        Returns:
            bool: False if error
        """
        self.log.info("*** After Loading treatment ***")
        return True