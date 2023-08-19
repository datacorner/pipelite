__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import utils.constants as C
from utils.log import log
import pandas as pd
from pipelines.datasources.DataSource import DataSource
import importlib
from .etlObject import etlObject

class pipeline(etlObject):
    def __init__(self, config, log):
        super().__init__(config, log)
        self.extractors = []
        self.loaders = []
        self.transformers = []

    def initETLObjects(self, etlObjParams) -> list:
        """ Initialize an set of similar etl object (can be a extractor, loader or transformer)
        Args:
            etlObjParams (JSON): Parameters for the etl Object. Look like something like this (below)
                [{
                    "classname": "pipelines.datasources.CSVFileDS",
                    "parameters": {
                        "separator": ",",
                        "filename": "test2.csv",
                        "path": "/tests/data/",
                        "encoding": "utf-8"
                }, { ... } ]
        Returns:
            list: list of etl objects initialized
        """
        objectList = []
        try:
            # Extractors init
            self.log.info("There is/are {} Class(es)".format(len(etlObjParams)))
            if (len(etlObjParams) < 1):
                raise ("At least one Object is needed for processing the pipeline.")
            # Initialize the Extractors
            for ds in etlObjParams:
                dsClassName = ds["classname"]
                self.log.info("Instantiate Object: {}".format(dsClassName))
                dsObj = etlObject.instantiate(dsClassName, self.config, self.log)
                # Initialize Extractor
                self.log.debug("Initialize Object: {}".format(dsClassName))
                if (dsObj.initialize(ds['parameters'])):
                    # Add the extractor in the pipeline list
                    self.log.debug("Object {} initialized successfully".format(dsClassName))
                    objectList.append(dsObj)
                else:
                    raise ("Object {} cannot be initialized properly".format(dsClassName))
            return objectList
        
        except Exception as e:
            self.log.error("pipeline.initETLObjects() -> {}".format(e))
            return objectList
    
    def initialize(self) -> bool:
        """Initialize the pipeline object by gathering the Pipeline infos and initializing the etl objects
        Returns:
            bool: False if error
        """
        try:
            self.log.info("*** Starting pipeline processing ***")
            # init Extractors
            self.log.info("Initializing Extractor(s) ...")
            self.extractors = self.initETLObjects(self.config.getParameter("$.extractors", C.EMPTY))
            if (len(self.extractors) == 0):
                raise Exception("Extractor(s) has/have not been initialized properly")
            self.log.info("There is/are {} extractor(s)".format(len(self.extractors)))

            # init Loaders
            self.log.info("Initializing Loaders(s) ...")
            self.loaders = self.initETLObjects(self.config.getParameter("$.loaders", C.EMPTY))
            if (len(self.loaders) == 0):
                raise Exception("Loader(s) has/have not been initialized properly")
            self.log.info("There is/are {} loader(s)".format(len(self.loaders)))

            # init Transformers
            self.log.info("Initializing Transformer(s) ...")
            self.transformers = self.initETLObjects(self.config.getParameter("$.transformers", C.EMPTY))
            if (len(self.transformers) == 0):
                raise Exception("Transformers(s) has/have not been initialized properly")
            self.log.info("There is/are {} Transformers(s)".format(len(self.transformers)))

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