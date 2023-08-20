__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import utils.constants as C
from utils.log import log
import pandas as pd
from .etlObject import etlObject

""" Pipeline Management rules:
    1) a pipeline can have 
        * Many Extractors
        * Many Transformers
        * Many Loaders
    BUT:
        * If many extractors -> The first transformer must merge them in one dataframe
        * If many loaders 

"""
class pipeline(etlObject):
    def __init__(self, config, log):
        super().__init__(config, log)
        self.extractors = []
        self.loaders = []
        self.transformers = []

    def initETLObjects(self, paramJSONPath) -> list:
        """ Initialize an set of similar etl object (can be a extractor, loader or transformer)
        Args:
            etlObjParams (JSON): "$.extractors"
            Parameters for the etl Object. Look like something like this (below)
                [{
                    "name": "...",
                    "classname": "pipelines.datasources.ObjClassExample",
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
            etlObjParams = self.config.getParameter(paramJSONPath, C.EMPTY)
            # Extractors init
            self.log.info("There is/are {} Class(es)".format(len(etlObjParams)))
            if (len(etlObjParams) < 1):
                raise ("At least one Object is needed for processing the pipeline.")
            # Initialize the Extractors
            for ds in etlObjParams:
                dsClassName = ds[C.PLJSONCFG_PROP_CLASSNAME]
                self.log.info("Instantiate Object: {}".format(dsClassName))
                dsObj = etlObject.instantiate(dsClassName, self.config, self.log)
                # Initialize Extractor
                self.log.debug("Initialize Object: {}".format(dsClassName))
                if (dsObj.initialize(ds[C.PLJSONCFG_PROP_PARAMETERS])):
                    # Add the extractor in the pipeline list
                    self.log.debug("Object {} initialized successfully".format(dsClassName))
                    dsObj.name = ds[C.PLJSONCFG_PROP_NAME]
                    dsObj.objtype = paramJSONPath
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
            self.extractors = self.initETLObjects(C.PLJSONCFG_EXTRACTOR)
            if (len(self.extractors) == 0):
                raise Exception("Extractor(s) has/have not been initialized properly")
            self.log.info("There is/are {} extractor(s)".format(len(self.extractors)))

            # init Loaders
            self.log.info("Initializing Loaders(s) ...")
            self.loaders = self.initETLObjects(C.PLJSONCFG_LOADER)
            if (len(self.loaders) == 0):
                raise Exception("Loader(s) has/have not been initialized properly")
            self.log.info("There is/are {} loader(s)".format(len(self.loaders)))

            # init Transformers
            self.log.info("Initializing Transformer(s) ...")
            self.transformers = self.initETLObjects(C.PLJSONCFG_TRANSFORMER)
            if (len(self.transformers) == 0):
                raise Exception("Transformers(s) has/have not been initialized properly")
            self.log.info("There is/are {} Transformers(s)".format(len(self.transformers)))

            # The first transfomer must have a Nb of ds equal to the number of extractors
            if (self.transformers[0].dsMaxEntryCount != len(self.extractors)):
                raise Exception("The first transfomer must have a number of Data Sources equal to the number of extractors")

            return True
        except Exception as e:
            self.log.error("pipeline.initialize() Error -> {}".format(e))
            return False

    def terminate(self) -> bool:
        # For surcharge
        self.log.info("*** End of Job treatment ***")
        return True
    
    def extract(self) -> int: 
        """This method must be surchaged and aims to collect the data from the datasource to provides the corresponding dataframe
        Returns:
            pd.DataFrame: Dataset in a pd.Dataframe object
        """
        totalCountExtracted = 0
        self.log.info("*** Extraction treatment ***")
        for item in self.extractors:
            self.log.info("Extracting data from the Data Source {}".format(item.name))
            if (item.extract()):
                self.log.info("Number of rows extracted {}".format(item.count))
                totalCountExtracted += item.count
        return totalCountExtracted

    def transform(self): 
        """ Make some modifications in the Dataset(s) after gathering the data and before loading
        Returns:
            pd.DataFrame: Output Dataframe
            int: Total Number of transformed rows
        """
        self.log.info("*** Data Transformation treatment ***")
        # Create a list with all the extractors
        inputdf = []
        totalCountTransformed = 0
        for extractorItem in self.extractors:
            inputdf.append(extractorItem.content)
        # Execute the Transformers stack on the inputs/extractors
        # 1st transformer can manage several inputs, not the nexts
        for item in self.transformers:
            self.log.info("Transforming data via transformer {}".format(item.name))
            inputdf, tfCount = item.transform(inputdf)
            totalCountTransformed += tfCount
            self.log.info("Number of rows transformed {} / {}".format(tfCount, totalCountTransformed))
        return inputdf[0], totalCountTransformed

    def load(self, dfDataset) -> int:
        """ Load the dataset transformed in one or more loaders
        Args:
            dfDataset (pd.DataFrame): DataFrame with the Data to load in one or several data sources
        Returns:
            bool: False if error
        """
        totalCountLoaded = 0
        self.log.info("*** Loading treatment ***")
        for item in self.loaders:
            item.content = dfDataset[0]
            self.log.info("Loading content to the Data Source {}".format(item.name))
            totalCountLoaded += item.load()
            self.log.info("Number of rows extracted {}".format(totalCountLoaded))
        return totalCountLoaded