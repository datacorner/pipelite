__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import utils.constants as C
from pipelite.dpObject import dpObject
from abc import abstractmethod
from jsonschema import validate
import json

""" Pipeline Management rules:
    1) a pipeline can have 
        * Many Extractors
        * Many Transformers
        * Many Loaders
    BUT:
        * If many extractors -> The first transformer must merge them in one dataframe
        * If many loaders 

"""
class Pipeline(dpObject):
    def __init__(self, config, log):
        super().__init__(config, log)
        self.extractors = []
        self.loaders = []
        self.transformers = []

    def __validateJSON(self, validationSchemeFile, jsonParameters) -> bool:
        """ Validate the JSON configuration (from parameters). This configuration stuff is specific to the object instantiated only.
            Use json-schema
        Args:
            validationSchemeFile (string): filename and path is a validation is needed, None else
            jsonParameters (json): parameters to check

        Returns:
            bool: _description_
        """
        try:
            # Check the parameter (json) structure against the json scheme provided (if any)
            if (validationSchemeFile != None):
                with open(validationSchemeFile, 'r') as f:
                    valScheme = json.load(f)
                # If no exception is raised by validate(), the instance is valid.
                validate(instance=jsonParameters, schema=valScheme)
            return True
            
        except Exception as e:
            self.log.error("pipeline.__validateJSON() -> {}".format(e))
            return False
        
    def __initETLObjects(self, paramJSONPath) -> list:
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
            # Initialize the Extractors/transformers
            for ds in etlObjParams:
                dsClassName = self.getValFromDict(ds, C.PLJSONCFG_PROP_CLASSNAME, C.EMPTY)
                self.log.info("Instantiate Object: {}".format(dsClassName))
                dsObj = dpObject.instantiate(dsClassName, self.config, self.log)
                # Initialize Extractors/transformers
                self.log.debug("Initialize Object: {}".format(dsClassName))
                objParams = self.getValFromDict(ds, C.PLJSONCFG_PROP_PARAMETERS)
                # Check the parameter (json) structure against the json scheme provided (if any)
                validationSchemeFile = self.getValFromDict(ds, C.PLJSONCFG_PROP_VALIDATION, None)
                if (not self.__validateJSON(validationSchemeFile, objParams)):
                    raise Exception("The {} parameters are not configured properly, check out the configuration file.".format(ds['name']))
                # some init considering the object
                if (paramJSONPath == C.PLJSONCFG_TRANSFORMER):
                    # Only for transformers ...
                    dsObj.dsInputs = self.getValFromDict(ds, C.PLJSONCFG_TRANSF_IN, [])
                    dsObj.dsOutputs = self.getValFromDict(ds, C.PLJSONCFG_TRANSF_OUT, [])
                if (dsObj.initialize(objParams)):
                    # Add the extractor in the pipeline list
                    self.log.debug("Object {} initialized successfully".format(dsClassName))
                    dsObj.name = self.getValFromDict(ds, C.PLJSONCFG_PROP_NAME, C.EMPTY)
                    dsObj.objtype = paramJSONPath
                    objectList.append(dsObj)
                else:
                    raise ("Object {} cannot be initialized properly".format(dsClassName))
            return objectList

        except Exception as e:
            self.log.error("pipeline.__initETLObjects() -> {}".format(e))
            return objectList
    
    def initialize(self) -> bool:
        """Initialize the pipeline object by gathering the Pipeline infos and initializing the etl objects
        Returns:
            bool: False if error
        """
        try:
            self.log.info("*** Starting pipeline processing ***")
            # 1) init Extractors
            self.log.info("Initializing Extractor(s) ...")
            self.extractors = self.__initETLObjects(C.PLJSONCFG_EXTRACTOR)
            if (len(self.extractors) == 0):
                raise Exception("Extractor(s) has/have not been initialized properly")
            self.log.info("There is/are {} extractor(s)".format(len(self.extractors)))

            # 2) init Loaders
            self.log.info("Initializing Loaders(s) ...")
            self.loaders = self.__initETLObjects(C.PLJSONCFG_LOADER)
            if (len(self.loaders) == 0):
                raise Exception("Loader(s) has/have not been initialized properly")
            self.log.info("There is/are {} loader(s)".format(len(self.loaders)))

            # 3) init Transformers
            self.log.info("Initializing Transformer(s) ...")
            self.transformers = self.__initETLObjects(C.PLJSONCFG_TRANSFORMER)
            if (len(self.transformers) == 0):
                raise Exception("Transformers(s) has/have not been initialized properly")
            self.log.info("There is/are {} Transformers(s)".format(len(self.transformers)))

            # CHECKS here ...
            # Check if No Inputs or No outputs
            # Warning if Loaders & Extractors have same names

            return True
        except Exception as e:
            self.log.error("pipeline.initialize() Error -> {}".format(e))
            return False

    def terminate(self) -> bool:
        # For surcharge
        self.log.info("*** End of Job treatment ***")
        return True
    
    @abstractmethod
    def extract(self) -> int: 
        """This method must be surchaged and aims to collect the data from the datasource to provides the corresponding dataframe
        Returns:
            pd.DataFrame: Dataset in a pd.Dataframe object
        """
        return 0
        
    @abstractmethod
    def transform(self): 
        """ Make some modifications in the Dataset(s) after gathering the data and before loading
        Returns:
            etlDataset: Output dataset
            int: Total Number of transformed rows
        """
        return None, 0
    
    @abstractmethod
    def load(self, dsPipelineStack) -> int:
        """ Load the dataset transformed in one or more loaders.
            Only load the datasets which are referenced as Data Source Load and are in the Stack.
            Be Careful: the loaders are not in the stack by default (because they don't still have data)
            so To load, 2 options:
                1) Use a name which exists in the extractors
                2) Use a Tranformer to create a new dataset
        Args:
            dfDataset (etlDataset): dataset with the Data to load in one or several data sources
        Returns:
            bool: False if error
        """
        return 0