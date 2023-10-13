__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.etlBaseObject import etlBaseObject
from abc import abstractmethod
from pipelite.plConfig import plConfig
from pipelite.plDatasets import plDatasets
from pipelite.utils.plReports import plReports

""" Pipeline Management rules:
    This Base Object MUST be inherited to create a pipeline
    1) a pipeline can have 
        * Many Extractors
        * Many Transformers
        * Many Loaders
"""
class BOPipeline(etlBaseObject):
    def __init__(self, config, log):
        super().__init__(config, log)
        # Note: ETL objects does not contain any data, just the pipeline specifications 
        self.extractors = []            # etlBaseObject list with all extractors     (type -> pipelite.datasources.BODataSource)
        self.loaders = []               # etlBaseObject list with all loaders        (type -> pipelite.datasources.BODataSource)
        self.transformers = []          # etlBaseObject list with all transformers   (type -> pipelite.datasources.BOTransformer)
        # datasets stack (contains the data) managed by the pipeline
        self.dsStack = plDatasets()   
        # reports / processing
        self.__report =  plReports()

    @property
    def report(self):
        return self.__report

    def __instantiateETLObject(self, paramJSONPath, ObjItem) -> etlBaseObject:
        """ Initialize an set one etl object one by one considering the configuration (can be a extractor, 
            loader or transformer)
        Args:
            paramJSONPath (str): Category in the configuration file
            ObjItem (json): configuration piece for the object
        Returns:
            etlBaseObject: etl object instantiated
        """
        try:
            dpConfig = plConfig(self.config, self.log, paramJSONPath, ObjItem)
            self.log.debug("Validate and initialize Object...")
            if not dpConfig.validate():
                raise Exception("Impossible to validate the object configuration")
            if not dpConfig.initialize():
                raise Exception("Impossible to initialize the object")
            # instantiate & Init the object
            self.log.info("Instantiate Object: {}".format(dpConfig.className))
            dsObj = etlBaseObject.instantiate(dpConfig.className, self.config, self.log)
            self.log.debug("Initialize Object: {}".format(dpConfig.className))
            # Check the parameter (json) structure against the json scheme provided (if any, otherwise try to get the one by default)
            valFileCfg = dpConfig.validation if dpConfig.validation != None else dsObj.parametersValidationFile
            if (not self.validateParametersCfg(valFileCfg, dpConfig.parameters)):
                raise Exception("The {} parameters are not configured properly, check out the configuration file.".format(dsObj['name']))
            # some init considering the object
            if (paramJSONPath == C.PLJSONCFG_TRANSFORMER): # Only for transformers ...
                dsObj.dsInputs = dpConfig.inputs
                dsObj.dsOutputs = dpConfig.outputs
            if (dsObj.initialize(dpConfig)):
                # Add the object in the list
                self.log.debug("ETL Object {} instantiated and initialized successfully".format(dpConfig.className))
                dsObj.name = dpConfig.name
                dsObj.objtype = paramJSONPath
                self.__report.addEntry(dsObj.name, dsObj.objtype)
            else:
                raise Exception("Object {} cannot be initialized properly".format(dpConfig.className))
            return dsObj
        except Exception as e:
            self.log.error("{}".format(e))
            return None

    def __instantiateETLObjectsByCategory(self, paramJSONPath) -> list:
        """ Initialize an set the ETL objects one by one considering the configuration (can be a extractor, 
            loader or transformer) and only for one category:
                $.extractor
                $.loaders
                $.transformers
        Args:
            paramJSONPath (str): Category in the configuration file 
        Returns:
            list: list of all etl objects initialized per category (E, T or L)
        """
        objectList = []
        try:
            etlObjParams = self.config.getParameter(paramJSONPath, C.EMPTY)
            # DS or TR to init
            self.log.info("There is/are {} [{}] objects(es)".format(len(etlObjParams), paramJSONPath))
            if (len(etlObjParams) < 1):
                raise Exception("At least one Object is needed for processing the pipeline!")
            self.log.debug("Intantiate needs and configured objects ...")
            # Initialize the Extractors/Transformers/Loaders
            for ObjItem in etlObjParams:
                etlobj = self.__instantiateETLObject(paramJSONPath, ObjItem)
                objectList.append(etlobj)
            return objectList
        except Exception as e:
            self.log.error("{}".format(e))
            return objectList
    
    def __instantiateAllETLObjects(self) -> bool:
        """Initialize all the pipeline object by gathering the Pipeline infos and initializing the etl objects
        Returns:
            bool: False if error
        """
        try:
            self.log.info("*** Starting pipeline processing ***")
            # 1) init Extractors
            self.log.info("Initializing Extractor(s) ...")
            self.extractors = self.__instantiateETLObjectsByCategory(C.PLJSONCFG_EXTRACTOR)
            if (len(self.extractors) == 0):
                raise Exception("Extractor(s) has/have not been initialized properly")
            self.log.info("There is/are {} extractor(s)".format(len(self.extractors)))
            # 2) init Loaders
            self.log.info("Initializing Loaders(s) ...")
            self.loaders = self.__instantiateETLObjectsByCategory(C.PLJSONCFG_LOADER)
            if (len(self.loaders) == 0):
                raise Exception("Loader(s) has/have not been initialized properly")
            self.log.info("There is/are {} loader(s)".format(len(self.loaders)))
            # 3) init Transformers
            self.log.info("Initializing Transformer(s) ...")
            self.transformers = self.__instantiateETLObjectsByCategory(C.PLJSONCFG_TRANSFORMER)
            self.log.info("There is/are {} Transformers(s)".format(len(self.transformers)))
            return True
        except Exception as e:
            self.log.error("{}".format(e))
            return False

    def initialize(self) -> bool:
        """Initialize the pipeline object by gathering the Pipeline infos and initializing all the etl objects and putting them in the 
        self.extractors, self.loaders and self.transformers arrays
        Returns:
            bool: False if error
        """
        try:
            # CHECK that all the objects names are unique here
            objects = [C.PLJSONCFG_EXTRACTOR, 
                       C.PLJSONCFG_LOADER, 
                       C.PLJSONCFG_TRANSFORMER]
            allDS = []
            for objType in objects:
                objTree = self.config.getParameter(objType, C.EMPTY)
                for ObjItem in objTree:
                    allDS.append(ObjItem['name'])
            if len(set(allDS)) != len(allDS):
                raise Exception ("Each pipeline objects must have a unique name in the configuration file")
            # Initialize the ETL Objects
            if not(self.__instantiateAllETLObjects()):
                raise Exception ("All the pipeline objects could not be configured and initialized properly")
            return True
        except Exception as e:
            self.log.error("{}".format(e))
            return False

    @abstractmethod
    def terminate(self) -> bool:
        return True
    
    @abstractmethod
    def execute(self) -> plReports:
        return None

    @abstractmethod
    def beforeProcess(self) -> bool:
        return True

    @abstractmethod
    def afterProcess(self) -> bool:
        return True
    
    @abstractmethod
    def prepare(self) -> bool:
        return True
