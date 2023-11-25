__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.plBaseObject import plBaseObject
from abc import abstractmethod
from pipelite.plConfig import plConfig
from pipelite.utils.plReports import plReports
from pipelite.plBaseObject import plBaseObject

ALL_OBJECTS = [C.PLJSONCFG_EXTRACTOR, C.PLJSONCFG_LOADER, C.PLJSONCFG_TRANSFORMER]

""" Pipeline Management rules:
    This Base Object MUST be inherited to create a pipeline
    1) a pipeline can have 
        * Many Extractors
        * Many Transformers
        * Many Loaders
"""
class BOPipeline(plBaseObject):
    def __init__(self, config, log):
        super().__init__(config, log)
        # Note: ETL objects does not contain any data, just the pipeline specifications 
        self.etlObjects = []   # all etlBaseObject (ETL)
        # reports / processing
        self.__report =  plReports()

    @property
    def transformers(self):
        return [ item for item in self.etlObjects if item.objtype == C.PLJSONCFG_TRANSFORMER ]
    @property
    def transformersNotSorted(self):
        return [ item for item in self.etlObjects if (item.objtype == C.PLJSONCFG_TRANSFORMER and item.order == 0) ] 
    @property
    def loaders(self):
        return [ item for item in self.etlObjects if item.objtype == C.PLJSONCFG_LOADER ] 
    @property
    def extractors(self):
        return [ item for item in self.etlObjects if item.objtype == C.PLJSONCFG_EXTRACTOR ] 
    @property
    def transformersNames(self):
        return [ item.id for item in self.etlObjects if item.objtype == C.PLJSONCFG_TRANSFORMER ]
    @property
    def transformersNamesNotSorted(self):
        return [ item.id for item in self.etlObjects if (item.objtype == C.PLJSONCFG_TRANSFORMER and item.order == 0) ] 
    @property
    def loadersNames(self):
        return [ item.id for item in self.etlObjects if item.objtype == C.PLJSONCFG_LOADER ] 
    @property
    def extractorsNames(self):
        return [ item.id for item in self.etlObjects if item.objtype == C.PLJSONCFG_EXTRACTOR ] 
    
    def getObjectFromId(self, id) -> plBaseObject:
        """Returns the object with the id in parameter
        Args:
            id (str): object id(in the config file)
        Returns:
            etlBaseObject: etl Object
        """
        for item in self.etlObjects:
            if (item.id == id):
                return item
        return None
    
    @property
    def report(self):
        return self.__report

    def __instantiateETLObject(self, paramJSONPath, ObjItem) -> plBaseObject:
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
            dsObj = plBaseObject.instantiate(dpConfig.className, self.config, self.log)
            dsObj.id = dpConfig.id
            dsObj.objtype = paramJSONPath
            self.log.debug("Initialize Object: {}".format(dpConfig.className))
            # Check the parameter (json) structure against the json scheme provided (if any, otherwise try to get the one by default)
            valFileCfg = dpConfig.validation if dpConfig.validation != None else dsObj.parametersValidationFile
            if (not self.validateParametersCfg(valFileCfg, dpConfig.parameters)):
                raise Exception("The {} parameters are not configured properly, check out the configuration file.".format(dsObj['id']))
            # some init considering the object
            if (paramJSONPath == C.PLJSONCFG_TRANSFORMER): # Only for transformers ...
                dsObj.dsInputs = dpConfig.inputs
                dsObj.dsOutputs = dpConfig.outputs
            if (dsObj.initialize(dpConfig)):
                # Add the object in the list
                self.log.debug("ETL Object {} instantiated and initialized successfully".format(dpConfig.className))
                self.__report.addEntry(dsObj.id, dsObj.objtype)
            else:
                raise Exception("Object {} cannot be initialized properly".format(dpConfig.className))
            return dsObj
        except Exception as e:
            self.log.error("{}".format(e))
            return None

    def __instantiateETLObjectsByCategory(self, paramJSONPath) -> bool:
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
        try:
            etlObjParams = self.config.getParameter(paramJSONPath, C.EMPTY)
            self.log.info("There is/are {} [{}] objects(es)".format(len(etlObjParams), paramJSONPath))
            self.log.debug("Intantiate needs and configured objects ...")
            # Initialize the Extractors/Transformers/Loaders
            for ObjItem in etlObjParams:
                etlobj = self.__instantiateETLObject(paramJSONPath, ObjItem)
                if (etlobj == None):
                    raise Exception("At least one pipeline object could not be initialized.")
                self.etlObjects.append(etlobj)
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
            allDS = []
            for objType in ALL_OBJECTS:
                objTree = self.config.getParameter(objType, C.EMPTY)
                for ObjItem in objTree:
                    allDS.append(ObjItem['id'])
            if len(set(allDS)) != len(allDS):
                raise Exception ("Each pipeline objects must have a unique id in the configuration file")
            # Instantiate all the ETL Objects (without flow ordering)
            for objType in ALL_OBJECTS:
                if not(self.__instantiateETLObjectsByCategory(objType)):
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
