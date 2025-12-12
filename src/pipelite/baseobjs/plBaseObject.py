from __future__ import annotations

__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from typing import Any, Optional

import importlib
import pipelite.constants as C
from jsonschema import validate
import json
import importlib.resources
from pipelite.plObject import plObject

class plBaseObject(plObject):
    def __init__(self, config, log):
        super().__init__(config, log) 
        self.id = C.EMPTY       # id of the object
        self.objtype = None     # E, T or L object
        self.executed = False   # the object has beedn executed

    def getResourceFile(self, package, file) -> str:
        """ returns the filename needed to access to the data resource file stored in a package
        Args:
            package (str): package name in the project
            file (str): name of the resource file (without path)
        Returns:
            str: real path and file name for accessing the data resource 
        """
        try:
            return str(importlib.resources.files(package).joinpath(file))
        except Exception as e:
            self.log.error("{}".format(e))
            return C.EMPTY
    
    @property
    def parametersValidationFile(self):
        return C.EMPTY

    def validateParametersCfg(self, inputValidationSchemeFile: Optional[str], jsonParameters: dict) -> bool:
        """Validate the JSON configuration (from parameters).
        
        This configuration validation is specific to the object instantiated only.
        Uses json-schema for validation.
        
        Args:
            inputValidationSchemeFile: Filename and path for validation schema, or None/empty
            jsonParameters: Parameters to check
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Check the parameter (json) structure against the json scheme provided (if any)
            if inputValidationSchemeFile and inputValidationSchemeFile != C.EMPTY:
                with open(inputValidationSchemeFile, 'r') as f:
                    valScheme = json.load(f)
                # If no exception is raised by validate(), the instance is valid.
                validate(instance=jsonParameters, schema=valScheme)
            return True
        except Exception as e:
            self.log.error("{}".format(e))
            return False

    def initialize(self, params) -> bool:
        """ initialize and check all the needed configuration parameters
        Args:
            params: set of json parameters for the object initialisation
        Returns:
            bool: False if error
        """
        return True
    
    @staticmethod
    def instantiate(fullClassPath, config, log):
        """Dynamically instantiate a class from its full module path.
        
        This function dynamically instantiates the right data Class to create 
        a pipeline object. Note: the class must inherit from plBaseObject.
        This avoids loading all connectors (if any of them failed) when making 
        a global import - only the needed import is done on the fly.
        
        Args:
            fullClassPath: Full classname (must inherit from plBaseObject)
            config: Configuration object
            log: Logger instance
            
        Returns:
            Instantiated object or None if error
        """
        try:
            # Get the class to instantiate
            if fullClassPath == C.EMPTY:
                raise Exception("The fullClassPath parameter is mandatory and cannot be empty")
            
            # Get the latest element: the class name without the path
            pipelineClass = fullClassPath.split(".")[-1]

            # Instantiate the object
            log.debug("Import module -> {}".format(fullClassPath))
            datasourceObject = importlib.import_module(name=fullClassPath)
            log.debug("Module {} imported, instantiate the class".format(fullClassPath))
            pipelineClassInst = getattr(datasourceObject, pipelineClass)
            objectInst = pipelineClassInst(config=config, log=log)
            log.info("Class instantiated successfully")
            return objectInst
        except Exception as e:
            log.error("Error when loading the Class: {}".format(str(e)))
            return None