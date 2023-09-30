__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import importlib
import pipelite.constants as C
from jsonschema import validate
import json
import importlib.resources

class dpObject:
    def __init__(self, config, log):
        self.log = log
        self.config = config
        self.name = ""
        self.ojbType = None

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

    def validateParametersCfg(self, inputValidationSchemeFile, jsonParameters) -> bool:
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
            if (inputValidationSchemeFile != C.EMPTY):
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
        """ This function dynamically instanciate the right data Class to create a pipeline object. Note: the class must inherit from the dpInstantiableObj class.
            This to avoid in loading all the connectors (if any of them failed for example) when making a global import, 
            by this way only the needed import is done on the fly
            Args:
                classname (str): full classname (must inherit from the dpInstantiableObj Class)
            Returns:
                Object (dpInstantiableObj): Object
        """
        try:
            # Get the class to instantiate
            if (fullClassPath == C.EMPTY):
                raise Exception("The {} parameter is mandatory and cannot be empty".format(fullClassPath))
            else:
                # Get the latest element : the class name without the path
                pipelineClass = fullClassPath.split(".")[-1]

            # Instantiate the object
            log.debug("pipelineFactory.instantiate(): Import module -> {}".format(fullClassPath))
            datasourceObject = importlib.import_module(name=fullClassPath)
            log.debug("pipelineFactory.instantiate(): Module {} imported, instantiate the class".format(fullClassPath))
            pipelineClassInst = getattr(datasourceObject, pipelineClass)
            objectInst = pipelineClassInst(config=config, log=log)
            log.info("Class instantiated successfully")
            return objectInst
        except Exception as e:
            log.error("etlObject.instantiate(): Error when loading the Class: {}".format(str(e)))
        return None