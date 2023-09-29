__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.utils.constants as C
from jsonschema import validate
import json

class objConfig:
    def __init__(self, config, log, type, objconfig):
        self.log = log
        self.config = config
        self.objectType = type
        self.objConfig = objconfig
        self.className = C.EMPTY
        self.parameters = {}
        self.inputs = []
        self.outputs = []
        self.validation = None
        self.name = C.EMPTY

    def getVal(self, params, name, default=None):
        """ return the param[name] value, if does not exist returns default
        Args:
            params (dict): python dict
            name (str): name
            default (obj, optional): Default value
        Returns:
            obj: value
        """
        try:
            return params[name] 
        except:
            return default

    def validate(self) -> bool:
        try:
            # Check the parameter (json) structure against the json scheme for all etl objects
            with open(C.CFGFILES_ETLOBJECT, 'r') as f:
                valScheme = json.load(f)
            # If no exception is raised by validate(), the instance is valid.
            validate(instance=self.objConfig, schema=valScheme)
            return True
        except Exception as e:
            self.log.error("objConfig.validate() Error: {}".format(e))
            return False
        
    def initialize(self) -> bool:
        try:
            # Get the object config elements first
            self.className = self.getVal(self.objConfig, C.PLJSONCFG_PROP_CLASSNAME, C.EMPTY)
            self.parameters = self.getVal(self.objConfig, C.PLJSONCFG_PROP_PARAMETERS)
            self.inputs = self.getVal(self.objConfig, C.PLJSONCFG_TRANSF_IN, [])
            self.outputs = self.getVal(self.objConfig, C.PLJSONCFG_TRANSF_OUT, [])
            self.validation = self.getVal(self.objConfig, C.PLJSONCFG_PROP_VALIDATION, None)
            self.name = self.getVal(self.objConfig, C.PLJSONCFG_PROP_NAME, C.EMPTY)
            return True
        except Exception as e:
            self.log.error("objConfig.initialize() Error: {}".format(e))
            return False

    def getParameter(self, name, default=None) -> str:
        return self.getVal(self.parameters, name, default)