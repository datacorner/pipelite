from __future__ import annotations

__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from typing import Any, Optional

import pipelite.constants as C
from jsonschema import validate
import json
import importlib.resources


class plConfig:
    """Configuration wrapper for individual ETL objects.
    
    Wraps the configuration for a specific extractor, transformer, or loader
    object, providing validation and parameter access.
    """
    
    def __init__(self, config, log, type: str, objconfig: dict):
        """Initialize object configuration wrapper.
        
        Args:
            config: Parent pipeline configuration
            log: Logger instance
            type: Object type (extractor, transformer, loader)
            objconfig: JSON configuration for this specific object
        """
        self.log = log
        self.config = config
        self.objectType = type
        self.objConfig = objconfig
        self.className: str = C.EMPTY
        self.parameters: dict = {}
        self.inputs: list[str] = []
        self.outputs: list[str] = []
        self.validation: Optional[str] = None
        self.name: str = C.EMPTY
        self.id: str = C.EMPTY

    def getVal(self, params: dict, name: str, default: Any = None) -> Any:
        """Safely get a value from a dictionary.
        
        Args:
            params: Dictionary to search
            name: Key name
            default: Default value if key not found
            
        Returns:
            Value or default if not found
        """
        return params.get(name, default)

    def validate(self) -> bool:
        """Validate the object configuration against the JSON schema.
        
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Check the parameter (json) structure against the json scheme for all etl objects
            filename = importlib.resources.files(C.RESOURCE_PKGFOLDER_ROOT).joinpath(C.RESOURCE_ETLOBJECTS_TEMPLATE)
            with open(str(filename), 'r') as f:
                valScheme = json.load(f)
            # If no exception is raised by validate(), the instance is valid.
            validate(instance=self.objConfig, schema=valScheme)
            return True
        except Exception as e:
            self.log.error("{}".format(e))
            return False
        
    def initialize(self) -> bool:
        """Initialize object configuration from the JSON config.
        
        Extracts className, parameters, inputs, outputs, and validation
        settings from the object configuration.
        
        Returns:
            True if initialization succeeds, False otherwise
        """
        try:
            # Get the object config elements first
            self.className = self.getVal(self.objConfig, C.PLJSONCFG_PROP_CLASSNAME, C.EMPTY)
            self.parameters = self.getVal(self.objConfig, C.PLJSONCFG_PROP_PARAMETERS, {})
            self.inputs = self.getVal(self.objConfig, C.PLJSONCFG_TRANSF_IN, [])
            self.outputs = self.getVal(self.objConfig, C.PLJSONCFG_TRANSF_OUT, [])
            self.validation = self.getVal(self.objConfig, C.PLJSONCFG_PROP_VALIDATION, None)
            self.id = self.getVal(self.objConfig, C.PLJSONCFG_PROP_NAME, C.EMPTY)
            return True
        except Exception as e:
            self.log.error("{}".format(e))
            return False

    def getParameter(self, name: str, default: Any = None) -> Any:
        """Get a specific parameter value.
        
        Args:
            name: Parameter name
            default: Default value if not found
            
        Returns:
            Parameter value or default
        """
        return self.getVal(self.parameters, name, default)