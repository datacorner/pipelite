from __future__ import annotations

__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from typing import Any, Optional

import pipelite.constants as C
import json
import jsonpath_ng

SECTION_PARAM_SEP = "."


class plConfig:
    """Configuration manager for pipeline settings.
    
    Loads and manages configuration from JSON files with JSONPath support
    for accessing nested configuration values.
    """
    
    def __init__(self):
        self.__parameters: dict[str, Any] = {}
        self.__pipelineConfig: Optional[dict] = None
        self.rootPath: str = C.EMPTY

    def addParameter(self, name: str, value: Any) -> None:
        """Add a new parameter to the dynamic parameter list.
        
        Args:
            name: Parameter name
            value: Parameter value
        """
        self.__parameters[name] = value

    def load(self, filename: str) -> bool:
        """Load configuration from a JSON file.
        
        Args:
            filename: Path to the JSON configuration file
            
        Returns:
            True if loaded successfully, False otherwise
        """
        try:
            with open(filename, 'r') as f:
                self.__pipelineConfig = json.load(f)
            return True
        except Exception as e:
            print("pipelineConfig.load() -> {}".format(e))
            return False

    def getParameter(self, paramPath: str, default: Any = C.EMPTY) -> Any:
        """Get a parameter value using JSONPath notation.
        
        First searches in the loaded configuration file using JSONPath,
        then falls back to the dynamic parameter list.
        
        Args:
            paramPath: JSONPath expression (e.g., '$.config.logger.level')
            default: Default value if parameter not found
            
        Returns:
            Parameter value or default if not found
        """
        try:
            # First search in the Configuration data file
            if self.__pipelineConfig is not None:
                jsonpath_expr = jsonpath_ng.parse(paramPath)
                extracted_data = jsonpath_expr.find(self.__pipelineConfig)
                if len(extracted_data) == 1:
                    return extracted_data[0].value
            
            # Secondly search into the parameter (dynamic) list
            return self.__parameters[paramPath]
        
        except (KeyError, Exception):
            return default
