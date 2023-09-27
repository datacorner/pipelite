__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.utils.constants as C
import json, jsonpath_ng

SECTION_PARAM_SEP = "."

class dpConfig():
    """This class contains all the configuration needed and loaded mainly from the INI file
    """

    def __init__(self):
        self.__parameters = {}
        self.__pipelineConfig = None
        return

    def addParameter(self, name, value):
        """Add a new parameter in the list
        Args:
            name (str): paramter name
            value (str): parameter value
        """
        try:
            self.__parameters[name] = value
        except Exception as e:
            print("addParameter() -> " + str(e))

    def load(self, filename) -> bool:
        """ Load the configuration from the INI file in parameter
        Args:
            filename (str): INI file name
        Returns:
            bool: False if error
        """
        try:
            with open(filename, 'r') as f:
                self.__pipelineConfig = json.load(f)
            return True
        except Exception as e:
            print("pipelineConfig.load() -> {}".format(e))
            return False

    def getParameter(self, paramPath, default=C.EMPTY) -> str:
        """ Returns the Parameter value based on the JSON Section & parameter name.
            If the parameter comes from the JSON Config file we use the SECTION_PARAM_SEP to separate the section with the parameter
        Args:
            parameter (str): INI parameter name (section.parameter)
            default (str): default value if not found (by default empty string)
        Returns:
            str: parameter value, empty if not found
        """
        try:
            # First search in the Configuration data file
            jsonpath_expr = jsonpath_ng.parse(paramPath) # example: $.config.logger.level
            extracted_data = jsonpath_expr.find(self.__pipelineConfig)
            if (len(extracted_data) == 1):
                return extracted_data[0].value
            
            # Secondly search into the parameter (dynamic) list
            return  self.__parameters[paramPath]
        
        except Exception as e:
            return default

    def addParameter(self, parameter, value):
        """ Add a new dynamic parameter
        Args:
            parameter (str): parameter name
            value (str): new value
        """
        try:
            self.__parameters[parameter] = str(value)
        except Exception as e:
            pass