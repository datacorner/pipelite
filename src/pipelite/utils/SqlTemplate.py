__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
import pathlib
from string import Template
import os

class SqlTemplate():
    def __init__(self, log):
        self.__log__ = log
    
    def getTemplate(self, template) -> Template:
        """ returns the template SQL file
        Args:
            filename (_type_): filename (from the INI database.query parameter)
        Returns:
            Template: Return the String template
        """
        try:
            if (os.path.exists(template)):
                # The SQL Query is in a separate file
                return Template(pathlib.Path(template).read_text())
            else:
                # The SQL Query is in the config file (json)
                return Template(template)
        except Exception as e:
            self.__log__.error("Error when reading the SQL template: {}".format(e))
            return ""

    def getQuery(self, entryTemplate, queryParameters) -> str:
        """Build the SQL Query based on a string template (stored in a file)
        Args:
            entryTemplate (str): query in a template format
            queryParameters (dict): dict with all the pair (template name, value)
        Returns:
            str: built SQL Query
        """
        try: 
            # Get the query skeleton in the sql file
            sqlTemplate = self.getTemplate(entryTemplate)
            # replace the values in the template
            return sqlTemplate.substitute(queryParameters)

        except Exception as e:
            self.__log__.error("Unable to build the Blue Prism Query: {}".format(e))
            return C.EMPTY