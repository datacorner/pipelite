__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import logging

ENCODING = "utf-8"
YES = "yes"
NO = "no"
EMPTY = ""
DEFCSVSEP = ","

# Command line parameters name
PARAM_PIPELINE_CONFIGFILE = "cfg"                         # pipeline configuration file (json)

# Configuration paths (pipeline configuration / JSON)
PLJSONCFG_LOGGER_LEVEL = "$.config.logger.level"
PLJSONCFG_LOGGER_FORMAT = "$.config.logger.format"
PLJSONCFG_LOGGER_PATH = "$.config.logger.path"
PLJSONCFG_LOGGER_FILENAME = "$.config.logger.filename"
PLJSONCFG_LOGGER_MAXBYTES = "$.config.logger.maxbytes"

# Logger configuration
TRACE_DEFAULT_LEVEL = logging.DEBUG
TRACE_DEFAULT_FORMAT = "%(asctime)s|%(name)s|%(levelname)s|%(message)s"
TRACE_DEFAULT_FILENAME = "dataplumber.log"
TRACE_DEFAULT_MAXBYTES = 1000000

