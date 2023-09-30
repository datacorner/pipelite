__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
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
PLJSONCFG_PL_CLASSNAME = "$.classname"
PLJSONCFG_LOGGER_LEVEL = "$.config.logger.level"
PLJSONCFG_LOGGER_FORMAT = "$.config.logger.format"
PLJSONCFG_LOGGER_PATH = "$.config.logger.path"
PLJSONCFG_LOGGER_FILENAME = "$.config.logger.filename"
PLJSONCFG_LOGGER_MAXBYTES = "$.config.logger.maxbytes"
PLJSONCFG_EXTRACTOR = "$.extractors"
PLJSONCFG_LOADER = "$.loaders"
PLJSONCFG_TRANSFORMER = "$.transformers"
PLJSONCFG_PROP_CLASSNAME = "classname"
PLJSONCFG_PROP_NAME = "name"
PLJSONCFG_PROP_PARAMETERS = "parameters"
PLJSONCFG_PROP_VALIDATION = "validation"
PLJSONCFG_TRANSF_IN = "inputs"
PLJSONCFG_TRANSF_OUT = "outputs"

# Logger configuration
TRACE_DEFAULT_LEVEL = logging.DEBUG
TRACE_DEFAULT_FORMAT = "%(asctime)s|%(name)s|%(levelname)s|%(message)s"
TRACE_DEFAULT_FILENAME = "pipelite.log"
TRACE_DEFAULT_MAXBYTES = 1000000

# Config files
RESOURCE_PKGFOLDER_ROOT = "config.parameters"
RESOURCE_PKGFOLDER_DATASOURCES = RESOURCE_PKGFOLDER_ROOT + ".datasources"
RESOURCE_PKGFOLDER_TRANSFORMERS = RESOURCE_PKGFOLDER_ROOT + ".transformers"
RESOURCE_ETLOBJECTS_TEMPLATE = "etlObjects.json"