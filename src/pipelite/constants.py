__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import logging

# Global constants
ENCODING = "utf-8"
YES = "yes"
NO = "no"
EMPTY = ""
DEFCSVSEP = ","

# Report
DATE_FORMAT = "%Y-%m-%d %H:%M:%S" 
SEPRPT = ""
TABRPT = "\t"

# Log levels
LOG_LEVEL_DEBUG = "DEBUG"
LOG_LEVEL_INFO = "INFO"
LOG_LEVEL_WARNING = "WARNING"
LOG_LEVEL_ERROR = "ERROR"
DEFAULT_LOG_FORMAT = "%(asctime)s|%(name)s|%(levelname)s|%(message)s"

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
PLJSONCFG_PROP_NAME = "id"
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
RESOURCE_TEMPLATES_ROOT = "config.templates"
RESOURCE_TEMPLATE_PROFILE = "profile.html"

# ABBYY Timeline PI API
API_1_0 = "/api/ext/1.0/"
API_REPOSITORY_CONFIG = "repository/repository-import-configuration"
API_SERVER_UPLOAD_INFOS = "repository/{}/file/upload-url"
API_SERVER_LOAD_2_REPO = "repository/{}/load"
API_PROCESSING_STATUS = "processing"
API_EXECUTE_TODO = "repository/{}/execute-todo-list"
API_DEF_WAIT_DURATION_SEC = 2
API_DEF_NB_ITERATION_MAX = 60
API_STATUS_IN_PROGRESS = "IN_PROGRESS"
API_STATUS_ERROR = "ERROR"
API_BLOC_SIZE_LIMIT = 10000 # Same limitation as the current API call via java