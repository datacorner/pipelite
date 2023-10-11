__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.baseobjs.BOPipeline import BOPipeline
from pipelite.utils.log import log
from pipelite.dpObject import dpObject
from pipelite.utils.etlReports import etlReports

class pipelineProcess:
	def __init__(self, config, log):
		self.__config = config
		self.__log = log

	@property
	def config(self):
		return self.__config
	@property
	def log(self) -> log:
		return self.__log
	
	@staticmethod
	def getLogger(config) -> log:
		if (config != None):
			# Init logger
			logfilename = config.getParameter(C.PLJSONCFG_LOGGER_PATH, C.EMPTY) + config.getParameter(C.PLJSONCFG_LOGGER_FILENAME, C.TRACE_DEFAULT_FILENAME)
			print("Log file: {}".format(logfilename))
			level = config.getParameter(C.PLJSONCFG_LOGGER_LEVEL, C.TRACE_DEFAULT_LEVEL)
			format = config.getParameter(C.PLJSONCFG_LOGGER_FORMAT, C.TRACE_DEFAULT_FORMAT)
			return log(__name__, logfilename, level, format)
		else:
			raise Exception ("Configuration failed, impossible to create the logger.")

	def process(self) -> etlReports:
		""" Initialize the process and execute the pipeline
		Returns:
			int: Number of rows read
			int: Number of rows transformed
			int: Number of rows loaded
		"""
		reports = etlReports()
		try:
			self.log.info("pipelite initialisation ...")
			pl = self.create()
			if (pl == None):
				raise Exception ("The Data pipeline has not been created successfully")
			if (pl.beforeProcess()):
				reports = pl.execute()
			pl.afterProcess()
			return reports
		except Exception as e:
			self.log.error("pipelite cannot be initialized: {}".format(str(e)))
			return reports

	def create(self) -> BOPipeline:
		""" This function dynamically instanciate the right data pipeline (manages ETL) class to create a pipeline object. 
			This to avoid in loading all the connectors (if any of them failed for example) when making a global import, 
			by this way only the needed import is done on the fly
			Args:
				pipeline (str): Datasource type
				config (config): Configuration set
			Returns:
				Object: Data Source Object
		"""
		try:
			# Get the pipeline class to instantiate from the config
			fullClassPath = self.config.getParameter(C.PLJSONCFG_PL_CLASSNAME, C.EMPTY)
			pipelineObject = dpObject.instantiate(fullClassPath, self.config, self.log)
			return pipelineObject
		except Exception as e:
			self.log.error("Error when loading the Data Source Factory: {}".format(str(e)))
			return None