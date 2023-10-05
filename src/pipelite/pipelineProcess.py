__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.interfaces.IPipeline import IPipeline
from pipelite.utils.log import log
from pipelite.dpObject import dpObject

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

	def process(self):
		""" Initialize the process and execute the pipeline
		Returns:
			int: Number of rows read
			int: Number of rows transformed
			int: Number of rows loaded
		"""
		try:
			# INSTANCIATE ONLY THE NEEDED CLASS / DATA SOURCE TYPE
			self.log.info("pipelite initialisation ...")
			pipeline = self.create()
			if (pipeline == None):
				raise Exception ("The Data pipeline has not been created successfully")
		except Exception as e:
			self.log.error("pipelite cannot be initialized: {}".format(str(e)))
			return
		return self.execute(pipeline=pipeline)

	def execute(self, pipeline) -> str:
		""" Execute the pipeline in this order:
				1) Extract the data sources
				2) Process the transformations
				3) load the data sources
		Returns:
			int: Number of rows read
			int: Number of rows transformed
			int: Number of rows loaded
		"""
		try:
			# PROCESS THE DATA
			if (pipeline.initialize()): # init logs here ...
				pipeline.log.info("pipelite has been initialized successfully")
				pipeline.log.info("--- EXTRACT ---")
				# EXTRACT (E of ETL)
				if (pipeline.extract()):
					pipeline.log.info("Data extracted successfully")
					pipeline.log.info("--- TRANSFORM ---")
					if (pipeline.transform()):	# TRANSFORM (T of ETL)
						pipeline.log.info("Data transformed successfully") 
						pipeline.log.info("--- LOAD ---")
						if (pipeline.load()): # LOAD (L of ETL)
							pipeline.log.info("Data loaded successfully")
			else:
				raise Exception("The Data pipeline has not been initialized properly")
			pipeline.terminate()
			return pipeline.report.getFullJSONReport()
		except Exception as e:
			self.log.error("Error when processing the data: {}".format(str(e)))
			try:
				return pipeline.report.getFullJSONReport()
			except:
				return "{}"

	def create(self) -> IPipeline:
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