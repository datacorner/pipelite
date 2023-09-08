__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import utils.constants as C
from .pipeline import pipeline
from utils.log import log
from fmk.etlObject import etlObject

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
			self.log.info("Data Plumber initialisation ...")
			pipeline = self.create()
			if (pipeline == None):
				raise Exception ("The Data pipeline has not been created successfully")
		except Exception as e:
			self.log.error("pipelineFactory.process(): Data Plumber cannot be initialized: {}".format(str(e)))
			return
		
		return self.execute(pipeline=pipeline)

	def execute(self, pipeline):
		""" Execute the pipeline
		Returns:
			int: Number of rows read
			int: Number of rows transformed
			int: Number of rows loaded
		"""
		E_counts, T_counts, L_counts = 0, 0, 0
		try:
			# PROCESS THE DATA
			if (pipeline.initialize()): # init logs here ...
				pipeline.log.info("Data Plumber has been initialized successfully")
				pipeline.log.info("Now, Extract data from Data Source ...")
				E_counts = pipeline.extract()	# EXTRACT (E of ETL)
				pipeline.log.info("Data extracted successfully: {} rows extracted".format(E_counts))
				if (E_counts == 0):
					pipeline.log.info("** There are no data to process, terminate here **")
				else:
					pipeline.log.info("Transform imported data ...")
					dsStack, T_counts = pipeline.transform()	# TRANSFORM (T of ETL)
					pipeline.log.info("Data transformed successfully, {} rows - after transformation - to import into the Data Source".format(T_counts))
					if (dsStack[0].count > 0): 
						# LOAD (L of ETL)
						pipeline.log.info("Load data into the Data Source ...")
						L_counts = pipeline.load(dsStack) # LOAD (L of ETL)
						if (L_counts > 0):
							pipeline.log.info("Data loaded successfully")
					pipeline.log.info("Pipeline Stats -> E:{} T:{} L:{}".format(E_counts, T_counts, L_counts))
			else:
				self.log.error("pipelineFactory.createAndExecute(): The Data pipeline has not been initialized properly")
			
			pipeline.terminate()
			return E_counts, T_counts, L_counts
		
		except Exception as e:
			self.log.error("pipelineFactory.createAndExecute(): Error when processing the data: {}".format(str(e)))
			return E_counts, T_counts, L_counts

	def create(self) -> pipeline:
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
			pipelineObject = etlObject.instantiate(fullClassPath, self.config, self.log)
			return pipelineObject

		except Exception as e:
			self.log.error("pipelineFactory.create(): Error when loading the Data Source Factory: {}".format(str(e)))
			return None