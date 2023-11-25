__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.baseobjs.BOPipeline import BOPipeline
from pipelite.utils.log import log
from pipelite.plBaseObject import plBaseObject
from pipelite.utils.plReports import plReports
from pipelite.plObject import plObject

class pipelineProcess(plObject):

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

	def process(self) -> plReports:
		""" Initialize the process and execute the pipeline
		Returns:
			int: Number of rows read
			int: Number of rows transformed
			int: Number of rows loaded
		"""
		reports = plReports()
		try:
			self.log.info("pipelite initialisation ...")
			pl = self.create()		# instantiate the pipeline
			if (pl == None):
				raise Exception ("The Data pipeline has not been created successfully")
			if not pl.initialize():	# check & initialize all the pipeline objects
				raise Exception ("The Data pipeline could not be initialized properly")
			if pl.prepare():		# prepare the pipeline for execution (like the flow)
				if (pl.beforeProcess()):
					reports = pl.execute()	# process the pipeline
				pl.afterProcess()
			pl.terminate()
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
			pipelineObject = plBaseObject.instantiate(fullClassPath, self.config, self.log)
			return pipelineObject
		except Exception as e:
			self.log.error("Error when loading the Data Source Factory: {}".format(str(e)))
			return None