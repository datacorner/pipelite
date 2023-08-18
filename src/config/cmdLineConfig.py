__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import utils.constants as C
from config.pipelineConfig import pipelineConfig

class cmdLineConfig:
	
	@staticmethod
	def manageArgs(args):
		""" manage the arguments in command line with the ini config file
		Args:
			args (_type_): command line arguments
		Returns:
			appConfig: cinfiguration object
		"""
		config = pipelineConfig()
		# Load configuration via the INI file
		if (args[C.PARAM_PIPELINE_CONFIGFILE] != 0):
			config.load(args[C.PARAM_PIPELINE_CONFIGFILE])
		else:
			raise Exception("Missing config file argument {}".format(C.PARAM_PIPELINE_CONFIGFILE))
		return config

	@staticmethod
	def readConfig(parser):
		""" This function gather the arguments sent in the CLI and build the configuration object / USE FOR INI FILE CONFIGURATION FILE ONLY
		Args:
			parser (argparse.ArgumentParser): CLI arguments
		Raises:
			Exception: Unable to gather the CLI args
		Returns:
			utils.appConfig: config object
			string: Data Source Tag (command line)
		"""
		try:
			# Parser CLI arguments
			parser.add_argument("-" + C.PARAM_PIPELINE_CONFIGFILE, help="Pipeline Configuration file with all configuration details (JSON)", required=True)
			args = vars(parser.parse_args())
			config = cmdLineConfig.manageArgs(args)
			return config

		except Exception as e:
			print("ERROR> {}".format(e))
			parser.print_help()
			return None
		
	@staticmethod
	def emulate_readIni(configfile):
		""" This function gather the arguments sent in the CLI and build the configuration object / USE FOR INI FILE CONFIGURATION FILE ONLY
		Args:
			parser (argparse.ArgumentParser): CLI arguments
		Raises:
			Exception: Unable to gather the CLI args
		Returns:
			utils.appConfig: config object
			string: Data Source Tag (command line)
		"""
		try:
			config = pipelineConfig()
			# Check Data Source Type
			args = dict(configfile=configfile)
			config = cmdLineConfig.manageArgs(args)
			return config

		except Exception as e:
			print("ERROR> " + str(e))
			return None