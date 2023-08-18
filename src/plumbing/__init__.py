__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import argparse
from pipelines.pipelineFactory import pipelineFactory
from config.cmdLineConfig import cmdLineConfig
import utils.constants as C

def main() -> None:
	"""Entry point for the application script"""
	
	# Get configuration from cmdline & ini file
	config = cmdLineConfig.readConfig(argparse.ArgumentParser())
	# Get the logger
	log = pipelineFactory.getLogger(config)
	# Execute the pipeline 
	pipelineFactory(config, log).process()