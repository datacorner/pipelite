__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import argparse
from pipelite.pipelineProcess import pipelineProcess
from pipelite.config.cmdLineConfig import cmdLineConfig
from pipelite.plDataset import plDataset
from pipelite.plDatasets import plDatasets
from pipelite.plObject import plObject


def main() -> int:
	"""Entry point for the application script.
	
	Returns:
		Number of errors encountered during pipeline execution
	"""
	# Get configuration from cmdline & ini file
	config = cmdLineConfig.readConfig(argparse.ArgumentParser())
	# Get the logger
	log = pipelineProcess.getLogger(config)
	# Execute the pipeline 
	pipelineProcess(config, log).process()
	return log.errorCounts


__all__ = [
	"main",
	"pipelineProcess",
	"cmdLineConfig",
	"plDataset",
	"plDatasets",
	"plObject",
]