import unittest
import sys
import os

from pipelite.pipelineProcess import pipelineProcess
from pipelite.config.cmdLineConfig import cmdLineConfig

class pipeliteTests(unittest.TestCase):
    def setUp(self):
        print("Running Pipelite Test")

    def tearDown(self):
        print("End of Pipelite Test")

    def processTest(self, configfile):
        print("Process Test")
	    # Get configuration from cmdline & ini file
        config = cmdLineConfig.set_config(cfg="./src/config/pipelines/" + configfile)
        config.rootPath = "src/"
        log = pipelineProcess.getLogger(config)
        return pipelineProcess(config, log).process()

    def checkResults(self, expectedResult, result):
        # Check results
        for key, value in result.items():
            self.assertTrue(expectedResult[key]==value)