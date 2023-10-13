import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from pipelite.pipelineProcess import pipelineProcess
from pipelite.config.cmdLineConfig import cmdLineConfig

class testCSVFiles(unittest.TestCase):
    def setUp(self):
        print("Running Test")

    def tearDown(self):
        print("End of Test")

    def processTest(self, configfile):
        print("Process Test")
	    # Get configuration from cmdline & ini file
        config = cmdLineConfig.set_config(cfg=configfile)
        config.rootPath = "src/"
        log = pipelineProcess.getLogger(config)
        return pipelineProcess(config, log).process()

    def checkResults(self, expectedResult, result):
        # Check results
        for key, value in result.items():
            print(key, "->", value)
            self.assertTrue(expectedResult[key]==value)

    def test_csv2csv_direct(self):
        expected = {'S1': '3', 
                    'S2': '3', 
                    'T': '3'}
        result = self.processTest("./src/config/pipelines/csv2csv_direct.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])

if __name__ == '__main__':
    unittest.main()