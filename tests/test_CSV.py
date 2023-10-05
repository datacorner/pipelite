import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from pipelite.pipelineProcess import pipelineProcess
from pipelite.config.cmdLineConfig import cmdLineConfig

class testCSVFiles(unittest.TestCase):
    def setUp(self):
        print("Running CSV import Test")

    def tearDown(self):
        print("End of CSV import Test")

    def processTest(self, configfile):
        print("Process Test")
	    # Get configuration from cmdline & ini file
        config = cmdLineConfig.set_config(cfg=configfile)
        config.rootPath = "src/"
        log = pipelineProcess.getLogger(config)
        return pipelineProcess(config, log).process()

    def checkResults(self, expectedResult, result):
        # Check results
        for i in range(len(expectedResult)):
            self.assertTrue(expectedResult[i]==result[i])

    def test_csv2csv_direct(self):
        expected = {'0': '3', '1': '3', '2': '3'}
        result = self.processTest("./src/config/pipelines/csv2csv_direct.json")
        print (result["Rows Processed"])
        self.checkResults(expected, result)

if __name__ == '__main__':
    unittest.main()