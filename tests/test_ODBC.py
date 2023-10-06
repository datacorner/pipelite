import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from pipelite.pipelineProcess import pipelineProcess
from pipelite.config.cmdLineConfig import cmdLineConfig

class testODBC(unittest.TestCase):
    def setUp(self):
        print("Running ODBC import Test")

    def tearDown(self):
        print("End of ODBC import Test")

    def processTest(self, configfile):
        print("Process Test")
	    # Get configuration from cmdline & ini file
        config = cmdLineConfig.set_config(cfg=configfile)
        log = pipelineProcess.getLogger(config)
        return pipelineProcess(config, log).process()

    def checkResults(self, expectedResult, result):
        # Check results
        for key, value in result.items():
            print(key, "->", value)
            self.assertTrue(expectedResult[key]==value)

    def test_odbc2csv_direct(self):
        expected = {'S1': '6', 
                    'S2': '6', 
                    'T1': '6'}
        result = self.processTest("./src/config/pipelines/odbc2csv_direct.json")
        self.checkResults(expected, result["Rows Processed"])

    def test_odbc2csv_direct_filequery(self):
        expected = {'S1': '6', 
                    'S2': '6', 
                    'T1': '6'}
        result = self.processTest("./src/config/pipelines/odbc2csv_direct_filequery.json")
        self.checkResults(expected, result["Rows Processed"])

if __name__ == '__main__':
    unittest.main()