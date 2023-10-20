import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from pipelite.pipelineProcess import pipelineProcess
from pipelite.config.cmdLineConfig import cmdLineConfig

CONFIG_FOLDER = "./src/config/pipelines/"

class testDatasources(unittest.TestCase):
    def setUp(self):
        print("Running Test")

    def tearDown(self):
        print("End of Test")

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

    def test_csv2csv_direct(self):
        expected = {'S1': '3', 
                    'S2': '3', 
                    'T': '3'}
        result = self.processTest(CONFIG_FOLDER + "csv2csv_direct.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])

    def test_excel2csv_direct(self):
        expected = {'S1': '1394', 
                    'S2': '1394', 
                    'T': '1394'}
        result = self.processTest(CONFIG_FOLDER + "excel2csv_direct.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])

    def test_odbc2csv_direct(self):
        expected = {'S1': '6', 
                    'S2': '6', 
                    'T1': '6'}
        result = self.processTest(CONFIG_FOLDER + "odbc2csv_direct.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])

    def test_odbc2csv_direct_filequery(self):
        expected = {'S1': '6', 
                    'S2': '6', 
                    'T1': '6'}
        result = self.processTest(CONFIG_FOLDER + "odbc2csv_direct_filequery.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])
        
    def test_xes2csv_direct(self):
        expected = {'S1': '1394', 
                    'S2': '1394', 
                    'T': '1394'}
        result = self.processTest(CONFIG_FOLDER + "xes2csv_direct.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])

if __name__ == '__main__':
    unittest.main()