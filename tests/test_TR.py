import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from pipelite.pipelineProcess import pipelineProcess
from pipelite.config.cmdLineConfig import cmdLineConfig

CONFIG_FOLDER = "./src/config/pipelines/"

class testTransformers(unittest.TestCase):
    def setUp(self):
        print("Running  import Test")

    def tearDown(self):
        print("End of  import Test")

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

    def test_csv2csv_extractstr(self):
        expected = {'S1': '1394', 
                    'S2': '1394', 
                    'T2': '1394'}
        result = self.processTest(CONFIG_FOLDER + "csv2csv_extractstr.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])

    def test_csv2csv_concat(self):
        expected = {'E1': '3', 
                    'E2': '4', 
                    'L1': '7', 
                    'T1': '7', 
                    'T2': '0'}
        result = self.processTest(CONFIG_FOLDER + "csv2csv_concat.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])

    def test_csv2csv_lookup(self):
        expected = {'E1': '3', 
                    'E2': '7', 
                    'T1': '9', 
                    'L1': '2'}
        result = self.processTest(CONFIG_FOLDER + "csv2csv_lookup.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])

    def test_csv2csv_concat_lookup(self):
        expected = {'I1': '3', 
                    'I2': '4', 
                    'I3': '7', 
                    'O2': '4', 
                    'T1': '7', 
                    'T2': '11', 
                    'T3': '0'}
        result = self.processTest(CONFIG_FOLDER + "csv2csv_concat_lookup.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])

    def test_csv2csv_renamecol(self):
        expected = {'S1': '1394', 
                    'S2': '1394', 
                    'T1': '1394'}
        result = self.processTest(CONFIG_FOLDER + "csv2csv_renamecol.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])

    def test_csv2csv_jinja(self):
        expected = {'S1': '3', 
                    'T': '3', 
                    'S2': '3'}
        result = self.processTest(CONFIG_FOLDER + "csv2csv_jinja.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])

    def test_csv2csv_join(self):
        expected = {'E1': '3', 
                    'E2': '7', 
                    'T1': '10', 
                    'L1': '3'}
        result = self.processTest(CONFIG_FOLDER + "csv2csv_join.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])

if __name__ == '__main__':
    unittest.main()