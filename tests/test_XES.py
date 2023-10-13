import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from pipelite.pipelineProcess import pipelineProcess
from pipelite.config.cmdLineConfig import cmdLineConfig

class testXES(unittest.TestCase):
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

    def test_xes2csv_direct(self):
        expected = {'S1': '1394', 
                    'S2': '1394', 
                    'T': '1394'}
        result = self.processTest("./src/config/pipelines/xes2csv_direct.json")
        self.checkResults(expected, result.getFullJSONReport()["Rows Processed"])

if __name__ == '__main__':
    unittest.main()