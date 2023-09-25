import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from pipelines.pipelineProcess import pipelineProcess
from config.cmdLineConfig import cmdLineConfig

class testCSVFiles(unittest.TestCase):
    def setUp(self):
        print("Running CSV import Test")

    def tearDown(self):
        print("**** E:{} T:{} L:{} ****".format(self.e, self.t, self.l))
        print("End of CSV import Test")

    def processTest(self, configfile):
        print("Process Test")
	    # Get configuration from cmdline & ini file
        config = cmdLineConfig.set_config(cfg=configfile)
        log = pipelineProcess.getLogger(config)
        return pipelineProcess(config, log).process()

    def checkResults(self, expectedResults):
        # Check results
        self.assertTrue(self.e==expectedResults[0] and 
                        self.t==expectedResults[1] and 
                        self.l==expectedResults[2])

    def test_csv2csv_direct(self):
        results = [3, 0, 3]
        self.e, self.t, self.l = self.processTest("./config/pipelines/csv2csv_direct.json")
        self.checkResults(results)

    def test_csv2csv_concat(self):
        results = [7, 7, 7]
        self.e, self.t, self.l = self.processTest("./config/pipelines/csv2csv_concat.json")
        self.checkResults(results)

    def test_csv2csv_lookup(self):
        results = [10, 1, 2]
        self.e, self.t, self.l = self.processTest("./config/pipelines/csv2csv_lookup.json")
        self.checkResults(results)

    def test_csv2csv_concat_lookup(self):
        results = [14, 10, 4]
        self.e, self.t, self.l = self.processTest("./config/pipelines/csv2csv_concat_lookup.json")
        self.checkResults(results)

if __name__ == '__main__':
    unittest.main()