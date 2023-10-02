import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from pipelite.pipelineProcess import pipelineProcess
from pipelite.config.cmdLineConfig import cmdLineConfig

class testXES(unittest.TestCase):
    def setUp(self):
        print("Running XES Test")

    def tearDown(self):
        print("**** E:{} T:{} L:{} ****".format(self.e, self.t, self.l))
        print("End of XES Test")

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

    def test_xes2csv_direct(self):
        results = [1394, 0, 1394]
        self.e, self.t, self.l = self.processTest("./src/config/pipelines/xes2csv_direct.json")
        self.checkResults(results)

if __name__ == '__main__':
    unittest.main()