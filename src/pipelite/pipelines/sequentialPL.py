__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.baseobjs.BOPipeline import BOPipeline
from pipelite.plDatasets import plDatasets
from pipelite.utils.plReports import plReports
import pipelite.constants as C

ITERATION_MAX = 10

class sequentialPL(BOPipeline):
    """ This class executes a pipeline like an ETL
        execute calls in this order:
            1) all the extractions in a bundle
            2) all the transformers (depending on their in/out)
            1) all the loaders in a bundle 
    Args:
        BOPipeline (BOPipeline): pipeline template class processed by pipelineProcess
    """
    def __init__(self, config, log):
        super().__init__(config, log)
        self.objExecutionIndex = 1
        self.objListOrdered = []

    def __addToExecutionList(self, object):
        if (object.order == 0):
            self.objListOrdered.append(object)
            object.order = self.objExecutionIndex
            self.objExecutionIndex += 1

    def prepare(self) -> bool:
        """ Calcultate the order of execution for each object in the pipeline. Each object.order property is updated
        after this method is executed (0 means the object will not be executed)
            Algorithm:
                1) iterate through all the transformers (Max 10 iterations / ITERATION_MAX)
                2) per transfromer, iterate through all the datasource IN
                    * check if the Datasource IN exists
                    * if not check in the extractors and the availableDS list, then add the new one in the availableDS
                    * at the end add the transformer in the order list, and also its outputs
                4) Add the needed loaders
        Returns:
            bool: No problem if True
        """
        try:
            availableDS = []
            iterationNb = 1
            # go through all the Transformer and check if each extractor is used
            # result = any(item in test_list for item in test_list)
            trOrderIndex = 1
            while (len(self.transformersNamesNotOrdered) > 0 and iterationNb <= ITERATION_MAX):
                trsIteration = self.transformersNotOrdered
                for tr in trsIteration:  # Go through all the transformers
                    trReady = True
                    trAvailableDS = []
                    for dsin in tr.dsInputs:    # check if the DS in inputs is already referenced or is in the extractors
                        if (availableDS.count(dsin) > 0):
                            # Transformer is ready to exec
                            pass
                        elif (self.extractorsNames.count(dsin) > 0):
                            trAvailableDS.append(dsin) #self.getObjectFromName(dsin))
                        else:
                            # transformer is not ready to execute, go to next
                            trReady = False
                            break
                    # if the transformer can be executed ...
                    if (trReady):
                        for item in trAvailableDS:  # add the datasources to the order list
                            obj = self.getObjectFromName(item)
                            self.__addToExecutionList(obj)
                        availableDS += trAvailableDS
                        self.__addToExecutionList(tr) # add the transformer to the order list
                        availableDS += tr.dsOutputs # Also add the transformer output in the available list now
                        trOrderIndex += 1
                iterationNb +=1
            # Manage the loaders now
            for ds in self.loaders:
                if (availableDS.count(ds.name) > 0):
                    self.__addToExecutionList(ds)
            return True
        
        except Exception as e:
            self.log.error("Error when preparing the pipeline: {}".format(str(e)))
            return False

    def execute(self) -> plReports:
        """ Execute the pipeline in this order:
				1) Extract the data sources
				2) Process the transformations
				3) load the data sources
		Returns:
			int: Number of rows read
			int: Number of rows transformed
			int: Number of rows loaded
		"""
        try:
            for obj in self.objListOrdered:
                report = self.report.getFromName(obj.name)
                # -- EXTRACT
                if (obj.objtype == C.PLJSONCFG_EXTRACTOR):
                    reportDesc = "{} -> Output: [{}]".format(obj.__module__.split(".")[-1], obj.name)
                    report.start(obj.order, reportDesc)
                    dsExtracted = obj.read()
                    report.end(dsExtracted.count)
                    dsExtracted.name = obj.name
                    self.dsStack.add(dsExtracted)
                    self.log.info("[ EXTRACT {} - Rows: {} - Columns: {} ]".format(dsExtracted.name, dsExtracted.count, dsExtracted.columns))
                # -- LOAD
                elif(obj.objtype == C.PLJSONCFG_LOADER):
                    dsToLoad = self.dsStack.getFromName(obj.name)
                    reportDesc = "{} -> Input: [{}]".format(obj.__module__.split(".")[-1], obj.name)
                    report.start(obj.order, reportDesc)
                    if (not obj.write(dsToLoad)):
                        raise Exception ("The Data Source {} could not be loaded properly".format(obj.name))
                    report.end(dsToLoad.count)
                    self.log.info("[ LOAD {} - Rows: {} - Columns: {} ]".format(dsToLoad.name, dsToLoad.count, dsToLoad.columns))
                # -- TRANSFORM
                elif(obj.objtype == C.PLJSONCFG_TRANSFORMER):
                    dsInputs = plDatasets()
                    reportDesc = "{} -> Inputs: [{}] / Outputs: [{}]".format(obj.__module__.split(".")[-1], ",".join(obj.dsInputs), ",".join(obj.dsOutputs))
                    report.start(obj.order, reportDesc)
                    for trName in obj.dsInputs: # datasets in input per transformer
                        dsInputs.add(self.dsStack.getFromName(trName))
                    if (not dsInputs.empty):
                        dsOutputs = obj.process(dsInputs)
                        self.log.info("[ TRANSFORM {} - Rows: {} ]".format(obj.name, dsInputs.totalRowCount))
                        report.end(dsInputs.totalRowCount)
                        self.dsStack.merge(dsOutputs)
                        self.log.info("Number of rows processed {} by {}".format(dsInputs.totalRowCount, obj.name))
                    else:
                        self.log.warning("The Tranformer {} has no input, by pass it !".format(obj.name))
                        report.end(0)
            return self.report

        except Exception as e:
            self.log.error("Error when processing the data: {}".format(str(e)))
            try:
                return self.report
            except:
                return plReports()
        
    def terminate(self) -> bool:
        # Display report
        self.log.info("Pipeline Report \n\n{}\n ".format(self.report.getFullSTRReport()))
        self.log.info("*** End of Job treatment ***")
        return True