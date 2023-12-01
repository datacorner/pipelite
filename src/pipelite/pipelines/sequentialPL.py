__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.baseobjs.BOPipeline import BOPipeline
from pipelite.plDatasets import plDatasets
from pipelite.utils.plReports import plReports
import pipelite.constants as C

ITERATION_MAX = 10

class sequentialPL(BOPipeline):
    """ This class executes a pipeline like an ETL. It  takes each transformers in the order and try to execute it if the data are already extracted
    if not it goes to the next (and will retry later the non exetecuted ones). It can chain several transformers in sequence and manage temparoraries
    datasets (created in output by the transformer).
        execute calls in this order:
            1) all the extractions in a bundle
            2) all the transformers (depending on their in/out)
            1) all the loaders in a bundle 
    Args:
        BOPipeline (BOPipeline): pipeline template class processed by pipelineProcess
    """
    def __init__(self, config, log):
        super().__init__(config, log)
        # Index of execution
        self.objExecutionIndex = 1
        # Objects sorted
        self.objListOrdered = []
        # datasets pool: contains all the datasets managed by the pipeline
        self.dsPool = plDatasets()   
    
    def __addToExecutionList(self, object):
        """Used when the flow path is calculated to add the next object in the stack (for execution flow)
        Args:
            object (etlBaseObject): etl object
        """
        if (object.order == 0):
            self.objListOrdered.append(object)
            object.order = self.objExecutionIndex
            self.objExecutionIndex += 1

    def getSortedFlow(self):
        flow = [ item.id for item in self.objListOrdered ]
        return "{}".format(" > ".join(flow))

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
            self.log.debug("Pipeline preparation")
            objToExec = []
            iterCounter = 1
            idx = 1
            # go through all the Transformer and check if each extractor is used
            while (len(self.transformersNamesNotSorted) > 0 and iterCounter <= ITERATION_MAX):
                trsIteration = self.transformersNotSorted
                for tr in trsIteration:  # Go through all the transformers
                    trReady = True
                    trAvailableDS = []
                    for dsin in tr.dsInputs:    # check if the DS in inputs is already referenced or is in the extractors
                        if (objToExec.count(dsin) > 0): # datasource already managed ?
                            # Transformer is ready to exec
                            pass
                        elif (self.extractorsNames.count(dsin) > 0): # Datasource in the the extractors
                            trAvailableDS.append(dsin) 
                        else:
                            # transformer is not ready to execute, go to next
                            trReady = False
                            break
                    # if the transformer can be executed ...
                    if (trReady):
                        for item in trAvailableDS:  # add the datasources to the order list
                            obj = self.getObjectFromId(item)
                            self.__addToExecutionList(obj)
                        objToExec += trAvailableDS
                        self.__addToExecutionList(tr) # add the transformer to the order list
                        objToExec += tr.dsOutputs # Also add the transformer output in the available list now
                        idx += 1
                iterCounter +=1
            if (iterCounter >= ITERATION_MAX): # That means some of the transformers inputs do not exists ...
                self.log.warning("Some of the objects may not be properly configured in the configuration file, some of the transformers inputs may not exists for example.")
            # Manage the loaders now
            for ds in self.loaders:
                if (objToExec.count(ds.id) > 0):
                    self.__addToExecutionList(ds)
            self.log.info("Pipeline prepared successfully. Flow -> [{}]".format(self.getSortedFlow()))
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
                report = self.report.getFromId(obj.id)
                # -- EXTRACT
                if (obj.objtype == C.PLJSONCFG_EXTRACTOR):
                    self.extract(report, obj)
                # -- LOAD
                elif(obj.objtype == C.PLJSONCFG_LOADER):
                    self.load(report, obj)
                # -- TRANSFORM
                elif(obj.objtype == C.PLJSONCFG_TRANSFORMER):
                    self.transform(report, obj)
            return self.report
        except Exception as e:
            self.log.error("Error when processing the data: {}".format(str(e)))
            try:
                return self.report
            except:
                return plReports()
            
    def extract(self, report, obj):
        """ Manage extraction objects
        Args:
            report (plReport): report
            obj (etlBaseObject): Object to execute
        """
        reportDesc = "{} -> Output: [{}]".format(obj.__module__.split(".")[-1], obj.id)
        report.start(obj.order, reportDesc)
        dsExtracted = obj.read()
        report.end(dsExtracted.count)
        dsExtracted.id = obj.id
        self.dsPool.add(dsExtracted)
        self.log.info("[ EXTRACT {} - Rows: {} - Columns: {} ]".format(dsExtracted.id, dsExtracted.count, dsExtracted.columns))

    def transform(self, report, obj):
        """ Manage transformation objects
        Args:
            report (plReport): report
            obj (etlBaseObject): Object to execute
        """
        dsInputs = plDatasets()
        reportDesc = "{} -> Inputs: [{}] / Outputs: [{}]".format(obj.__module__.split(".")[-1], ",".join(obj.dsInputs), ",".join(obj.dsOutputs))
        report.start(obj.order, reportDesc)
        for trName in obj.dsInputs: # datasets in input per transformer
            dsInputs.add(self.dsPool.getFromId(trName))
        if (not dsInputs.empty):
            dsOutputs = obj.process(dsInputs)
            self.log.info("[ TRANSFORM {} - Rows: {} ]".format(obj.id, dsInputs.totalRowCount))
            report.end(dsInputs.totalRowCount)
            self.dsPool.merge(dsOutputs)
            self.log.info("Number of rows processed {} by {}".format(dsInputs.totalRowCount, obj.id))
        else:
            self.log.warning("The Tranformer {} has no input, by pass it !".format(obj.id))
            report.end(0)

    def load(self, report, obj):
        """ Manage load objects
        Args:
            report (plReport): report
            obj (etlBaseObject): Object to execute
        """
        dsToLoad = self.dsPool.getFromId(obj.id)
        reportDesc = "{} -> Input: [{}]".format(obj.__module__.split(".")[-1], obj.id)
        report.start(obj.order, reportDesc)
        if (not obj.write(dsToLoad)):
            raise Exception ("The Data Source {} could not be loaded properly".format(obj.id))
        report.end(dsToLoad.count)
        self.log.info("[ LOAD {} - Rows: {} - Columns: {} ]".format(dsToLoad.id, dsToLoad.count, dsToLoad.columns))

    def terminate(self) -> bool:
        # Display report
        self.log.info("Pipeline Report \n\n{}\n ".format(self.report.getFullSTRReport()))
        self.log.info("Warnings: {}".format(self.log.warningCounts))
        self.log.info("Errors: {}\n".format(self.log.errorCounts))
        self.log.info("*** End of Job treatment ***")
        return True