__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.baseobjs.BOPipeline import BOPipeline
from pipelite.plDatasets import plDatasets
from pipelite.utils.plReports import plReports
import pipelite.constants as C

from pipelite.pipelines.management.plTree import plTree

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

    def prepare(self) -> bool:
        """ Calculate the order of execution for each object in the pipeline. Each object.order property is updated
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
            pipe = plTree()
            pipe.load(self.etlObjects)
            self.objListOrdered = pipe.buildSeqPipeline()
            self.log.info("Pipeline prepared successfully. Flow -> [{}]".format(" -> ".join(self.objListOrdered)))
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
            for objId in self.objListOrdered:
                report = self.report.getFromId(objId)
                action = self.getObjectFromId(objId)
                # -- EXTRACT
                if (action.objtype == C.PLJSONCFG_EXTRACTOR):
                    self.log.info("Extract from dataset: {}".format(objId))
                    self.extract(report, action)
                # -- LOAD
                elif(action.objtype == C.PLJSONCFG_LOADER):
                    self.log.info("Load from dataset: {}".format(objId))
                    self.load(report, action)
                # -- TRANSFORM
                elif(action.objtype == C.PLJSONCFG_TRANSFORMER):
                    self.log.info("Apply Transformation: {}".format(objId))
                    self.transform(report, action)
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
        # Read the dataset (+ report)
        report.start(reportDesc)
        dsExtracted = obj.read()
        report.end(dsExtracted.count)
        # Add the Dataset to the pool
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
        report.start(reportDesc)
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
        report.start(reportDesc)
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