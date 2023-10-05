__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.interfaces.IPipeline import IPipeline
from pipelite.etlDatasets import etlDatasets
from pipelite.utils.etlReports import etlReports
from pipelite.utils.etlReport import etlReport

class directPL(IPipeline):

    def extract(self) -> int: 
        """This method must be surchaged and aims to collect the data from the datasource to provides the corresponding dataframe
        Returns:
            pd.DataFrame: Dataset in a pd.Dataframe object
        """
        try:
            totalCountExtracted = 0
            for item in self.extractors:
                self.log.info("Extracting data from the Data Source {}".format(item.name))
                report = self.report.getFromName(item.name)
                report.start()
                dsExtracted = item.extract()    # extract the data from the DataSource
                dsExtracted.name = item.name
                report.end(dsExtracted.count)
                if (dsExtracted.count > 0):
                    self.dsStack.add(dsExtracted)   # Add the Data source content (dataset) in the Dataset stack
                    self.log.info(" <{}> Rows: {} | Columns: {} ".format(dsExtracted.name, dsExtracted.count, dsExtracted.columns))
                    totalCountExtracted += dsExtracted.count
            return totalCountExtracted
        except Exception as e:
            self.log.error("{}".format(e))
            return 0
        
    def transform(self): 
        """ Make some modifications in the Dataset(s) after gathering the data and before loading
        Returns:
            pd.DataFrame: Output Dataframe
            int: Total Number of transformed rows
        """
        try:
            # Select only the Inputs needed for the transformer, initialize the data source stack with all extractors first
            # this dsPipelineStack object will store all the initial datasets (extractors) but also the transformation results.
            rowProcessed = 0

            # Execute the Transformers stack on the inputs/extractors
            for item in self.transformers:   # Process all the Tranformers ...
                report = self.report.getFromName(item.name)
                report.start()
                dsInputs = etlDatasets()
                for trName in item.dsInputs: # datasets in input per transformer
                    dsInputs.add(self.dsStack.getFromName(trName))
                if (not dsInputs.empty):
                    dsOutputs = item.transform(dsInputs)
                    self.dsStack.merge(dsOutputs)
                    rowProcessed += dsInputs.totalRowCount #- dsOutputs.totalRowCount
                    self.log.info("Number of rows processed {} / {}".format(rowProcessed, rowProcessed))
                else:
                    self.log.warning("The Tranformer {} has no input, by pass it !".format(item.name))
                report.end(dsInputs.totalRowCount)
            return rowProcessed
        except Exception as e:
            self.log.error("{}".format(e))
            return 0
        
    def load(self) -> int:
        """ Load the dataset transformed in one or more loaders.
            Only load the datasets which are referenced as Data Source Load and are in the Stack.
            Be Careful: the loaders are not in the stack by default (because they don't still have data)
            so To load, 2 options:
                1) Use a name which exists in the extractors
                2) Use a Tranformer to create a new dataset
        Args:
            dfDataset (pd.DataFrame): DataFrame with the Data to load in one or several data sources
        Returns:
            bool: False if error
        """
        try:
            totalCountLoaded = 0
            for item in self.loaders:
                # Load only the dataset which have a loader
                dsToLoad = self.dsStack.getFromName(item.name)
                if (dsToLoad == None):
                    self.log.warning("There are no data to load into the Data Source {}".format(item.name))
                else:
                    self.log.info("Loading content to the Data Source {}".format(dsToLoad.name))
                    report = self.report.getFromName(item.name)
                    report.start()
                    dsLoadedCount = item.load(dsToLoad)
                    report.end(dsLoadedCount)
                    totalCountLoaded += dsLoadedCount
                    self.log.info(" <{}> Rows: {} | Columns: {} ".format(dsToLoad.name, dsLoadedCount, dsToLoad.columns))
            return totalCountLoaded
        except Exception as e:
            self.log.error("{}".format(e))
            return 0