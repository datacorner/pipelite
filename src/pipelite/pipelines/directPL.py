__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.interfaces.IPipeline import IPipeline
from pipelite.etlDatasets import etlDatasets

class directPL(IPipeline):
    def __init__(self, config, log):
        super().__init__(config, log)

    def extract(self) -> int: 
        """This method must be surchaged and aims to collect the data from the datasource to provides the corresponding dataframe
        Returns:
            pd.DataFrame: Dataset in a pd.Dataframe object
        """
        try:
            totalCountExtracted = 0
            self.log.info("*** EXTRACT ***")
            for item in self.extractors:
                self.log.info("Extracting data from the Data Source {}".format(item.name))
                dsExtracted = item.extract()    # extract the data from the DataSource
                dsExtracted.name = item.name
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
            self.log.info("*** TRANSFORM ***")
            # Select only the Inputs needed for the transformer, initialize the data source stack with all extractors first
            # this dsPipelineStack object will store all the initial datasets (extractors) but also the transformation results.
            totalCountTransformed = 0

            # Execute the Transformers stack on the inputs/extractors
            for transformerItem in self.transformers:   # Pass through all the Tranformers ...
                dsInputs = etlDatasets()
                for trName in transformerItem.dsInputs: # datasets in input per transformer
                    dsInputs.add(self.dsStack.getFromName(trName))
                if (not dsInputs.empty):
                    iCountBefore = dsInputs.totalRowCount
                    dsOutputs = transformerItem.transform(dsInputs)
                    self.dsStack.merge(dsOutputs)
                    totalCountTransformed += iCountBefore #- dsOutputs.totalRowCount
                    self.log.info("Number of rows transformed {} / {}".format(totalCountTransformed, totalCountTransformed))
                else:
                    self.log.warning("The Tranformer {} has no input, by pass it !".format(transformerItem.name))
            return totalCountTransformed
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
            self.log.info("*** LOAD ***")
            for item in self.loaders:
                # Load only the dataset which have a loader
                dsToLoad = self.dsStack.getFromName(item.name)
                if (dsToLoad == None):
                    self.log.warning("There are no data to load into the Data Source {}".format(item.name))
                else:
                    self.log.info("Loading content to the Data Source {}".format(dsToLoad.name))
                    totalCountLoaded += item.load(dsToLoad)
                    self.log.info(" <{}> Rows: {} | Columns: {} ".format(dsToLoad.name, dsToLoad.count, dsToLoad.columns))
            return totalCountLoaded
        except Exception as e:
            self.log.error("{}".format(e))
            return 0