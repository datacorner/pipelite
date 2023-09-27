__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.utils.constants as C
from pipelite.parents.Pipeline import Pipeline
from pipelite.etlDatasets import etlDatasets

class directPL(Pipeline):
    def __init__(self, config, log):
        super().__init__(config, log)

    def extract(self) -> int: 
        """This method must be surchaged and aims to collect the data from the datasource to provides the corresponding dataframe
        Returns:
            pd.DataFrame: Dataset in a pd.Dataframe object
        """
        try:
            totalCountExtracted = 0
            self.log.info("*** Extraction treatment ***")
            for item in self.extractors:
                self.log.info("Extracting data from the Data Source {}".format(item.name))
                if (item.extract()):
                    self.log.info(" <{}> Rows: {} | Columns: {} ".format(item.name, item.count, item.content.columns))
                    totalCountExtracted += item.count
            return totalCountExtracted
        except Exception as e:
            self.log.error("pipeline.extract() Error -> {}".format(e))
            return 0
        
    def transform(self): 
        """ Make some modifications in the Dataset(s) after gathering the data and before loading
        Returns:
            pd.DataFrame: Output Dataframe
            int: Total Number of transformed rows
        """
        try:
            self.log.info("*** Data Transformation treatment ***")
            # Select only the Inputs needed for the transformer, initialize the data source stack with all extractors first
            # this dsPipelineStack object will store all the initial datasets (extractors) but also the transformation results.
            dsPipelineStack = etlDatasets()
            for extractorItem in self.extractors:   
                self.log.debug("Adding Extractor/dataset {} in the data stack".format(extractorItem.name))
                extractorItem.content.name = extractorItem.name
                dsPipelineStack.add(extractorItem.content)
            totalCountTransformed = 0

            # Execute the Transformers stack on the inputs/extractors
            for transformerItem in self.transformers:   # Pass through all the Tranformers ...
                dsInputs = etlDatasets()
                for dsItem in dsPipelineStack:
                    if (dsItem.name in transformerItem.dsInputs):
                        self.log.info("Including dataset {} for the transformation".format(dsItem.name))
                        self.log.info(" <{}> Rows: {} | Columns: {} ".format(dsItem.name, dsItem.count, dsItem.columns))
                        dsInputs.add(dsItem.copy())
                self.log.info("Apply Transformation via transformer {} ...".format(transformerItem.name))
                if (not dsInputs.empty):
                    dsOutputs, tfCount = transformerItem.transform(dsInputs)
                    dsPipelineStack.merge(dsOutputs)
                    totalCountTransformed += tfCount
                    self.log.info("Number of rows transformed {} / {}".format(tfCount, totalCountTransformed))
                else:
                    self.log.warning("The Tranformer {} has no input, by pass it !".format(transformerItem.name))

            return dsPipelineStack, totalCountTransformed
        except Exception as e:
            self.log.error("pipeline.transform() Error -> {}".format(e))
            return None, 0
        
    def load(self, dsPipelineStack) -> int:
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
            self.log.info("*** Loading treatment ***")
            for item in self.loaders:
                # Load only the dataset which have a loader
                dsToLoad = dsPipelineStack.getFromName(item.name)
                if (dsToLoad == None):
                    self.log.warning("There are no data to load into the Data Source {}".format(item.name))
                else:
                    self.log.info("Loading content to the Data Source {}".format(dsToLoad.name))
                    item.content = dsToLoad
                    totalCountLoaded += item.load()
                    self.log.info(" <{}> Rows: {} | Columns: {} ".format(item.name, item.count, item.content.columns))
            return totalCountLoaded
        except Exception as e:
            self.log.error("pipeline.load() Error -> {}".format(e))
            return 0