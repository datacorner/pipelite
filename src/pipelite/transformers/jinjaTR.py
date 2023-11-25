__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.baseobjs.BOTransformer import BOTransformer
from pipelite.plDatasets import plDatasets
from jinja2 import Template

CFGFILES_DSOBJECT = "jinjaTR.json"
PARAM_TEMPLATE = "template"
PARAM_COLUMN_TARGET = "column-name"
PARAM_CONSTANTS = "constants"

class jinjaTR(BOTransformer):

    @property
    def parametersValidationFile(self):
        return self.getResourceFile(package=C.RESOURCE_PKGFOLDER_TRANSFORMERS, 
                                    file=CFGFILES_DSOBJECT)

    def __init__(self, config, log):
        super().__init__(config, log)
        self.template = C.EMPTY
        self.columnName = C.EMPTY
        self.constants = {}

    def initialize(self, params) -> bool:
        """ Initialize and makes some checks (params) for that transformer
        Args:
            params (json): parameters
        Returns:
            bool: False if error
        """
        try:
            self.template = params.getParameter(PARAM_TEMPLATE)
            self.columnName = params.getParameter(PARAM_COLUMN_TARGET)
            self.constants = params.getParameter(PARAM_CONSTANTS)
            if (len(self.dsOutputs) != 1 and len(self.dsInputs) != 1):
                raise Exception("This transformer must have only one input dataset and one output dataset")
            return True
        except Exception as e:
            self.log.error("{}".format(str(e)))
            return False

    def process(self, dsTransformerInputs) -> plDatasets:
        """ this transformer uses jinja templates to update a column in a dataset
        Args:
            inputDataFrames (etlDatasets): multiple dataset in a collection
            outputNames (array): output names list
        Returns:
            etlDatasets: same as input
        """
        try :
            jinjaTemplate = Template(self.template)
            outputs = plDatasets()
            out = dsTransformerInputs[0].copy()
            out.columnTransform(self.columnName, 
                                lambda r: jinjaTemplate.render(r.to_dict() | self.constants))
            out.id = self.dsOutputs[0]
            outputs.add(out)
            return outputs
        
        except Exception as e:
            self.log.error("{}".format(str(e)))
            return dsTransformerInputs