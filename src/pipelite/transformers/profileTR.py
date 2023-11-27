__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.baseobjs.BOTransformer import BOTransformer
from pipelite.plDatasets import plDatasets
import json
import os
import pathlib
from jinja2 import Template
import importlib.resources

CFGFILES_DSOBJECT = "profileTR.json"
PARAM_PROFILEFILE = "filename"
PARAM_PROFILEDIR = "directory"
PARAM_MAXVALUECOUNTS = "maxvaluecounts"
PARAM_OUTPUT = "output"
DEFAULT_PROFILEFILE = "pipelite.profile"
OUTPUT_JSON = "json"
OUTPUT_HTML = "html"
DEFAULT_OUTPUT = OUTPUT_JSON
DEFAULT_PROFILE_FILENAME = "profile-index.html"
OUTPUT_ACCEPTED = [ OUTPUT_JSON, OUTPUT_HTML ]

class profileTR(BOTransformer):

    @property
    def parametersValidationFile(self):
        return self.getResourceFile(package=C.RESOURCE_PKGFOLDER_TRANSFORMERS, 
                                    file=CFGFILES_DSOBJECT)

    def __init__(self, config, log):
        super().__init__(config, log)
        self.profileFile = C.EMPTY

    def initialize(self, params) -> bool:
        """ Initialize and makes some checks (params) for that transformer
        Args:
            params (json): parameters
        Returns:
            bool: False if error
        """
        try:
            self.profileFile = params.getParameter(PARAM_PROFILEFILE, DEFAULT_PROFILE_FILENAME)
            self.maxvaluecounts = params.getParameter(PARAM_MAXVALUECOUNTS)
            self.output = params.getParameter(PARAM_OUTPUT, DEFAULT_OUTPUT)
            self.directory = params.getParameter(PARAM_PROFILEDIR, C.EMPTY)
            if (not self.output in OUTPUT_ACCEPTED):
                self.log.error("The output specified {} is not aauthorized".format(self.output))
                return False
            if (self.profileFile == C.EMPTY and self.output == OUTPUT_JSON):
                self.profileFile = DEFAULT_PROFILEFILE
            return True
        except Exception as e:
            self.log.error("{}".format(str(e)))
            return False

    def process(self, dsTransformerInputs) -> plDatasets:
        """ this transformer profiles the dataset in inputs and provides several profile datasets in output
        Args:
            inputDataFrames (etlDatasets): multiple dataset in a collection
            outputNames (array): output names list
        Returns:
            etlDatasets: same as input
        """
        try :
            self.log.info("Profile the Data Sources ...")
            globalprofile = []
            for index, input in enumerate(dsTransformerInputs):
                self.log.info("Profiling Data Source {} ...".format(input.id))
                subprofile = {}
                profile = input.profile(maxvaluecounts=self.maxvaluecounts)
                subprofile['id'] = input.id
                subprofile['profile'] = profile
                globalprofile.append(subprofile) 
            finalProfile = {}
            finalProfile["sources"] = globalprofile
            # Create the Directory if not exist
            if (not os.path.isdir(self.directory)):
                self.log.info("Create the directory {}".format(self.directory))
                os.makedirs(self.directory)
            fullFilename = os.path.join(self.directory, self.profileFile)
            if (self.output == OUTPUT_JSON):
                # Export the result in a JSON file
                self.log.info("Export the profile as a JSON file ...")
                with open(fullFilename, "w") as outfile: 
                    json.dump(finalProfile, outfile)
                self.log.info("The JSON data profile has been successfully generated in the file {}".format(self.profileFile))
            elif (self.output == OUTPUT_HTML):
                self.log.info("Export the profile as a HTML file ...")
                # Create the home.html page with the profiling basics infos
                templateFile = str(importlib.resources.files(C.RESOURCE_TEMPLATES_ROOT).joinpath(C.RESOURCE_TEMPLATE_PROFILE))
                templateContent = pathlib.Path(templateFile).read_text()
                j2_template = Template(templateContent)
                htmlContent = j2_template.render(profiles=finalProfile, 
                                                 sample=input.head(10)) # profiles = Jinja2 variable inside the template file
                with open(fullFilename, "w") as fileIndex:
                    fileIndex.write(htmlContent)
                    self.log.info("The HTML data profile {} has been successfully generated in the directory {}".format(fullFilename, self.directory))

            return plDatasets()
        except Exception as e:
            self.log.error("{}".format(str(e)))
            return plDatasets()