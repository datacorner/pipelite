__author__ = "ExyPro Community"
__email__ = "admin@exypro.org"
__license__ = "MIT"

import utils.constants as C
from pipelines.pipeline import pipeline
import pandas as pd

class directPipeline(pipeline):
    def __init__(self, config, log):
        super().__init__(config, log)