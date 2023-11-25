__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

from pipelite.plObject import plObject

class plQueue(plObject):
    def __init__(self, config, log):
        super().__init__()
        self.objects = [] # array of dpObject

    