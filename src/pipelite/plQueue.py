__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"


class plQueue():
    def __init__(self, config, log):
        self.__config = config
        self.__log = log
        self.objects = [] # array of dpObject


    