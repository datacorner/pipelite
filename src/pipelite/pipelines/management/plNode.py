__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

class plNode():
    def __init__(self):
        self.inputs = None
        self.outputs = None
        self.id = None
        self.objtype = None
        self.ready = False

    def __repr__(self):
        return self.id
    