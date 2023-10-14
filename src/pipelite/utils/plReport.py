__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
import datetime

class plReport:

    def __init__(self):
        self.__timestampStart = None
        self.__timestampEnd = None
        self.id = C.EMPTY
        self.description = C.EMPTY
        self.__type = C.EMPTY
        self.__processedRowsCount = 0
        self.status = "NOT STARTED"
        self.order = 0

    @property
    def type(self):
        return self.__type
    @type.setter
    def type(self, value):
        if (value == C.PLJSONCFG_EXTRACTOR):
            self.__type = "Extractor"
        elif (value == C.PLJSONCFG_LOADER):
            self.__type = "Loader"
        elif (value == C.PLJSONCFG_TRANSFORMER):
            self.__type = "Transformer"
        else:
            self.__type = "Other"

    def start(self, order=0, description=C.EMPTY):
        self.order = order
        self.description = description
        if (self.__timestampStart == None):
            self.__timestampStart = datetime.datetime.now()

    def end(self, rowProcessed=0):
        if (self.__timestampStart != None):
            self.__timestampEnd = datetime.datetime.now()
            self.__processedRowsCount = rowProcessed

    @property
    def startTime(self) -> datetime:
        return self.__timestampStart
    @property
    def startTimeFMT(self) -> datetime:
        try:
            return self.__timestampStart.strftime(C.DATE_FORMAT)
        except:
            return C.EMPTY
        
    @property
    def endTime(self) -> datetime:
        return self.__timestampEnd
    @property
    def endTimeFMT(self) -> datetime:
        try:
            return self.__timestampEnd.strftime(C.DATE_FORMAT)
        except:
            return C.EMPTY
    
    @property
    def processedRows(self) -> int:
        return self.__processedRowsCount

    @property
    def duration(self) -> int:
        try:
            dur = self.__timestampEnd - self.__timestampStart
            return dur.total_seconds()
        except:
            return 0
    
    @property
    def isStarted(self) -> bool:
        return self.__timestampStart != None
    
    @property
    def isFinished(self) -> bool:
        return self.__timestampEnd != None
    


    
