__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
import datetime

class etlReport:

    def __init__(self):
        self.__timestampStart = None
        self.__timestampEnd = None
        self.name = C.EMPTY
        self.description = C.EMPTY
        self.type = C.EMPTY # E, T or L
        self.__processedRowsCount = 0
        self.status = "NOT STARTED"
        pass

    def start(self):
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
    def endTime(self) -> datetime:
        return self.__timestampEnd

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
    


    
