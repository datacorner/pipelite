__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import logging
from logging.handlers import RotatingFileHandler
import pipelite.constants as C
import inspect
import datetime

class log:

    def __init__(self, 
                 loggerName, 
                 logfilename, 
                 level, 
                 format):
        self.__logger = logging.getLogger(loggerName)
        logHandler = RotatingFileHandler(logfilename, 
                                         mode="a", 
                                         maxBytes= C.TRACE_DEFAULT_MAXBYTES, 
                                         backupCount=1, 
                                         encoding=C.ENCODING)
        logHandler.setFormatter(logging.Formatter(format))
        if (level == C.LOG_LEVEL_INFO):
            loglevel = logging.INFO
        elif (level == C.LOG_LEVEL_DEBUG):
            loglevel = logging.DEBUG
        elif (level == C.LOG_LEVEL_WARNING):
            loglevel = logging.WARNING
        else:
            loglevel = logging.ERROR
        self.__logger.setLevel(loglevel)
        self.__logger.addHandler(logHandler)
        self.__warnings = []
        self.__errors = []

    @property
    def warnings(self) -> list:
        return self.__warnings
    @property
    def errors(self) -> list:
        return self.__errors
    @property
    def warningCounts(self) -> int:
        return len(self.__warnings)
    @property
    def errorCounts(self) -> int:
        return len(self.__errors)
    
    def consoleOutput(self, message):
        """Just display the message in the console output
        Args:
            message (str): message
        """
        print(str(datetime.datetime.now()) + " " + message)
    
    def buildMessage(self, callerInfo, msg): 
        """Build the message to log
        Args:
            callerInfo (str): caller information (package + method)
            msg (str): message type
        Returns:
            str: global message to log
        """
        final_message = callerInfo + "|" if (callerInfo != C.EMPTY and self.__logger.level == logging.DEBUG)  else ""
        for msgItem in msg:
            final_message += str(msgItem)
        return final_message
    
    def getCallerInfo(self) -> str:
        """ get the class caller by looking at the stack call
        Returns:
            str: caller class & method 
        """
        try:
            # Get the package and method name first (non static calls)
            prev_frame = (inspect.currentframe().f_back).f_back
            the_class = prev_frame.f_locals["self"].__class__
            the_method = prev_frame.f_code.co_name
            return the_class.__module__ + "." + the_method + "()"
        except Exception as e:
            try:
                # if the exception comes from a Static call, we gather the package in another way
                return prev_frame.f_globals["__name__"]
            except:
                return "..."

    def info(self, *message):
        final_message = self.buildMessage(self.getCallerInfo(), message)
        self.consoleOutput("INFO|" + final_message)
        self.__logger.info(final_message)

    def error(self, *message):
        final_message = self.buildMessage(self.getCallerInfo(), message)
        self.consoleOutput("ERROR|" + final_message)
        self.__logger.error(final_message)
        self.__errors.append(final_message)

    def debug(self, *message):
        final_message = self.buildMessage(self.getCallerInfo(), message)
        self.consoleOutput("DEBUG|" + final_message)
        self.__logger.debug(final_message)

    def warning(self, *message):
        final_message = self.buildMessage(self.getCallerInfo(), message)
        self.consoleOutput("WARNING|" + final_message)
        self.__logger.warning(final_message)
        self.__warnings.append(final_message)