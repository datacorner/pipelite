__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import logging
from logging.handlers import RotatingFileHandler
import pipelite.utils.constants as C
import inspect

class log:

    def __init__(self, loggerName, logfilename, level, format):
        self.__logger = logging.getLogger(loggerName)
        logHandler = RotatingFileHandler(logfilename, 
                                            mode="a", 
                                            maxBytes= C.TRACE_DEFAULT_MAXBYTES, 
                                            backupCount=1 , 
                                            encoding=C.ENCODING)
        logHandler.setFormatter(logging.Formatter(format))
        if (level == "INFO"):
            loglevel = logging.INFO
        elif (level == "DEBUG"):
            loglevel = logging.DEBUG
        elif (level == "WARNING"):
            loglevel = logging.WARNING
        else:
            loglevel = logging.ERROR
        self.__logger.setLevel(loglevel)
        self.__logger.addHandler(logHandler)

    def display(self, message):
        print(message)
    
    def buildMessage(self, callerInfo, msg):
        final_message = "{" +callerInfo + "} "
        for msgItem in msg:
            final_message += str(msgItem)
        return final_message
    
    def getCallerInfo(self) -> str:
        """ get the class caller by looking at the stack call
        Returns:
            str: caller class & method 
        """
        try:
            prev_frame = (inspect.currentframe().f_back).f_back
            the_class = prev_frame.f_locals["self"].__class__
            the_method = prev_frame.f_code.co_name
            return the_class.__module__ + "." + the_method + "()"
        except Exception as e:
            return "N.A."

    def info(self, *message):
        final_message = self.buildMessage(self.getCallerInfo(), message)
        self.display("Info|" + final_message)
        self.__logger.info(final_message)

    def error(self, *message):
        final_message = self.buildMessage(self.getCallerInfo(), message)
        self.display("ERROR|" + final_message)
        self.__logger.error(final_message)

    def debug(self, *message):
        final_message = self.buildMessage(self.getCallerInfo(), message)
        self.display("DEBUG|" + final_message)
        self.__logger.debug(final_message)

    def warning(self, *message):
        final_message = self.buildMessage(self.getCallerInfo(), message)
        self.display("WARNING|" + final_message)
        self.__logger.warning(final_message)