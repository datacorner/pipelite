__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from .etlReport import etlReport

class etlReports:

    def __init__(self):
        self.reports = [] # Array of etlReport

    @property
    def count(self) -> int:
        """Returns the Number of etlDatasets inside the collection
        Returns:
            int: dataset count
        """
        return len(self.reports)
    
    def addEntry(self, name, type):
        report = etlReport()
        report.name = name
        report.type = type
        self.reports.append(report)

    def getFullReport(self):
        # Content
        report =  C.SEPRPT + "Name" + C.TABRPT
        report += C.SEPRPT + "Type" + C.TABRPT
        report += C.SEPRPT + "Start" + C.TABRPT
        report += C.SEPRPT + "End" + C.TABRPT
        report += C.SEPRPT + "Duration" + C.TABRPT
        report += C.SEPRPT + "Rows" + C.TABRPT
        report +=  C.SEPRPT
        report += "\n"
        # Content
        for rep in self.reports:
            report +=  C.SEPRPT + rep.name + C.TABRPT
            report += C.SEPRPT + rep.type + C.TABRPT
            if (rep.startTime != None):
                report += C.SEPRPT + rep.startTime.strftime(C.DATE_FORMAT) + C.TABRPT
            else:
                report += C.SEPRPT + "N.A." + C.TABRPT
            if (rep.endTime != None):
                report += C.SEPRPT + rep.endTime.strftime(C.DATE_FORMAT) + C.TABRPT
            else:
                report += C.SEPRPT + "N.A." + C.TABRPT
            report += C.SEPRPT + str(rep.duration) + "s" + C.TABRPT
            report += C.SEPRPT + str(rep.processedRows) + C.TABRPT
            report += "\n"
        return report

    @property
    def names(self):
        """ Returns a list with all etlDatasets names like [ "E1", ...., "En"]
        Returns:
            list: names list
        """
        return [ ds.name for ds in self.reports ]
    
    def getFromName(self, name) -> etlReport:
        """ Returns the rerport by searching it by id
        Args:
            name (str): dataset name/id
        Returns:
            etlReport: rerport
        """
        for rep in self.reports:
            if (rep.name == name):
                return rep
        return None
    
    def __getitem__(self, item):
        """ Makes the Data column accessible via [] array
            example: df['colName']
        Args:
            item (str): attribute/column name
        Returns:
            object: data
        """
        return self.reports.__getitem__(item)