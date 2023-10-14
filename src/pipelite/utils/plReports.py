__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from .plReport import plReport
import pandas as pd
import json

class plReports:

    def __init__(self):
        self.reports = []   # Array of etlReport
        self.orderIndex = 1      # call ordering

    @property
    def count(self) -> int:
        """Returns the Number of etlDatasets inside the collection
        Returns:
            int: dataset count
        """
        return len(self.reports)
    
    def addEntry(self, id, type):
        report = plReport()
        report.id = id
        report.type = type
        self.reports.append(report)

    def globalDuration(self):
        return sum([item.duration for item in self.reports])

    def __getFullDataFrameReport(self) -> pd.DataFrame:
        dfRep = pd.DataFrame() 
        for rep in self.reports:
            entry = {"id" : rep.id, 
                    "Type" : rep.type, 
                    "Description" : rep.description,
                    "Start" : rep.startTimeFMT,
                    "End" : rep.endTimeFMT,
                    "Duration" : str(rep.duration),
                    "Rows Processed" : str(rep.processedRows),
                    "Order" : str(rep.order),
                    "id" : rep.id}
            dfEntry = pd.DataFrame([entry])
            dfEntry = dfEntry.set_index("id")
            if (not dfRep.empty):
                dfRep = pd.concat([dfRep, dfEntry])
            else:
                dfRep = dfEntry
            dfRep = dfRep.sort_values(by = 'Order')
        return dfRep
    
    def getFullJSONReport(self) -> json:
        return json.loads(self.__getFullDataFrameReport().to_json(orient="columns"))

    def getFullSTRReport(self) -> str:
        finalReport = "--- PIPELITE REPORT ---\n"
        finalReport += str(self.__getFullDataFrameReport())
        finalReport += "\n\nTotal Duration {} sec\n".format(self.globalDuration())
        return finalReport
    
    @property
    def names(self):
        """ Returns a list with all etlDatasets names like [ "E1", ...., "En"]
        Returns:
            list: names list
        """
        return [ ds.name for ds in self.reports ]
    
    def getFromId(self, id) -> plReport:
        """ Returns the rerport by searching it by id
        Args:
            name (str): dataset name/id
        Returns:
            etlReport: rerport
        """
        for rep in self.reports:
            if (rep.id == id):
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