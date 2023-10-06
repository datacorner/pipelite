__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from .etlReport import etlReport
import pandas as pd
import json

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

    def __getFullDataFrameReport(self) -> pd.DataFrame:
        dfRep = pd.DataFrame() #columns=["Name", "Type", "Start", "End", "Duration", "Rows Processed", "id"])
        for rep in self.reports:
            entry = {"Name" : rep.name, 
                    "Type" : rep.type, 
                    "Start" : rep.startTimeFMT,
                    "End" : rep.endTimeFMT,
                    "Duration" : str(rep.duration),
                    "Rows Processed" : str(rep.processedRows),
                    "id" : rep.id}
            dfEntry = pd.DataFrame([entry])
            dfEntry = dfEntry.set_index("id")
            if (not dfRep.empty):
                dfRep = pd.concat([dfRep, dfEntry])
            else:
                dfRep = dfEntry
        return dfRep
    
    def getFullJSONReport(self) -> json:
        return json.loads(self.__getFullDataFrameReport().to_json(orient="columns"))

    def getFullSTRReport(self) -> str:
        return str(self.__getFullDataFrameReport())
    
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