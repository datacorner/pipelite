__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pandas as pd
from collections import Counter
import re
import datetime
import math
from dateutil.parser import parse

REGEX_CARS = r'[a-zA-Z]'
REGEX_NUMS = r'\d'
REGEX_NOISE = r'[µ&#@^~]'
REGEX_SPECCARS = r'[<>µ+=*&\'"#{}()|/\\@][]^~]'
REGEX_PONCT = r'[,.;:?!]'

class datasetProfiler:
    """ This class manages a Data profiling
    """
    def __init__(self, df, log):
        self.log = log
        self.__dfFullContent = df

    def getStringPattern(self, string):
        """Returns the data pattern by replacing strings by S, digits by N and noises (special characters) by ?
        Args:
            string (str): data in input
        Returns:
            str: pattern
        """
        if (string == 'nan'):
            return "NULL"
        mystr = re.sub(REGEX_CARS, 'C', string)
        mystr = re.sub(REGEX_NUMS, 'N', mystr)
        mystr = re.sub(REGEX_NOISE, '?', mystr)
        #mystr = re.sub(REGEX_PONCT, 'P', mystr)
        return mystr

    def getType(self, value):
        """Returns the data type on the value in input
        Args:
            value (object): data
        Returns:
            str: type name can be [ null, number, date, string, unknown]
        """
        try: 
            if value is None:
                return "null"
            if isinstance(value, (int, float)):
                if math.isnan(value):
                    return "null"
                else:
                    return "number"
            elif isinstance(value, str):
                try:
                    parse(value, fuzzy=False)
                    return "date"
                except ValueError:
                    return "string"
            elif isinstance(value, datetime.datetime):
                return "date"
            else:
                return "unknown"
        except:
            return "unknown"

    def splitDataset(self, dataframe, chunk_size=2):
        chunks = []
        num_chunks = len(dataframe) // chunk_size + 1
        for index in range(num_chunks):
            chunks.append(dataframe[index * chunk_size:(index+1) * chunk_size])
        return chunks

    def run(self, datasourceid, maxvaluecounts=10):
        return self.__dataProfile__(self.__dfFullContent, datasourceid, maxvaluecounts)
        #chunk_size = 100
        #num_chunks = len(self.__dfFullContent) // chunk_size + 1
        #for index in range(num_chunks):
        #    prof = self.__dataProfile__(self.__dfFullContent[index * chunk_size:(index+1) * chunk_size], maxvaluecounts)
        #return prof
    
    def __dataProfile__(self, df, datasourceid, maxvaluecounts=10) -> dict:
        """Build a JSON which contains some basic profiling informations
        Args:
            df( pd.DataFrame): Pandas Dataframe to profile
            maxvaluecounts (int, optional): Limits the number of value_counts() return. Defaults to 10.
        Returns:
            json: data profile in a JSON format
        """
        profile = {}
        # Get stats per columns/fields
        self.log.info("Profile Data Source {} ...".format(datasourceid))
        profileColumns = []
        for idx, col in enumerate(df.columns):
            self.log.debug("Profile Column {} ...".format(col))
            profileCol = {}
            counts = df[col].value_counts()
            profileCol['id'] = str(idx)
            profileCol['name'] = col
            profileCol['type'] = str(df[col].dtypes)
            profileCol['inferred'] = str(df[col].infer_objects().dtypes)
            profileCol['distinct'] = int(len(counts))
            profileCol['nan'] = int(df[col].isna().sum())
            profileCol['null'] = int(df[col].isnull().sum())
            profileCol['stats'] = df[col].describe().to_dict()
            profileCol['top values'] = dict(Counter(counts.to_dict()).most_common(maxvaluecounts))
            # Get pattern for that column
            dfProfPattern = pd.DataFrame()
            dfProfPattern['profile'] = df[col].apply(lambda x:self.getStringPattern(str(x)))
            profileCol['pattern'] = dict(Counter(dfProfPattern['profile'].value_counts().to_dict()).most_common(maxvaluecounts))
            # get types for that columns
            dfProfType = pd.DataFrame()
            dfProfType['types'] = df[col].apply(lambda x:self.getType(x))
            profileCol['types'] = dict(Counter(dfProfType['types'].value_counts().to_dict()).most_common(maxvaluecounts))
            profileColumns.append(profileCol)
        profile["rows count"] = df.shape[0]
        profile["columns count"] = df.shape[1]
        profile["columns names"] = [ name for name in df.columns ]
        profile["columns"] = profileColumns
        return profile
