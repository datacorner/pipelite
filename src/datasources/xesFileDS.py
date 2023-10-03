__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import xmltodict
from pipelite.interfaces.IDataSource import IDataSource 
import pipelite.constants as C
import os
from json import dumps, loads
from pipelite.etlDataset import etlDataset

# Highly inspired by https://github.com/FrankBGao/read_xes/tree/master ;-)
DATATYPES = ['string',  'int', 'date', 'float', 'boolean', 'id']
CASE_KEY = 'concept-name-attr'

# json validation Configuration 
CFGFILES_DSOBJECT = "xesFileDS.json"
CFGPARAMS_PATH = "path"
CFGPARAMS_FILENAME = "filename"

class xesFileDS(IDataSource):

    def __init__(self, config, log):
        super().__init__(config, log)
        self.filename = C.EMPTY

    @property
    def parametersValidationFile(self):
        return self.getResourceFile(package=C.RESOURCE_PKGFOLDER_DATASOURCES, 
                                    file=CFGFILES_DSOBJECT)
    
    def initialize(self, cfg) -> bool:
        """ initialize and check all the needed configuration parameters
            A CSV Extractor/Loader must have:
                * A filename
                * A path name
                params['filename']
        Args:
            params (json list) : params for the data source.
                example: {'separator': ',', 'filename': 'test2.csv', 'path': '/tests/data/', 'encoding': 'utf-8'}
        Returns:
            bool: False if error
        """
        try:
            self.filename = os.path.join(cfg.getParameter(CFGPARAMS_PATH, C.EMPTY), 
                                         cfg.getParameter(CFGPARAMS_FILENAME, C.EMPTY))
            # Checks ...
            if (self.ojbType == C.PLJSONCFG_LOADER):
                if (not os.path.isfile(self.filename)):
                    raise Exception("The XES file {} does not exist or is not accessible.".format(self.filename))
            return True
        except Exception as e:
            self.log.error("{}".format(e))
            return False
    
    def extract(self) -> etlDataset:
        """ flaten the XES (XML format) and returns all the data in a etlDataset format
        Returns:
            etlDataset: data set
        """
        try:
            dsExtract = etlDataset()
            # Get the XES content (XML format)
            xmldata = open(self.filename, mode='r').read()
            # Extract/flatten XES data
            events, attributes = self.__extractAll(xmldata)
            dsExtract.initFromList(events)
    
            return dsExtract
        except Exception as e:
            self.log.error("{}".format(e))
            return etlDataset()
         
    def __getEventDetails(self, event, id):
        """ returns all columns for one event (in a list)
        Args:
            event (_type_): event details
            id (_type_): trace id
        Returns:
            list: events details
        """
        one_event_attri = list(event.keys())
        one_event_dict = {}
        for i in DATATYPES:
            if i in one_event_attri:
                if type(event[i]) == list:
                    for j in event[i]:
                        one_event_dict[j['@key']] = j['@value']
                else:
                    one_event_dict[event[i]['@key']] = event[i]['@value']
        one_event_dict[CASE_KEY] = id
        return one_event_dict

    def __ExtractOneTrace(self, trace_item):
        """ extract logs and attributes from 1 trace
        Args:
            trace_item (_type_): 1 trace (contains attrs + several logs)
        Returns:
            dict: trace attributes
            list: events
        """

        # Build atributes / trace
        attrs = list(trace_item.keys())
        attrs_dict = {}
        for i in DATATYPES:
            if i in attrs:
                if type(trace_item[i]) == list:
                    for j in trace_item[i]:
                        attrs_dict[j['@key']] = j['@value']
                else:
                    attrs_dict[trace_item[i]['@key']] = trace_item[i]['@value']
        # build events / trace
        events = []
        if type(trace_item['event']) == dict:
            trace_item['event'] = [trace_item['event']]

        for i in trace_item['event']:
            inter_event = self.__getEventDetails(i, attrs_dict['concept:name'])
            events.append(inter_event)
        return attrs_dict, events

    def __extractAll(self, xml):
        """ This functions reads the XES file and extract all the events and attributes
        Args:
            xml (str): XML flow (XES)
        Returns:
            list: event list
            list: attributes
        """
        traces = loads(dumps(xmltodict.parse(xml)))['log']['trace']
        self.log.debug("{} traces to manage".format(len(traces)))
        attributes_list = []
        event_list = []
        # reads the traces tags one by one and get all the events & attrs
        traceIdx = 1
        for trace in traces:
            trace_item = self.__ExtractOneTrace(trace)
            attributes_list.append(trace_item[0]) # Attributes
            event_list = event_list + trace_item[1] # Event details
            self.log.debug("{}/{} -> {} evts".format(traceIdx, trace_item[0]['concept:name'], len(trace_item[1])))
            traceIdx += 1
        return event_list, attributes_list
    
    