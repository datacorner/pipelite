__author__ = "Benoit CAYLA"
__email__ = "benoit@datacorner.fr"
__license__ = "MIT"

import requests
import json
from pipelite.datasources.abbyypi.repConfig import repConfig
from pipelite.datasources.abbyypi.uploadConfig import uploadConfig
from urllib import request
import pipelite.constants as C

class piApiRepositoryWrapper:
    """This class acts as a gateway for the ABBYY Timeline API calls
    """
    def __init__(self, token, serverURL):
        self.__token = token
        self.__serverURL = serverURL
        self.__log = None

    @property
    def log(self):
        return self.__log
    @log.setter   
    def log(self, value):
        self.__log = value

    @property
    def apiRootPath(self):
        return self.__serverURL + C.API_1_0
    @property
    def URL(self):
        return self.__serverURL
    @property
    def Token(self):
        return self.__token

    def getRepositoryConfiguration(self) -> repConfig:
        """ HTTP GET / Gather the repository details & config from the server
        Returns:
            repConfig: ABBYY Timeline repository config
        """
        try: 
            # Get Api call for getting Repository informations
            self.log.info("Get Api call for getting Repository informations ...")
            url = self.apiRootPath + C.API_REPOSITORY_CONFIG
            self.log.debug("HTTP GET Request sent: " + url)
            headers = {}
            headers["Authorization"] = "Bearer " + self.Token
            headers["content-type"] = "application/json"
            httpResponse = requests.get(url , headers=headers) 
            repositoryCfg = repConfig(httpResponse) # content in repositoryCfg.jsonContent
            if (repositoryCfg.loaded):
                self.log.info("Informations from Abbyy Timeline Repository collected successfully")
            else:
                raise Exception ("Impossible to collect repository informations.")
            return repositoryCfg
        
        except Exception as e:
            self.log.error(e)
            return repConfig()

    def prepareUpload(self, repositoryId) -> uploadConfig:
        """ HTTP POST Call / get the Server info for upload / timeline.getUploadData
        Args:
            repositoryId (_type_): ABBYY Timeline Repository ID
        Returns:
            uploadConfig: Upload details configuration
        """
        try: 
            self.log.info("Get the Server info for upload ...")
            url = self.apiRootPath + C.API_SERVER_UPLOAD_INFOS.format(repositoryId)
            self.log.debug("AHTTP POST Request " + url)
            jsondata = json.dumps({"fileName": "timeline.csv"}).encode("utf8")
            self.log.debug("HTTP POST Data sent: ", jsondata)
            req = request.Request(url)
            req.add_header('Content-Type', 'application/json; charset=utf-8')
            req.add_header('Authorization', 'Bearer ' + self.Token)
            httpResponse = request.urlopen(req, jsondata).read().decode("utf8")
            cfg = uploadConfig(httpResponse) # see cfg.jsonContent
            if (cfg.loaded):
                self.log.info("Upload prepared successfully")
            else:
                raise Exception ("Impossible to prepare the upload")
            return cfg
        
        except Exception as e:
            self.log.error(e)
            return uploadConfig()

    def uploadData(self, csvData, url, headersAcl) -> bool:
        """HTTP PUT Call / Upload data (csv format) to the server
        Args:
            csvData (_type_): Data (CSV format)
            url (_type_): ABBYY Timeline URL (upload destination <- uploadConfig)
            headersAcl (_type_): Header ACL (<- uploadConfig)
        Returns:
            bool: _description_
        """
        try:
            self.log.info("ABBYY Timeline API - Upload CSV formatted data to the ABBYY Timeline Server")
            headers = {}
            headers["Authorization"] = "Bearer " + self.Token
            headers["content-type"] = "text/csv"
            headers.update(headersAcl)
            self.log.debug("ABBYY Timeline API - HTTP PUT Request " + url)
            response = requests.put(url , data=csvData, headers=headers)
            self.log.debug("ABBYY Timeline API - HTTP Response {}".format(str(response)))
            return response.ok
        except Exception as e:
            self.log.error(e)
            return False

    def loadFileToPIRepository(self, repositoryId, fkeys, repositoryTable) -> str:
        """ HTTP POST Call / Upload the file in ABBYY Timeline repo / timeline.loadFileIntoRepositoryTable
        Args:
            repositoryId (_type_): ABBYY Timeline Repository ID
            fkeys (_type_): Keys
            repositoryTable (_type_): Table to create/append in the Repository
        Returns:
            str: ID of the Process execution
        """
        try:
            self.log.info("Load the file to the ABBYY Timeline repository")
            url = self.apiRootPath + C.API_SERVER_LOAD_2_REPO.format(repositoryId)
            self.log.debug("HTTP POST Request " + url)
            req = request.Request(url)
            req.add_header('Content-Type', 'application/json; charset=utf-8')
            req.add_header('Authorization', 'Bearer ' + self.Token)
            js = {}
            js["fileKeys"] = json.loads(fkeys)
            js["tableName"] = repositoryTable
            jsondata = json.dumps(js).encode("utf8")
            self.log.debug("HTTP POST Data sent: ", jsondata)
            httpResponse = request.urlopen(req, jsondata).read()
            jres2 = json.loads(httpResponse.decode("utf8"))
            self.log.debug("HTTP Response {}".format(jres2))
            self.log.info("Loading the file with process ID {} ".format(jres2["processingId"]))
            return jres2["processingId"]
        
        except Exception as e:
            self.log.error(e)
            return str(-1)

    def getProcessingStatus(self, processID) -> str:
        """ HTTP Returns the processing Status 
        Args:
            processID (_type_): Process ID
        Raises:
            Exception: Exception / Error with HTTP dump
        Returns:
            str: Status
        """
        try:
            self.log.info("Check status for the ABBYY Timeline Task {}".format(processID))
            url = self.apiRootPath + C.API_PROCESSING_STATUS + "/" + processID
            self.log.debug("HTTP GET Request " + url)
            response = requests.get(url, headers={ 'Authorization': 'Bearer ' + self.Token, 'content-type': 'application/json' })
            jres = json.loads(response.content)
            self.log.debug("HTTP Response {}".format(response.content))
            self.log.info("ABBYY Timeline Task {} status is {} ".format(processID, jres["status"]))
            if (jres["status"] == C.API_STATUS_ERROR):
                raise Exception(json.dumps(jres))
            return jres["status"]
        
        except Exception as e:
            self.log.error(e)
            return C.API_STATUS_ERROR

    def executeTODO(self, repositoryId, todo, tableName) -> str:
        """ HTTPPOST Call / get the Server info for upload / timeline.getUploadData
        Args:
            repositoryId (_type_): Repository ID
            todo (_type_): TO DO Name
            tableName (_type_): Table name
        Returns:
            str: Process ID
        """
        try: 
            self.log.info("Execute a To Do in ABBYY Timeline repository")
            url = self.apiRootPath + C.API_EXECUTE_TODO.format(repositoryId)
            self.log.debug("HTTP POST Request " + url)
            jsondata = json.dumps({"todoListNames": todo, "tableName" : tableName}).encode("utf8")
            self.log.debug("HTTP POST Data sent: ", jsondata)
            req = request.Request(url)
            req.add_header('Content-Type', 'application/json; charset=utf-8')
            req.add_header('Authorization', 'Bearer ' + self.Token)
            httpResponse = request.urlopen(req, jsondata).read()
            jres2 = json.loads(httpResponse.decode("utf8"))
            self.log.debug("HTTP Response {}".format(jres2))
            self.log.info("Loading the file with process ID: " + jres2["processingId"])
            return jres2["processingId"]
        except Exception as e:
            self.log.error(e)
            return str(-1)