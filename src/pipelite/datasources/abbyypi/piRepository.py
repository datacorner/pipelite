__author__ = "Benoit CAYLA"
__email__ = "benoit@datacorner.fr"
__license__ = "MIT"

from pipelite.datasources.abbyypi.piApiRepositoryWrapper import piApiRepositoryWrapper
import pipelite.constants as C
from pipelite.datasources.abbyypi.repConfig import repConfig
import json
import time

class piRepository():
    def __init__(self, log):
        self.__repositoryInfos = None   # ABBYY Timeline Repository infos (gathered from the ABBYY Timeline server)
        self.log = log
        self.server = ""
        self.token = ""

    @property
    def repositoryConfig(self) -> repConfig:
        return self.__repositoryInfos
    
    def initialize(self, server, token) -> bool:
        """Initialize the Class instance by gathering the ABBYY Timeline repository infos.
            * initialize the logger
            * check the mandatory parameters
            * init the API (get the ABBYY Timeline Repository infos)
        Returns:
            bool: False if error
        """
        try:
            self.server = server
            self.token = token
            # Get the repository configuration infos
            api = piApiRepositoryWrapper(self.token, self.server)
            api.log = self.log
            self.__repositoryInfos = api.getRepositoryConfiguration()
            return True
        except Exception as e:
            self.log.error("initialize() Error -> " + str(e))
            return False

    def getStatus(self, processingId) -> str:
        """Return the status of a process launched on the ABBYY Timeline server
        Args:
            processingId (_type_): ID of the ABBYY Timeline Process
        Returns:
            str: Process status (from ABBYY Timeline server)
        """
        try:
            api = piApiRepositoryWrapper(self.token, self.server)
            api.log = self.log
            return api.getProcessingStatus(processingId)
        except Exception as e:
            self.log.error("getStatus() Error -> " + str(e))
            return C.API_STATUS_ERROR

    def waitForEndOfProcessing(self, processId) -> str:
        """Wait for the end of the ABBYY Timeline process execution
        Args:
            processId (_type_): ID of the ABBYY Timeline Process
        Returns:
            str: Final Status
        """
        try:
            self.log.info("Wait for the end of a process execution")
            EndOfWait = True
            nbIterations = 0
            api = piApiRepositoryWrapper(self.token, self.server)
            api.log = self.log
            while (EndOfWait):
                # 5 - Check the status to veriify if the task is finished
                status = self.getStatus(processId)
                if ((status != C.API_STATUS_IN_PROGRESS) or (nbIterations > C.API_DEF_NB_ITERATION_MAX)):
                    EndOfWait = False
                time.sleep(C.API_DEF_WAIT_DURATION_SEC)
                nbIterations += 1
            return status
        except Exception as e:
            self.log.error("waitForEndOfProcessing() Error -> " + str(e))
            return C.API_STATUS_ERROR

    def executeToDo(self, todos, table) -> bool:
        """Execute a ABBYY Timeline TO DO (be careful as this TO DO must exists)
        Returns:
            bool: False if error or the TO DO does not exists
        """
        try:
            api = piApiRepositoryWrapper(self.token, self.server)
            api.log = self.log
            self.log.info("Execute these TO DO: {}".format(",".join(todos)))
            if (self.repositoryConfig.loaded):
                if (len(todos) > 0):
                    processId = api.executeTODO(self.repositoryConfig.repositoryId, 
                                                todos, 
                                                table)
                    self.waitForEndOfProcessing(processId)
                    self.log.info("To Do executed successfully")
                    return True
                else:
                    self.log.info("No configured To Do to execute")
                    return False
        except Exception as e:
            self.log.error("executeToDo() Error -> " + str(e))
            return False

    def load(self, dataset, table) -> bool:
        """ Upload a dataset (etlDataset) in the ABBYY Timeline repository (in one transaction)
        Args:
            dfDataset (etlDataset): Dataset with the Data to upload
        Returns:
            bool: False if error
        """
        try:
            self.log.info("Upload the data into the ABBYY Timeline repository in one transaction")
            api = piApiRepositoryWrapper(self.token, self.server)
            api.log = self.log
            if (self.repositoryConfig.loaded):
                fileKeys = []
                blocIdx, blocIdxEnd = 0, 0
                datasize = dataset.count
                if (datasize > C.API_BLOC_SIZE_LIMIT):
                    self.log.info("Data (all) size (Nb Lines= {}) is larger than the upload limit {}, split the data in several data blocs".format(datasize , C.API_BLOC_SIZE_LIMIT))
                    blocNum = 1
                    while (blocIdxEnd < dataset.count-1):
                        # Create the blocs (Nb of line to API_BLOC_SIZE_LIMIT)
                        blocIdxEnd = blocIdx + C.API_BLOC_SIZE_LIMIT - 1
                        if (blocIdxEnd >= dataset.count-1):
                            blocIdxEnd = dataset.count-1
                        self.log.debug("Data bloc N°{}, Index from {} -> {}".format(blocNum, blocIdx, blocIdxEnd))
                        #blocData = dataset.iloc[blocIdx:blocIdxEnd:,:]
                        blocData = dataset.getRowBloc(blocIdx, blocIdxEnd)
                        blocIdx += C.API_BLOC_SIZE_LIMIT 
                        # 2 - Prepare the upload
                        uploadCfg = api.prepareUpload(self.repositoryConfig.repositoryId)
                        # 3 - Upload the file to the server
                        blocData_toupload = blocData.get_csv()
                        uploadOK = api.uploadData(blocData_toupload, uploadCfg.url, uploadCfg.headers)
                        fileKeys.append(uploadCfg.key)
                        if (uploadOK):
                            self.log.info("Data bloc N°{} was uploaded successfully".format(blocNum))
                        else:
                            self.log.warning("Data bloc N°{} was NOT uploaded successfully".format(blocNum))
                            break
                        blocNum +=1
                else:
                    self.log.debug("The data can be uploaded in one unique bloc")
                    # 2 - Prepare the complete file upload
                    uploadCfg = api.prepareUpload(self.repositoryConfig.repositoryId)
                    fileKeys.append(uploadCfg.key)
                    blocData_toupload = dataset.get_csv()
                    uploadOK = api.uploadData(blocData_toupload, uploadCfg.url, uploadCfg.headers)
                    keys = uploadCfg.key
                    if (uploadOK):
                        self.log.info("Data was uploaded successfully")
                    else:
                        self.log.warning("Data was NOT uploaded successfully")
                keys = json.dumps(fileKeys)
                if (uploadOK):
                    self.log.info("Load the uploaded data/bloc(s) into the ABBYY Timeline repository table {}".format(table))
                    # 4 - Load the file into the ABBYY Timeline repository
                    processId = api.loadFileToPIRepository(self.repositoryConfig.repositoryId, keys, table)
                    self.waitForEndOfProcessing(processId)
                else:
                    self.log.error("The data have not been loaded successfully")
            return True
        
        except Exception as e:
            self.log.error("upload() Error -> " + str(e))
            return False