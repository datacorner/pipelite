__author__ = "datacorner.fr"
__email__ = "admin@datacorner.fr"
__license__ = "MIT"

import pipelite.constants as C
from pipelite.pipelines.management.plNode import plNode

class plTree():
    def __init__(self):
        self.flatNodes = [] # array of plNode
    
    def load(self, etlObjects):
        """ Load the plObject inside a tree & flat structure
        Args:
            etlObjects (etlObjects): All the pipeline objects
        """
        # Create the nodes without in an out
        self.flatNodes = []
        for obj in etlObjects:
            newNode = plNode()
            newNode.id = obj.id
            newNode.objtype = obj.objtype
            newNode.ready = (obj.objtype == C.PLJSONCFG_EXTRACTOR or obj.objtype == C.PLJSONCFG_TRANSFORMER)
            newNode.inputs = []
            newNode.outputs = []
            self.flatNodes.append(newNode)
        # Set the in and out entries
        for obj in etlObjects:
            if (obj.objtype == C.PLJSONCFG_TRANSFORMER):
                self.setEntries(obj.id, obj.dsInputs, obj.dsOutputs)
    
    def buildSeqPipeline(self) -> []:
        """ Generate the sequential list of the etlObjects to exectue (in the good order)
        Returns:
            [str]: List of objects Ids
        """
        def addToPipeline(pipeline, node):
            if (node.objtype != C.PLJSONCFG_BUFFERPIPELINE):
                pipeline.append(node.id)

        trNode = self.findNextTransformer() # first here !
        pipeline = []
        while (trNode != None):
            for node in trNode.inputs:
                addToPipeline(pipeline, node)
            addToPipeline(pipeline, trNode)
            for node in trNode.outputs:
                if (node.objtype == C.PLJSONCFG_LOADER):
                    addToPipeline(pipeline, node)
                node.ready = True
            trNode.ready = False # Flag the transformer to not reuse it
            trNode = self.findNextTransformer() # first here !
        return pipeline

    def __canBeExecuted__(self, node) -> bool:
        """Verify if the node can be executed or not
        Args:
            node (plNode): node
        Returns:
            bool: True if the node is a step in the pipeline
        """
        canBeTransformerRoot = True
        for ds in node.inputs:
            if (not ds.ready):
                canBeTransformerRoot = False
                break
        return canBeTransformerRoot
    
    def findNextTransformer(self) -> plNode:
        """ find out in the transformer list which one is the next that can be executed (because its inputs are ready)
        Returns:
            plNode: next transformer to execute
        """
        # Find the root node in the flatNodes list
        for node in self.flatNodes:
            if (node.objtype == C.PLJSONCFG_TRANSFORMER and len(node.inputs) > 0 and node.ready):
                if (self.__canBeExecuted__(node)):
                    return node
        return None

    def getNode(self, id) -> plNode:
        """ Returns the plNode with that id
        Args:
            id (str): node id
        Returns:
            plNode: node
        """
        # Check if the node already exists
        for node in self.flatNodes:
            if (node.id == id):
                return node
        # Create a new node (because the search node is a node created during the pipeline execution)
        newNode = plNode()
        newNode.id = id
        newNode.objtype = C.PLJSONCFG_BUFFERPIPELINE
        self.flatNodes.append(newNode)
        return newNode

    def setEntries(self, id, inputs, outputs):
        """ initialize the entries
        Args:
            id (str): id
            inputs ([]): inputs
            outputs ([]): outputs
        """
        # get the node
        currentNode = self.getNode(id)
        # Branch the inputs with existing nodes
        for input in inputs:
            currentNode.inputs.append(self.getNode(input))
        # Branch the outputs with existing nodes
        for output in outputs:
            currentNode.outputs.append(self.getNode(output))

