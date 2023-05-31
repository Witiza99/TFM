#####################################################################################################
#                                           Code of STRPLibrary                                     #
#####################################################################################################

# Libraries
import paho.mqtt.client as libmqtt
from _thread import allocate_lock, start_new_thread
import random
import json
from time import sleep
import re
from uuid import getnode as get_mac
from datetime import datetime
# modules for get info
import psutil
import platform

# Global variables
ROOTMASTER = "/CONTROL/MASTER/"
ROOTSLAVE = "/CONTROL/SLAVE/"

#####################################################################################################
#                                           Class Master                                            #
#                   This class controls the entire stream processing control structure              #
#####################################################################################################
class Master:

    ###########################
    #    Private variables    #
    ###########################

    # Connection variables 
    _IP_SERVER = ""
    _PORT = -1
    conex_to_Server= libmqtt.Client()
    conex = False

    # Numers of stages and Structure
    _Stages = 0
    _Structure = []

    # ID Given
    _Status_IDS = []

    # Lock
    lock = allocate_lock()# lock for event processor
    lock.acquire()
    lock_event = allocate_lock()# lock for events

    # Message
    message = None

    # Event
    event = 0
    ESTABLISHED_CONEX = 1
    FAIL_CONEX = 2
    PUBLICATION_MADE = 3
    ACTIVE_SUBSCRIPTION = 4
    DATA_RECEIVED = 5



    ###########################
    #         Methods         #
    ###########################


    ###########################
    #         Private         #
    ###########################

    # ==============================
    # Builder
    # ==============================
    # This funcion create the obj master
    # Param in ->
    # pipelines: Nº pipelines 
    # stages: Nº stages 
    # IP_SERVER: Ip server
    # PORT: Nº port 
    # Param out ->
    # None
    def __init__(self, pipelines, stages, IP_SERVER, PORT):
        #create events for eventsProcessor
        self.__events()

        # Create a thread with the func __eventsProcessor
        start_new_thread(self.__eventsProcessor,())

        # Assign initial values to private variables
        self._Stages = stages
        self._Pipelines = pipelines
        for i in range(pipelines):
            self._Structure.append([])

        self._IP_SERVER = IP_SERVER
        self._PORT      = PORT
        self.__connect_to_server()# try to connect to the server


    # ==============================
    # __established_connection
    # ==============================
    # Func for established connection
    # Param in ->
    # client: client
    # userdata: userdata
    # flags: flags
    # rc: rc
    # Param out ->
    # None
    def __established_connection(self, client, userdata, flags, rc):
        if rc == 0:
            #This code controls the use of the threat
            while(not self.lock.locked()):
                sleep(1)
            self.lock_event.acquire()
            self.event = self.ESTABLISHED_CONEX
            self.lock.release()


    # ==============================
    # __fail_connection
    # ==============================
    # Func for fail connection
    # Param in ->
    # client: client
    # *args: args
    # Param out ->
    # None
    def __fail_connection(self, client, *args):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        self.lock_event.acquire()
        self.event = self.FAIL_CONEX
        self.lock.release()


    # ==============================
    # __publication_made
    # ==============================
    # Func for publication made
    # Param in ->
    # client: client
    # *args: args
    # Param out ->
    # None
    def __publication_made(self, client, *args):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        self.lock_event.acquire()
        self.event = self.PUBLICATION_MADE
        self.lock.release()


    # ==============================
    # __active_subscription
    # ==============================
    # Func for active subscription
    # Param in ->
    # client: client
    # *args: args
    # Param out ->
    # None
    def __active_subscription(self, client, *args):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        self.lock_event.acquire()
        self.event = self.ACTIVE_SUBSCRIPTION
        self.lock.release()


    # ==============================
    # __data_received
    # ==============================
    # Func for data received
    # Param in ->
    # client: client
    # *args: args
    # msg: msg with the client event
    # Param out ->
    # None
    def __data_received(self, client, data, msg):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        # lock
        self.lock_event.acquire()
        self.message = msg
        self.event = self.DATA_RECEIVED
        self.lock.release()


    # ==============================
    # __events
    # ==============================
    # Event register
    # Param in ->
    # None
    # Param out ->
    # None
    def __events(self):
        # Event register
        self.conex_to_Server.on_connect = self.__established_connection
        self.conex_to_Server.on_publish = self.__publication_made
        self.conex_to_Server.on_connect_fail = self.__fail_connection
        self.conex_to_Server.on_subscribe = self.__active_subscription
        self.conex_to_Server.on_message = self.__data_received


    # ==============================
    # __eventsProcessor
    # ==============================
    # Func to process the events (previously modified)
    # Param in ->
    # None
    # Param out ->
    # None
    def __eventsProcessor(self):
        while True:
            self.lock.acquire()
            if self.event == self.FAIL_CONEX:
                print ("Error, can't connect to the server")

            elif self.event == self.ESTABLISHED_CONEX:
                self.conex = True
                MQTT_TOPIC = [(ROOTMASTER + "#",0)]
                self.conex_to_Server.subscribe(MQTT_TOPIC)
                print("ESTABLISHED_CONEX")

            elif self.event == self.PUBLICATION_MADE:
                print ("PUBLICATION_MADE")

            elif self.event == self.ACTIVE_SUBSCRIPTION:
                print ("ACTIVE_SUBSCRIPTION")

            elif self.event == self.DATA_RECEIVED:
                # check the correct topic for get number
                RegEx_Get_Id = "^"+ ROOTMASTER +"GET_MY_ID/.*$"
                RegEx_Get_Connect_Nodes = "^"+ ROOTMASTER +"ID-.+/GET_CONNECT_NODES$"
                Regex_Give_Info = "^"+ ROOTMASTER + "ID-.+/GIVE_INFO$"

                # if the topic match with the Regex Get Id, give id to this slave
                if re.search(RegEx_Get_Id, self.message.topic): 
                    print (f"DATA_RECEIVED: {self.message.topic}={self.message.payload}")
                    code = self.message.topic[-29:]# get code to return the new id
                    data_in=json.loads(self.message.payload)
                    # take pipeline where it will work
                    Number_Pipeline = data_in
                    # assign number
                    new_Id = 0
                    for i in range(len(self.get_Structure())):
                        new_Id += len(self.get_Structure()[i])
                    # assign stage
                    if len(self.get_Structure()[Number_Pipeline]) != 0:# check if the pipeline is empty
                        if len(self.get_Structure()[Number_Pipeline][-1]['List_Stages']) != 0:# check if List_Stages its empty
                            if self.get_Structure()[Number_Pipeline][-1]['List_Stages'][-1] != -1:# check if Last node its not working
                                Number_Stage = [self.get_Structure()[Number_Pipeline][-1]['List_Stages'][-1] + 1] #take new stage inside the pipeline
                                if Number_Stage[-1] >= self.get_Stages():# check if new stage is out of range
                                    Number_Stage = []
                            else:
                                Number_Stage = []
                        else:
                            Number_Stage = []
                    else:
                        Number_Stage = [0]
                    # create dicc for new slave
                    dicc = {
                        "ID":new_Id,
                        "List_Stages":Number_Stage
                    }
                    dicc_tmp = {}# dicc empty
                    self._Status_IDS.insert(new_Id,[0,dicc_tmp])# id with counter + diccionary with status
                    self.get_Structure()[Number_Pipeline].append(dicc)
                    data_out=json.dumps(dicc)
                    self.conex_to_Server.publish(ROOTSLAVE +"SET_MY_ID/" + code, data_out)

                # if the topic match with the Get Connect Nodes, for this id get the next and previous nodes
                elif re.search(RegEx_Get_Connect_Nodes, self.message.topic):
                    print (f"DATA_RECEIVED: {self.message.topic}={self.message.payload}")
                    data_in=json.loads(self.message.payload)
                    dicc = data_in
                    Number_Pipeline = self.__Get_Pipeline(dicc["ID"])
                    #Get connect nodes
                    if len(dicc["List_Stages"]) != 0:# check if List_Stages is empty
                        Previus_ID = self.ID_Previus_Stage(Number_Pipeline, dicc["List_Stages"][0])
                        if  Previus_ID != -1:# check if exist previus id
                            data_out=json.dumps(Previus_ID)
                            self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(dicc["ID"]) + "/NEW_SUSCRIBER", data_out)# update previous nodes for actual id
                            data_out=json.dumps(dicc["ID"])
                            self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(Previus_ID) + "/NEW_PUBLISHER", data_out)# update next nodes for before id

                # if the topic match with the Give Info, update _Status_IDS with new status info and reset the counter
                elif re.search(Regex_Give_Info, self.message.topic):
                    print (f"DATA_RECEIVED: {self.message.topic}")
                    data_in=json.loads(self.message.payload)
                    dicc = data_in
                    Iterator_ID = dicc["ID"]
                    self._Status_IDS[Iterator_ID] = [0, dicc] 

            self.lock_event.release()


    # ==============================
    # __connect_to_server
    # ==============================
    # Func that while the master is not connected (server MQTT), try to connect
    # Param in ->
    # None
    # Param out ->
    # None
    def __connect_to_server(self):
        while(not self.conex):
            print("connecting...")
            try:
                self.conex_to_Server.connect(self.get_IP_SERVER(), self.get_PORT())
                self.conex_to_Server.loop_start()
            except:
                self.conex_to_Server.loop_stop(force=True)
                self.event = self.FAIL_CONEX
                self.lock.release()
            sleep(5)


    # ==============================
    # __Get_Pipeline
    # ==============================
    # Func get the pipeline for id given
    # Param in ->
    # Id: Nº id
    # Param out ->
    # i: pipeline for id, return -1 if dont exist
    def __Get_Pipeline(self, Id):
        structure = self.get_Structure()
        for i in range(len(structure)):
            for w in structure[i]:
                if w["ID"] == Id:
                    return i
        return -1


    ###########################
    #          Public         #
    ###########################

    # ==============================
    # get_Stages
    # ==============================
    # This funcion return Nº stages
    # Param in ->
    # None
    # Param out ->
    # _Stages: Nº stages 
    def get_Stages(self):
        return self._Stages


    # ==============================
    # get_Pipelines
    # ==============================
    # This funcion return pipelines
    # Param in ->
    # None
    # Param out ->
    # _Pipelines: Nº pipelines 
    def get_Pipelines(self):
        return self._Pipelines
        

    # ==============================
    # get_Structure
    # ==============================
    # This funcion return Structure
    # Param in ->
    # None
    # Param out ->
    # _Structure: List with net structure 
    def get_Structure(self):
        return self._Structure


    # ==============================
    # get_Status_IDS
    # ==============================
    # This funcion return Status_IDS
    # Param in ->
    # None
    # Param out ->
    # _Status_IDS: List with id and status counters of each id and dicc with status info
    def get_Status_IDS(self):
        return self._Status_IDS


    # ==============================
    # get_IP_SERVER
    # ==============================
    # This funcion return Ip server
    # Param in ->
    # None
    # Param out ->
    # _IP_SERVER: Ip server
    def get_IP_SERVER(self):
        return self._IP_SERVER


    # ==============================
    # get_PORT
    # ==============================
    # This funcion return port
    # Param in ->
    # None
    # Param out ->
    # _PORT: port
    def get_PORT(self):
        return self._PORT


    # ==============================
    # get_stages_from_Id
    # ==============================
    # This funcion return stages from id
    # Param in -> 
    # Id: Nº id
    # Param out ->
    # list_stages: list stages
    def get_stages_from_Id(self, Id):
        structure = self.get_Structure()
        list_stages = []
        for i in range(len(structure)):
            for w in structure[i]:
                if w["ID"] == Id:
                    if len(w["List_Stages"]) != 0:# check if List_Stages is empty
                        list_stages = w["List_Stages"]
        return list_stages


    # ==============================
    # set_stages_from_Id
    # ==============================
    # This funcion return stages from id
    # Param in -> 
    # Id: Nº id
    # list_stages: list stages
    # Param out ->
    # None
    def set_stages_from_Id(self, Id, list_stages):
        structure = self.get_Structure()
        for i in range(len(structure)):
            for w in structure[i]:
                if w["ID"] == Id:
                    w["List_Stages"] = list_stages


    # ==============================
    # request_info
    # ==============================
    # Func that increase all counters for status counters and publish REQUEST_INFO
    # Param in ->
    # None
    # Param out ->
    # None
    def request_info(self):
        for i in self.get_Status_IDS():
            if i[0] < 999: #999 is the counter limit
                i[0] = i[0] + 1 
        self.conex_to_Server.publish(ROOTSLAVE + "REQUEST_INFO", 0)


    # ==============================
    # ID_Previus_Stage
    # ==============================
    # Func get the previous stage for stage given (inside pipeline)
    # Param in ->
    # pipeline: Nº pipeline 
    # stage: Nº stage 
    # Param out ->
    # ID_Previus_Node: ID for previous stage to the stage passed
    def ID_Previus_Stage(self, pipeline, stage):
        structure = self.get_Structure()
        ID_Previus_Node = -1
        if stage != 0:# if stage is 0, we dont need to check
            for i in range(len(structure)):
                if i == pipeline:
                    for w in structure[i]:
                        for x in w["List_Stages"]:
                            if x == stage-1:
                                ID_Previus_Node = w["ID"]
        return ID_Previus_Node
        

    # ==============================
    # ID_Next_Stage
    # ==============================
    # Func get the next stage for stage given (inside pipeline)
    # Param in ->
    # pipeline: Nº pipeline 
    # stage: Nº stage 
    # Param out ->
    # ID_Next_Node: ID for next stage to the stage passed
    def ID_Next_Stage(self, pipeline, stage):
        structure = self.get_Structure()
        ID_Next_Node = -1
        for i in range(len(structure)):
            if i == pipeline:
                for w in structure[i]:
                    for x in w["List_Stages"]:
                        if x == stage+1:
                            ID_Next_Node = w["ID"]
        return ID_Next_Node


    # ==============================
    # delete_Id_status
    # ==============================
    # Func that delete status for id given, update neighbors status, this id dont work
    # Param in ->
    # Id: Nº id
    # Param out ->
    # None
    def delete_Id_status(self, Id):
        structure = self.get_Structure()
        for i in range(len(structure)):
            for w in structure[i]:
                if w["ID"] == Id:
                    if len(w["List_Stages"]) != 0:# check if List_Stages is empty
                        empty_stages = w["List_Stages"]# save stages for update neighbors
                        w["List_Stages"] = [-1]# this id dont work
                        dicc = {
                            "ID":Id,
                            "List_Stages": w["List_Stages"]
                        }
                        # update id given
                        data_out=json.dumps(dicc)
                        self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(Id) + "/UPDATE_STAGE", data_out)
                        data_out=json.dumps(-1)
                        self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(Id) + "/NEW_SUSCRIBER", data_out)
                        self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(Id) + "/NEW_PUBLISHER", data_out)
                        # update id for next stage
                        Previus_ID = self.ID_Previus_Stage(i, empty_stages[0])
                        if Previus_ID != -1:
                            self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(Previus_ID) + "/NEW_PUBLISHER", data_out)
                        # update id for previous stage
                        Post_ID = self.ID_Next_Stage(i, empty_stages[-1])
                        if Post_ID != -1:
                            self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(Post_ID) + "/NEW_SUSCRIBER", data_out)
    

    # ==============================
    # get_Stages_Without_ID
    # ==============================
    # Func get Stages that dont have ID
    # Param in ->
    # pipeline: Nº pipeline
    # Param out ->
    # Stage_Without_ID: return list stage without ID
    def get_Stages_Without_ID(self, pipeline)  :              
        structure = self.get_Structure()
        list_Stages_With_ID = []
        list_Stages_tmp = []
        for i in range(self.get_Stages()):
            list_Stages_tmp.append(i)# save all stages
            for w in structure[pipeline]:
                if len(w) != 0:# check if pipeline is empty
                    if i in w["List_Stages"]:# check if status i is inside in w["List_Stages"]
                        list_Stages_With_ID.append(i)
        Stages_Without_ID = list(set(list_Stages_tmp) - set(list_Stages_With_ID))
        return Stages_Without_ID


    # ==============================
    # get_Free_Slaves
    # ==============================
    # Func get free slaves
    # Param in ->
    # pipeline: Nº pipeline
    # Param out ->
    # list_free_slaves: return list free slaves
    def get_Free_Slaves(self, pipeline):
        structure = self.get_Structure()
        list_free_slaves = []
        for i in structure[pipeline]:
            if len(i) != 0:# check if pipeline is empty
                if len(i["List_Stages"]) == 0:# check if ["List_Stages"] is empty for each id in pipeline
                    list_free_slaves.append(i["ID"])
        return list_free_slaves


    # ==============================
    # set_Stage_To_Id
    # ==============================
    # Func get free slaves
    # Param in ->
    # stage: Nº stage
    # Id: Nº Id
    # Param out ->
    # None
    def set_Stage_To_Id(self, stage, Id):
        structure = self.get_Structure()
        for i in range(len(structure)):
            for w in structure[i]:
                if w["ID"] == Id:
                    # update id
                    print(w["List_Stages"])
                    w["List_Stages"].append(stage)
                    print(w["List_Stages"])
                    tmp_list = w["List_Stages"]
                    print(tmp_list)
                    w["List_Stages"] = list(set(tmp_list))
                    dicc = {
                        "ID":Id,
                        "List_Stages": w["List_Stages"]
                    }
                    data_out=json.dumps(dicc)
                    self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(Id) + "/UPDATE_STAGE", data_out)
                    # update id for previous stage
                    Previus_ID = self.ID_Previus_Stage(i, w["List_Stages"][0])
                    if  Previus_ID != -1:
                        data_out=json.dumps(Previus_ID)
                        self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(dicc["ID"]) + "/NEW_SUSCRIBER", data_out)
                        data_out=json.dumps(dicc["ID"])
                        self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(Previus_ID) + "/NEW_PUBLISHER", data_out)
                    # update id for next stage
                    Post_ID = self.ID_Next_Stage(i, w["List_Stages"][-1])
                    if Post_ID != -1:
                        data_out=json.dumps(Post_ID)
                        self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(dicc["ID"]) + "/NEW_PUBLISHER", data_out)
                        data_out=json.dumps(dicc["ID"])
                        self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(Post_ID) + "/NEW_SUSCRIBER", data_out)


    # ==============================
    # delete_Stage_To_Id
    # ==============================
    # Func delete stage from id
    # Param in ->
    # stage: Nº stage
    # Id: Nº Id
    # Param out ->
    # None
    def delete_Stage_To_Id(self, stage, Id):
        structure = self.get_Structure()
        for i in range(len(structure)):
            for w in structure[i]:
                if w["ID"] == Id:
                    # update id
                    if stage in w["List_Stages"]:
                        list_stages_update = w["List_Stages"]
                        while(stage in list_stages_update):# remove the stage
                            list_stages_update.remove(stage)
                        # update id
                        w["List_Stages"] = list(set(list_stages_update))
                        dicc = {
                            "ID":Id,
                            "List_Stages": w["List_Stages"]
                        }
                        data_out=json.dumps(dicc)
                        self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(Id) + "/UPDATE_STAGE", data_out)
                        # update id for previous stage
                        Previus_ID = self.ID_Previus_Stage(i, w["List_Stages"][0])
                        if  Previus_ID != -1:
                            data_out=json.dumps(Previus_ID)
                            self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(dicc["ID"]) + "/NEW_SUSCRIBER", data_out)
                            data_out=json.dumps(dicc["ID"])
                            self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(Previus_ID) + "/NEW_PUBLISHER", data_out)
                        # update id for next stage
                        Post_ID = self.ID_Next_Stage(i, w["List_Stages"][-1])
                        if Post_ID != -1:
                            data_out=json.dumps(Post_ID)
                            self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(dicc["ID"]) + "/NEW_PUBLISHER", data_out)
                            data_out=json.dumps(dicc["ID"])
                            self.conex_to_Server.publish(ROOTSLAVE + "ID-" + str(Post_ID) + "/NEW_SUSCRIBER", data_out)
                    else:
                        print("Can't delete stage, that stage no exist in this id")



#####################################################################################################
#                                           Class Slave                                             #
#                                This class is a slave of the structure                             #
#####################################################################################################
class Slave:

    ###########################
    #    Private variables    #
    ###########################

    # Connection variables 
    _IP_SERVER = ""
    _PORT = -1
    conex_to_Server= libmqtt.Client()
    conex = False

    # Slave MY_ID, stage, and mac
    _MY_ID = -1
    _Stages = []
    _Mac = hex(get_mac())
    _Time = datetime.now().time()

    # Nodes 
    _Previus_Node = []
    _Next_Node = []

    # Topics
    _Topics_Subscribe = []
    _Topics_Publish = []

    # Lock
    lock = allocate_lock()# lock for event processor 
    lock.acquire()
    lock_event = allocate_lock()# lock for events

    # Message
    message = None

    # Event
    event = 0
    ESTABLISHED_CONEX = 1
    FAIL_CONEX = 2
    PUBLICATION_MADE = 3
    ACTIVE_SUBSCRIPTION = 4
    DATA_RECEIVED = 5



    ###########################
    #         Methods         #
    ###########################


    ###########################
    #         Private         #
    ###########################

    # ==============================
    # Builder
    # ==============================
    # This funcion create the obj slave
    # Param in ->
    # pipeline: Nº pipeline 
    # IP_SERVER: Ip server
    # PORT: Nº port 
    # Param out ->
    # None
    def __init__(self, pipeline, IP_SERVER, PORT):
        #create events for eventsProcessor
        self.__events()

        # Create a thread with the func __eventsProcessor
        start_new_thread(self.__eventsProcessor,())

        # Assign initial values to private variables
        self._Pipeline = pipeline

        self._IP_SERVER = IP_SERVER
        self._PORT      = PORT
        self.__connect_to_server()# try to connect to the server

        # Stop code when this slave doesn't have ID
        while(self.get_MY_ID() == -1):
            self.__TakeNumber()
            print("Taking a ID from Master... ")
            sleep(10)

        # This slave subscribes to its own id
        MQTT_TOPIC = [(ROOTSLAVE + "ID-" + str(self.get_MY_ID()) + "/" +"#",0)]
        self.conex_to_Server.subscribe(MQTT_TOPIC)
        # The slave requests his neighbors nodes
        dicc = {
            "ID":self.get_MY_ID(),
            "List_Stages":self.get_Stages()
        }
        data_out=json.dumps(dicc)
        self.conex_to_Server.publish(ROOTMASTER + "ID-" + str(self.get_MY_ID()) + "/GET_CONNECT_NODES", data_out)

        # The slave is ready to give his info 
        MQTT_TOPIC = [(ROOTSLAVE + "REQUEST_INFO",0)]
        self.conex_to_Server.subscribe(MQTT_TOPIC)


    # ==============================
    # __established_connection
    # ==============================
    # Func for established connection
    # Param in ->
    # client: client
    # userdata: userdata
    # flags: flags
    # rc: rc
    # Param out ->
    # None
    def __established_connection(self, client, userdata, flags, rc):
        if rc == 0:
            #This code controls the use of the threat
            while(not self.lock.locked()):
                sleep(1)
            self.lock_event.acquire()
            self.event = self.ESTABLISHED_CONEX
            self.lock.release()


    # ==============================
    # __fail_connection
    # ==============================
    # Func for fail connection
    # Param in ->
    # client: client
    # *args: args
    # Param out ->
    # None
    def __fail_connection(self, client, *args):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        self.lock_event.acquire()
        self.event = self.FAIL_CONEX
        self.lock.release()


    # ==============================
    # __publication_made
    # ==============================
    # Func for publication made
    # Param in ->
    # client: client
    # *args: args
    # Param out ->
    # None
    def __publication_made(self, client, *args):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        self.lock_event.acquire()
        self.event = self.PUBLICATION_MADE
        self.lock.release()


    # ==============================
    # __active_subscription
    # ==============================
    # Func for active subscription
    # Param in ->
    # client: client
    # *args: args
    # Param out ->
    # None
    def __active_subscription(self, client, *args):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        self.lock_event.acquire()
        self.event = self.ACTIVE_SUBSCRIPTION
        self.lock.release()


    # ==============================
    # __data_received
    # ==============================
    # Func for data received
    # Param in ->
    # client: client
    # *args: args
    # msg: msg with the client event
    # Param out ->
    # None
    def __data_received(self, client, data, msg):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        # lock
        self.lock_event.acquire()
        self.message = msg
        self.event = self.DATA_RECEIVED
        self.lock.release()


    # ==============================
    # __events
    # ==============================
    # Event register
    # Param in ->
    # None
    # Param out ->
    # None
    def __events(self):
        # Event register
        self.conex_to_Server.on_connect = self.__established_connection
        self.conex_to_Server.on_publish = self.__publication_made
        self.conex_to_Server.on_connect_fail = self.__fail_connection
        self.conex_to_Server.on_subscribe = self.__active_subscription
        self.conex_to_Server.on_message = self.__data_received


    # ==============================
    # __eventsProcessor
    # ==============================
    # Func to process the events (previously modified)
    # Param in ->
    # None
    # Param out ->
    # None
    def __eventsProcessor(self):
        while True:
            self.lock.acquire()
            if self.event == self.FAIL_CONEX:
                print ("Error, can't connect to the server")

            elif self.event == self.ESTABLISHED_CONEX:
                self.conex = True
                print("ESTABLISHED_CONEX")

            elif self.event == self.PUBLICATION_MADE:
                print ("PUBLICATION_MADE")

            elif self.event == self.ACTIVE_SUBSCRIPTION:
                print ("ACTIVE_SUBSCRIPTION")

            elif self.event == self.DATA_RECEIVED:
                print (f"DATA_RECEIVED: {self.message.topic}={self.message.payload}")
                # Check topic for save the number
                RegEx_Set_ID = "^"+ ROOTSLAVE + "SET_MY_ID/" + str(self._Mac) + str(self._Time) + "$"#quitar time en el futuro
                RegEx_New_Subcriber = "^"+ ROOTSLAVE + "ID-" + str(self.get_MY_ID()) + "/NEW_SUSCRIBER"
                RegEx_New_Publisher = "^"+ ROOTSLAVE + "ID-" + str(self.get_MY_ID()) + "/NEW_PUBLISHER"
                RegEx_Update_Stage = "^"+ ROOTSLAVE + "ID-" + str(self.get_MY_ID()) + "/UPDATE_STAGE"
                RegeX_Request_Info = "^"+ ROOTSLAVE + "REQUEST_INFO" + "$"

                # if the topic match with the RegEx Set ID, assign the id to this slave
                if re.search(RegEx_Set_ID, self.message.topic):
                    # Assign number ID and list stages
                    data_in=json.loads(self.message.payload)
                    print(data_in)
                    self._MY_ID = data_in['ID']
                    self._Stages = data_in['List_Stages']

                # if the topic match with the RegEx New Subcriber, update the previous node
                elif re.search(RegEx_New_Subcriber, self.message.topic):
                    # Assign new subcriber
                    data_in=json.loads(self.message.payload)
                    if data_in != -1:
                        self._Previus_Node.clear()
                        self._Previus_Node.append(data_in) 
                    else:
                        self._Previus_Node.clear()

                # if the topic match with the RegEx New Publisher, update the next node
                elif re.search(RegEx_New_Publisher, self.message.topic):
                    # Assign next node
                    data_in=json.loads(self.message.payload)
                    if data_in != -1:
                        self._Next_Node.clear()
                        self._Next_Node.append(data_in)
                    else:
                        self._Next_Node.clear()

                # if the topic match with the RegEx Update Stage, update the stage for this slave
                elif re.search(RegEx_Update_Stage, self.message.topic):
                    data_in=json.loads(self.message.payload)
                    self._Stages = data_in['List_Stages']

                # if the topic match with the RegeX Request Info, send the status info to master
                elif re.search(RegeX_Request_Info, self.message.topic):
                    # Get info from this slave
                    dicc = self.__get_Info_From_Slave()
                    data_out=json.dumps(dicc)
                    self.conex_to_Server.publish(ROOTMASTER + "ID-" + str(self.get_MY_ID()) + "/GIVE_INFO", data_out)

            self.lock_event.release()


    # ==============================
    # __connect_to_server
    # ==============================
    # Func that while the slave is not connected (server MQTT), try to connect
    # Param in ->
    # None
    # Param out ->
    # None
    def __connect_to_server(self):
        while(not self.conex):
            print("connecting...")
            try:
                self.conex_to_Server.connect(self.get_IP_SERVER(), self.get_PORT())
                self.conex_to_Server.loop_start()
            except:
                self.conex_to_Server.loop_stop(force=True)
                self.event = self.FAIL_CONEX
                self.lock.release()
            sleep(5)

    # ==============================
    # __TakeNumber
    # ==============================
    # This funcion take Id from master to this slave
    # Param in ->
    # None
    # Param out ->
    # None
    def __TakeNumber(self):
        MQTT_TOPIC = [(ROOTSLAVE + "SET_MY_ID/" + str(self._Mac) + str(self._Time),0)]# subscribe to his set id
        self.conex_to_Server.subscribe(MQTT_TOPIC)
        data_out=json.dumps(self.get_Pipeline())
        self.conex_to_Server.publish(ROOTMASTER + "GET_MY_ID/" + str(self._Mac) + str(self._Time), data_out)


    # ==============================
    # __get_size
    # ==============================
    # This funcion Scale bytes to its proper format
    # Param in ->
    # bytes: Nº bytes
    # suffix: type format
    # Param out ->
    # bytes: bytes with correct format
    """def __get_size(bytes, suffix="B"):
        
        Example:
            1253656 => '1.20MB'
            1253656678 => '1.17GB'
        
        factor = 1024
        for unit in ["", "K", "M", "G", "T", "P"]:
            if bytes < factor:
                return f"{bytes:.2f}{unit}{suffix}"
            bytes /= factor"""


    # ==============================
    # __get_Info_From_Slave
    # ==============================
    # This funcion get all status info from slave
    # Param in ->
    # None
    # Param out ->
    # dicc: dicc with all status info from slave
    def __get_Info_From_Slave(self):
        # Stats Frecuency
        cpufreq = psutil.cpu_freq()
        # Stats Mem
        svmem = psutil.virtual_memory()
        # Stats Network
        NetSpeed = psutil.net_if_stats()
        InfoAllNIC = []
        for nic, addrs in psutil.net_if_addrs().items():
            if nic in NetSpeed:
                st = NetSpeed[nic]
                diccNic ={
                    "NIC": nic,
                    "Speed": st.speed
                }
                InfoAllNIC.append(diccNic)

        # Stats Battery
        battery = psutil.sensors_battery()
        dicc = {
                        "ID":self.get_MY_ID(),
                        "MAX_FREQUENCY(MHZ)": round(cpufreq.max, 2),
                        "MIN_FREQUENCY(MHZ)": round(cpufreq.min, 2),
                        "CURRENT_FREQUENCY(MHZ)": round(cpufreq.current, 2),
                        "TOTAL_CPU_USAGE(%)": psutil.cpu_percent(),
                        "MEMORY_PERCENTAGE(%)": svmem.percent,
                        "NETWORK_SPEED(MB)": InfoAllNIC,
                        "BATTERY_PERCENTAGE(%)": battery.percent
                    }
        return dicc


    ###########################
    #          Public         #
    ###########################

    # ==============================
    # get_MY_ID
    # ==============================
    # This funcion return Id from slave
    # Param in ->
    # None
    # Param out ->
    # _MY_ID: Nº Id
    def get_MY_ID(self):
        return self._MY_ID


    # ==============================
    # get_Stages
    # ==============================
    # This funcion return the stages from slave 
    # Param in ->
    # None
    # Param out ->
    # _Stages: List with stages
    def get_Stages(self):
        Tmp_Stages = self._Stages
        Tmp_Stages.sort()
        self._Stages = Tmp_Stages
        return self._Stages


    # ==============================
    # get_Pipeline
    # ==============================
    # This funcion return the pipeline from slave 
    # Param in ->
    # None
    # Param out ->
    # _Pipeline: Nº pipeline
    def get_Pipeline(self):
        return self._Pipeline


    # ==============================
    # get_IP_SERVER
    # ==============================
    # This funcion return Ip server
    # Param in ->
    # None
    # Param out ->
    # _IP_SERVER: Ip server
    def get_IP_SERVER(self):
        return self._IP_SERVER


    # ==============================
    # get_PORT
    # ==============================
    # This funcion return port
    # Param in ->
    # None
    # Param out ->
    # _PORT: port
    def get_PORT(self):
        return self._PORT


    # ==============================
    # get_Previus_Node
    # ==============================
    # This funcion return the previous node from slave 
    # Param in ->
    # None
    # Param out ->
    # _Previus_Node: List with the previous node
    def get_Previus_Node(self):
        return self._Previus_Node


    # ==============================
    # get_Next_Node
    # ==============================
    # This funcion return the next node from slave 
    # Param in ->
    # None
    # Param out ->
    # _Next_Node: List with the next node
    def get_Next_Node(self):
        return self._Next_Node


    # ==============================
    # get_Topics_Subscribe
    # ==============================
    # This funcion return the topics to which the slave subscribes 
    # Param in ->
    # None
    # Param out ->
    # _Topics_Subscribe: List with Topics Subscribe
    def get_Topics_Subscribe(self):
        _Topics_Subscribe = []
        for i in self.get_Previus_Node():
            _Topics_Subscribe.append("/APPLICATION_CONTEXT/ID-" + str(i))
        return _Topics_Subscribe


    # ==============================
    # get_Topics_Publish
    # ==============================
    # This funcion return the topics to which the slave publishes 
    # Param in ->
    # None
    # Param out ->
    # _Topics_Subscribe: List with Topics Publish
    def get_Topics_Publish(self):
        _Topics_Publish = []
        for i in self.get_Next_Node():
            _Topics_Publish.append("/APPLICATION_CONTEXT/ID-" + str(i))
        return _Topics_Publish
