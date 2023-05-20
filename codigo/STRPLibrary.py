#libraries
import paho.mqtt.client as libmqtt
from _thread import allocate_lock, start_new_thread
import random
import json
from time import sleep
import re
from uuid import getnode as get_mac
import datetime

#modules for get info
import psutil
import platform
from datetime import datetime

ROOT = "/CONTROL/OUT/"


"""
Class Master

This class controls the entire stream processing control structure.
"""
class Master:

    #Connection variables 
    _IP_SERVER = ""
    _PORT = -1

    # Numers of stages
    _Stages = 0
    _Structure = []

    # ID Given
    _Status_IDS = []

    conex_to_Server= libmqtt.Client()
    conex = False

    # lock
    lock = allocate_lock()
    lock.acquire()

    # message
    message = None

    #event
    event = 0
    ESTABLISHED_CONEX = 1
    FAIL_CONEX = 2
    PUBLICATION_MADE = 3
    ACTIVE_SUBSCRIPTION = 4
    DATA_RECEIVED = 5

    # Constructor
    def __init__(self, pipelines, stages, IP_SERVER, PORT):
        
        self.__events()

        start_new_thread(self.__eventsProcessor,())

        self._Stages = stages
        self._Pipelines = pipelines
        for i in range(pipelines):
            self._Structure.append([])

        self._IP_SERVER = IP_SERVER
        self._PORT      = PORT
        self.__connect_to_server()

    # Methods
    def get_Stages(self):
        return self._Stages

    def get_Pipelines(self):
        return self._Pipelines
        
    def get_Structure(self):
        return self._Structure

    def get_Status_IDS(self):
        return self._Status_IDS

    def get_IP_SERVER(self):
        return self._IP_SERVER

    def get_PORT(self):
        return self._PORT

    lock_event = allocate_lock()

    def __established_connection(self, client, userdata, flags, rc):
        if rc == 0:
            #This code controls the use of the threat
            while(not self.lock.locked()):
                sleep(1)
            self.lock_event.acquire()
            self.event = self.ESTABLISHED_CONEX
            self.lock.release()



    def __fail_connection(self, client, *args):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        self.lock_event.acquire()
        self.event = self.FAIL_CONEX
        self.lock.release()


    def __publication_made(self, client, *args):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        self.lock_event.acquire()
        self.event = self.PUBLICATION_MADE
        self.lock.release()


    def __active_subscription(self, client, *args):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        self.lock_event.acquire()
        self.event = self.ACTIVE_SUBSCRIPTION
        self.lock.release()

    
    def __data_received(self, client, data, msg):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        # lock
        self.lock_event.acquire()
        self.message = msg
        self.event = self.DATA_RECEIVED
        self.lock.release()


    def __events(self):
        # Event register
        self.conex_to_Server.on_connect = self.__established_connection
        self.conex_to_Server.on_publish = self.__publication_made
        self.conex_to_Server.on_connect_fail = self.__fail_connection
        self.conex_to_Server.on_subscribe = self.__active_subscription
        self.conex_to_Server.on_message = self.__data_received

    def __eventsProcessor(self):
        while True:
            self.lock.acquire()
            if self.event == self.FAIL_CONEX:
                print ("Error, can't connect to the server")
            elif self.event == self.ESTABLISHED_CONEX:
                self.conex = True
                MQTT_TOPIC = [(ROOT + "#",0)]
                self.conex_to_Server.subscribe(MQTT_TOPIC)
                print("ESTABLISHED_CONEX")
            elif self.event == self.PUBLICATION_MADE:
                print ("PUBLICATION_MADE")
            elif self.event == self.ACTIVE_SUBSCRIPTION:
                print ("ACTIVE_SUBSCRIPTION")
            elif self.event == self.DATA_RECEIVED:
                print (f"DATA_RECEIVED: {self.message.topic}={self.message.payload}")
                #check the correct topic for get number
                RegEx_Get_Id = "^"+ROOT+"GET_MY_ID/.*$"
                RegEx_Get_Connect_Nodes = "^"+ROOT+"ID-.+/GET_CONNECT_NODES$"
                Regex_Give_Info = "^"+ ROOT + "ID-.+/GIVE_INFO$"

                if re.search(RegEx_Get_Id, self.message.topic):
                    code = self.message.topic[-29:]
                    data_in=json.loads(self.message.payload)
                    #take pipeline where it will work
                    Number_Pipeline = data_in
                    #assign number
                    new_Id = 0
                    for i in range(len(self.get_Structure())):
                        new_Id += len(self.get_Structure()[i])
                    #assign stage
                    if len(self.get_Structure()[Number_Pipeline]) != 0:#check if the pipeline is empty
                        if len(self.get_Structure()[Number_Pipeline][-1]['List_Stages']) != 0:
                            Number_Stage = [self.get_Structure()[Number_Pipeline][-1]['List_Stages'][-1] + 1] #take new stage inside the pipeline
                            if Number_Stage[-1] >= self.get_Stages():#check if new stage is out of range
                                Number_Stage = []
                        else:
                            Number_Stage = []
                    else:
                        Number_Stage = [0]

                    dicc = {
                        "ID":new_Id,
                        "List_Stages":Number_Stage
                    }
                    dicc_tmp = {}
                    self._Status_IDS.insert(new_Id,[0,dicc_tmp])#counter + diccionary
                    self.get_Structure()[Number_Pipeline].append(dicc)
                    data_out=json.dumps(dicc)
                    self.conex_to_Server.publish(ROOT +"SET_MY_ID/" + code, data_out)

                elif re.search(RegEx_Get_Connect_Nodes, self.message.topic):
                    data_in=json.loads(self.message.payload)
                    dicc = data_in
                    Number_Pipeline = self.__Get_Pipeline(dicc["ID"])
                    #Get connect nodes
                    for i in dicc["List_Stages"]:
                        Previus_ID = self.__ID_Previus_Stage(Number_Pipeline, i)
                        if  Previus_ID != -1:
                            data_out=json.dumps(Previus_ID)
                            self.conex_to_Server.publish(ROOT + "ID-" + str(dicc["ID"]) + "/NEW_SUSCRIBER", data_out)
                            data_out=json.dumps(dicc["ID"])
                            self.conex_to_Server.publish(ROOT + "ID-" + str(Previus_ID) + "/NEW_PUBLISHER", data_out)

                elif re.search(Regex_Give_Info, self.message.topic):
                    data_in=json.loads(self.message.payload)
                    dicc = data_in
                    Iterator_ID = dicc["ID"]
                    self._Status_IDS[Iterator_ID] = [0, dicc] 

            self.lock_event.release()


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


    def __ID_Previus_Stage(self, pipeline, stage):
        structure = self.get_Structure()
        _ID_Previus_Node = -1
        
        for i in range(len(structure)):
            if i == pipeline:
                for w in structure[i]:
                    for x in w["List_Stages"]:
                        if x == stage-1:
                            _ID_Previus_Node = w["ID"]
        return _ID_Previus_Node
        

    def __ID_Next_Stage(self, pipeline, stage):
        structure = self.get_Structure()
        _ID_Next_Node = -1
        for i in range(len(structure)):
            if i == pipeline:
                for w in structure[i]:
                    for x in w["List_Stages"]:
                        if x == stage+1:
                            _ID_Next_Node = w["ID"]
        return _ID_Next_Node

    def __Get_Pipeline(self, Id):
        structure = self.get_Structure()
        for i in range(len(structure)):
            for w in structure[i]:
                if w["ID"] == Id:
                    return i
        return -1

    def request_info(self):
        for i in self.get_Status_IDS():
            if i[0] < 999: #999 is the counter limit
                i[0] = i[0] + 1 
        self.conex_to_Server.publish(ROOT + "REQUEST_INFO", 0)

    def delete_Id_status(self, Id):
        structure = self.get_Structure()
        for i in range(len(structure)):
            for w in structure[i]:
                if w["ID"] == Id:
                    if len(w["List_Stages"]) != 0:
                        empty_stages = w["List_Stages"]
                        w["List_Stages"] = []
                        dicc = {
                            "ID":Id,
                            "List_Stages": w["List_Stages"]
                        }
                        data_out=json.dumps(dicc)
                        self.conex_to_Server.publish(ROOT + "ID-" + str(Id) + "/UPDATE_STAGE", data_out)
                        data_out=json.dumps(-1)
                        self.conex_to_Server.publish(ROOT + "ID-" + str(Id) + "/NEW_SUSCRIBER", data_out)
                        self.conex_to_Server.publish(ROOT + "ID-" + str(Id) + "/NEW_PUBLISHER", data_out)
                        Previus_ID = self.__ID_Previus_Stage(i, empty_stages[0])
                        if Previus_ID != -1:
                            self.conex_to_Server.publish(ROOT + "ID-" + str(Previus_ID) + "/NEW_PUBLISHER", data_out)
                        ##actualizar para el siguiente quien era su anterior
                        Post_ID = self.__ID_Next_Stage(i, empty_stages[-1])
                        if Post_ID != -1:
                            self.conex_to_Server.publish(ROOT + "ID-" + str(Post_ID) + "/NEW_SUSCRIBER", data_out)
                    

        



"""
Class Slave

This class is a slave of the structure 
"""
class Slave:


    #Connection variables 
    _IP_SERVER = ""
    _PORT = -1

    # Slave MY_ID, stage, and mac
    _MY_ID = -1
    _Stages = []
    _Mac = hex(get_mac())
    _Time = datetime.now().time()

    #Nodes 
    _Previus_Node = []
    _Next_Node = []

    #Topics
    _Topics_Subscribe = []
    _Topics_Publish = []

    conex_to_Server= libmqtt.Client()
    conex = False

    # lock
    lock = allocate_lock()
    lock.acquire()

    # message
    message = None

    #event
    event = 0
    ESTABLISHED_CONEX = 1
    FAIL_CONEX = 2
    PUBLICATION_MADE = 3
    ACTIVE_SUBSCRIPTION = 4
    DATA_RECEIVED = 5

    # Constructor
    def __init__(self, pipeline, IP_SERVER, PORT):

        self.__events()

        start_new_thread(self.__eventsProcessor,())

        self._Pipeline = pipeline

        self._IP_SERVER = IP_SERVER
        self._PORT      = PORT
        self.__connect_to_server()

        #Stop code when this slave doesn't have ID
        while(self.get_MY_ID() == -1):
            self.__TakeNumber()
            print("Taking a ID from Master... ")
            sleep(10)

        MQTT_TOPIC = [(ROOT + "ID-" + str(self.get_MY_ID()) + "/" +"#",0)]
        self.conex_to_Server.subscribe(MQTT_TOPIC)
        dicc = {
            "ID":self.get_MY_ID(),
            "List_Stages":self.get_Stages()
        }
        data_out=json.dumps(dicc)
        self.conex_to_Server.publish(ROOT + "ID-" + str(self.get_MY_ID()) + "/GET_CONNECT_NODES", data_out)

        #The slave is ready to give his info 
        MQTT_TOPIC = [(ROOT + "REQUEST_INFO",0)]
        self.conex_to_Server.subscribe(MQTT_TOPIC)


    # Methods
    def get_MY_ID(self):
        return self._MY_ID

    def get_Stages(self):
        Tmp_Stages = self._Stages
        Tmp_Stages.sort()
        self._Stages = Tmp_Stages
        return self._Stages

    def get_Pipeline(self):
        return self._Pipeline

    def get_Previus_Node(self):
        return self._Previus_Node

    def get_Next_Node(self):
        return self._Next_Node

    def get_Topics_Subscribe(self):
        _Topics_Subscribe = []
        for i in self.get_Previus_Node():
            _Topics_Subscribe.append("/APPLICATION_CONTEXT/ID-" + str(i))
        return _Topics_Subscribe

    def get_Topics_Publish(self):
        _Topics_Publish = []
        for i in self.get_Next_Node():
            _Topics_Publish.append("/APPLICATION_CONTEXT/ID-" + str(i))
        return _Topics_Publish

    def get_IP_SERVER(self):
        return self._IP_SERVER

    def get_PORT(self):
        return self._PORT

    def __TakeNumber(self):
        data_out=json.dumps(self.get_Pipeline())
        self.conex_to_Server.publish(ROOT + "GET_MY_ID/" + str(self._Mac) + str(self._Time), data_out)

    def get_size(bytes, suffix="B"):
        """
        Scale bytes to its proper format
        e.g:
            1253656 => '1.20MB'
            1253656678 => '1.17GB'
        """
        factor = 1024
        for unit in ["", "K", "M", "G", "T", "P"]:
            if bytes < factor:
                return f"{bytes:.2f}{unit}{suffix}"
            bytes /= factor

    def get_Info_From_Slave(self):

        #stats Frecuency
        cpufreq = psutil.cpu_freq()
        #stats Mem
        svmem = psutil.virtual_memory()
        #stats Network
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

        #stats Battery
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


    lock_event = allocate_lock()

    def __established_connection(self, client, userdata, flags, rc):
        if rc == 0:
            #This code controls the use of the threat
            while(not self.lock.locked()):
                sleep(1)
            self.lock_event.acquire()
            self.event = self.ESTABLISHED_CONEX
            self.lock.release()


    def __established_connection(self, client, userdata, flags, rc):
        if rc == 0:
            #This code controls the use of the threat
            while(not self.lock.locked()):
                sleep(1)
            self.lock_event.acquire()
            self.event = self.ESTABLISHED_CONEX
            self.lock.release()


    def __fail_connection(self, client, *args):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        self.lock_event.acquire()
        self.event = self.FAIL_CONEX
        self.lock.release()


    def __publication_made(self, client, *args):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        self.lock_event.acquire()
        self.event = self.PUBLICATION_MADE
        self.lock.release()


    def __active_subscription(self, client, *args):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        self.lock_event.acquire()
        self.event = self.ACTIVE_SUBSCRIPTION
        self.lock.release()

    
    def __data_received(self, client, data, msg):
        #This code controls the use of the threat
        while(not self.lock.locked()):
            sleep(1)
        # lock
        self.lock_event.acquire()
        self.message = msg
        self.event = self.DATA_RECEIVED
        self.lock.release()

    def __events(self):
        # Event register
        self.conex_to_Server.on_connect = self.__established_connection
        self.conex_to_Server.on_publish = self.__publication_made
        self.conex_to_Server.on_connect_fail = self.__fail_connection
        self.conex_to_Server.on_subscribe = self.__active_subscription
        self.conex_to_Server.on_message = self.__data_received

    def __eventsProcessor(self):
        while True:
            self.lock.acquire()
            if self.event == self.FAIL_CONEX:
                print ("Error, can't connect to the server")
            elif self.event == self.ESTABLISHED_CONEX:
                self.conex = True
                MQTT_TOPIC = [(ROOT + "SET_MY_ID/" +"#",0)]
                self.conex_to_Server.subscribe(MQTT_TOPIC)
                print("ESTABLISHED_CONEX")
            elif self.event == self.PUBLICATION_MADE:
                print ("PUBLICATION_MADE")
            elif self.event == self.ACTIVE_SUBSCRIPTION:
                print ("ACTIVE_SUBSCRIPTION")
            elif self.event == self.DATA_RECEIVED:
                print (f"DATA_RECEIVED: {self.message.topic}={self.message.payload}")
                #check topic for save the number
                RegEx_Set_ID = "^"+ ROOT + "SET_MY_ID/" + str(self._Mac) + str(self._Time) + "$"#quitar time en el futuro
                RegEx_New_Subcriber = "^"+ ROOT + "ID-" + str(self.get_MY_ID()) + "/NEW_SUSCRIBER"
                RegEx_New_Publisher = "^"+ ROOT + "ID-" + str(self.get_MY_ID()) + "/NEW_PUBLISHER"
                RegEx_Update_Stage = "^"+ ROOT + "ID-" + str(self.get_MY_ID()) + "/UPDATE_STAGE"
                RegeX_Request_Info = "^"+ ROOT + "REQUEST_INFO" + "$"


                if re.search(RegEx_Set_ID, self.message.topic):
                    #assign number
                    data_in=json.loads(self.message.payload)
                    print(data_in)
                    self._MY_ID = data_in['ID']
                    self._Stages = data_in['List_Stages']

                elif re.search(RegEx_New_Subcriber, self.message.topic):
                    #assign new subcriber
                    data_in=json.loads(self.message.payload)
                    if data_in != -1:
                        self._Previus_Node.append(data_in)
                    else:
                        self._Previus_Node.clear()

                elif re.search(RegEx_New_Publisher, self.message.topic):
                    #assign next node
                    data_in=json.loads(self.message.payload)
                    if data_in != -1:
                        self._Next_Node.append(data_in)
                    else:
                        self._Next_Node.clear()

                elif re.search(RegEx_Update_Stage, self.message.topic):
                    self._Stages = []

                elif re.search(RegeX_Request_Info, self.message.topic):
                    #get info from this slave
                    dicc = self.get_Info_From_Slave()
                    data_out=json.dumps(dicc)
                    self.conex_to_Server.publish(ROOT + "ID-" + str(self.get_MY_ID()) + "/GIVE_INFO", data_out)


            self.lock_event.release()


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
