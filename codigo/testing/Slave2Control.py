#####################################################################################################
#                       Code Example to use the Slave Class (STRPLibrary)                          #
#####################################################################################################

# Libraries
import paho.mqtt.client as libmqtt
from _thread import allocate_lock, start_new_thread
from time import sleep
from random import randint
import re
import json
import math
import STRPLibrary

# Global variables
# Server
IP_SERVER = "localhost"
PORT      = 1883
conex_to_Server= libmqtt.Client()
conex = False

# Lock
lock = allocate_lock()
lock.acquire()

# Message
message = None

# Event
event = 0
ESTABLISHED_CONEX = 1
FAIL_CONEX = 2
PUBLICATION_MADE = 3
ACTIVE_SUBSCRIPTION = 4
DATA_RECEIVED = 5

# Stage Number
N_STAGE = 5

# N Pipeline
N_PIPELINE = 2

# List Stages
n_stage = []
for i in range(0, N_STAGE):
    n_stage.append(i)

# Package Number
N_PACKAGE = 0

# String application context
STR_APPLICATION_CONTEXT = "/APPLICATION_CONTEXT/"


# Func for established connection
def __established_connection(client, userdata, flags, rc):
    global lock, event
    if rc == 0:
        #This code controls the use of the threat
        while(not lock.locked()):
            sleep(1)
        event = ESTABLISHED_CONEX
        lock.release()

# Func for fail connection
def __fail_connection(client, *args):
    global lock, event
    #This code controls the use of the threat
    while(not lock.locked()):
        sleep(1)
    event = FAIL_CONEX
    lock.release()

# Func for publication made
def __publication_made(client, *args):
    global lock, event
    #This code controls the use of the threat
    while(not lock.locked()):
        sleep(1)
    event = PUBLICATION_MADE
    lock.release()

# Func for active subscription
def __active_subscription(client, *args):
    global lock, event
    #This code controls the use of the threat
    while(not lock.locked()):
        sleep(1)
    event = ACTIVE_SUBSCRIPTION
    lock.release()

# Func for data received
def __data_received(client, data, msg):
    global lock, event, message
    #This code controls the use of the threat
    while(not lock.locked()):
        sleep(1)
    # lock
    message = msg
    event = DATA_RECEIVED
    lock.release()

# Event register
conex_to_Server.on_connect = __established_connection
conex_to_Server.on_publish = __publication_made
conex_to_Server.on_connect_fail = __fail_connection
conex_to_Server.on_subscribe = __active_subscription
conex_to_Server.on_message = __data_received

# Func to process the events (previously modified)
def __eventsProcessor():
    global conex_to_Server, lock, conex, message
    while True:
        lock.acquire()
        if event == FAIL_CONEX:
            print ("Error, can't connect to the server")
        elif event == ESTABLISHED_CONEX:
            conex = True
            print("ESTABLISHED_CONEX")
        elif event == PUBLICATION_MADE:
            print ("PUBLICATION_MADE")
        elif event == ACTIVE_SUBSCRIPTION:
            print ("ACTIVE_SUBSCRIPTION")
        elif event == DATA_RECEIVED:
            print (f"DATA_RECEIVED: {message.topic}={message.payload}")
            RegEx_Its_Me = "^"+ STR_APPLICATION_CONTEXT + "ID-" + str(test.get_MY_ID())
            if re.search(RegEx_Its_Me, message.topic): #if the topic match with the Regex Its me, check stages and process
                n_stage_without_firts_stage = n_stage[1:]
                data_out = message.payload
                matches = [value for value in n_stage_without_firts_stage if value in test.get_Stages()]# Check the stage with matches
                if matches != 0:# check if its empty
                    for stage in matches:# process all stage with matches
                        print("Processing Stage " + str(stage) + "...")
                        data_out = Stages(stage, data_out)
                    if matches[-1] != N_STAGE - 1:#check if it's the last stage
                        if len(test.get_Topics_Publish()) != 0:# check if there are some topic to publish
                            conex_to_Server.publish(test.get_Topics_Publish()[-1], data_out)#the final result is published
                    else:
                        STR_N_PIPELINE = "PIPELINE-" + str(N_PIPELINE)
                        topic = STR_APPLICATION_CONTEXT + STR_N_PIPELINE + "/RESULT"
                        conex_to_Server.publish(topic, data_out)#the final result is published

# Create a thread with the func __eventsProcessor
start_new_thread(__eventsProcessor,())

# While the Application context is not connected (server MQTT), try to connect
while(not conex):
    print("connecting...")
    try:
        conex_to_Server.connect(IP_SERVER, PORT)
        conex_to_Server.loop_start()
    except:
        conex_to_Server.loop_stop(force=True)
        event = FAIL_CONEX
        lock.release()
    sleep(5)

# Stages examples
def Stages(stage, message):
    global N_PACKAGE
    # Stage 0, create iterator
    if stage == 0:
        buffer = []
        #Nº Package
        buffer.append(N_PACKAGE)
        N_PACKAGE = N_PACKAGE + 1
        buffer.append(1)
        data_out = json.dumps(buffer)
        return data_out

    # Stage 1, iterator + 1
    elif stage == 1:
        data_in = json.loads(message)
        iterator = data_in[-1]
        iterator = iterator + 1
        print("Iterator")
        print(iterator)
        data_in.pop()
        data_in.append(iterator)
        data_out = json.dumps(data_in)
        return data_out

    # Stage 2, iterator + 1
    elif stage == 2:
        data_in = json.loads(message)
        iterator = data_in[-1]
        iterator = iterator + 1
        print("Iterator")
        print(iterator)
        data_in.pop()
        data_in.append(iterator)
        data_out = json.dumps(data_in)
        return data_out

    # Stage 3, iterator + 1
    elif stage == 3:
        data_in = json.loads(message)
        iterator = data_in[-1]
        iterator = iterator + 1
        print("Iterator")
        print(iterator)
        data_in.pop()
        data_in.append(iterator)
        data_out = json.dumps(data_in)
        return data_out

    # Stage 4, iterator + 1
    elif stage == 4:
        data_in = json.loads(message)
        iterator = data_in[-1]
        iterator = iterator + 1
        print("Iterator")
        print(iterator)
        data_in.pop()
        data_in.append(iterator)
        data_out = json.dumps(data_in)
        return data_out

# Call the slave class
test = STRPLibrary.Slave(N_PIPELINE, IP_SERVER, PORT) # 0 is pipeline 0

# Initial print with some info
print("="*40, "Slave, Initial Info", "="*40)
print("My Id->" + str(test.get_MY_ID()))
print("My Pipeline->" + str(test.get_Pipeline()))
print("My Stages->" + str(test.get_Stages()))
print("IP_SERVER->" + str(test.get_IP_SERVER()))
print("Port->" + str(test.get_PORT()) + "\n")
print("="*80)

Suscribers = []

# Loop with the main program
while(True):
    # Print with some info (current stage, next id, before id, etc)
    print("="*40, "Slave, I'm living", "="*40)
    print("ID->" + str(test.get_MY_ID()))
    print("Pipeline->" + str(test.get_Pipeline()))
    print("Stages->" + str(test.get_Stages()))
    print("Previous_Node->" + str(test.get_Previous_Node()))
    print("Next_Node->" + str(test.get_Next_Node()))
    Topic = STR_APPLICATION_CONTEXT + "ID-" + str(test.get_MY_ID())

    print("Suscribe to ->")# print suscribers
    print(Topic)
    MQTT_TOPIC = [(Topic,0)]
    conex_to_Server.subscribe(MQTT_TOPIC)
    print()

    print("Current Publishers ->")# print publisher
    for i in test.get_Topics_Publish():
        print(i)
    print()

    # Check if i'm the first stage
    data_out=json.dumps([])
    if len(test.get_Stages()) != 0:#check if there are stages in this slave
        matches = [value for value in n_stage if value in test.get_Stages()]# Check the stage with matches
        if len(matches) != 0:# check if its empty
            if 0 in matches:# check if 0 is inside the matches
                for stage in matches:# process all stage with matches
                    print("Processing Stage " + str(stage) + "...")
                    data_out = Stages(stage, data_out)
                if len(test.get_Topics_Publish()) != 0:# check if there are some topic to publish
                    conex_to_Server.publish(test.get_Topics_Publish()[-1], data_out)#the final result is published

    print("="*80)

    sleep(30)

    
