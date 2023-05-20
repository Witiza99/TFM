#libraries
import paho.mqtt.client as libmqtt
from _thread import allocate_lock, start_new_thread
from time import sleep
from random import randint
import re
import json
import math

import STRPLibrary

IP_SERVER = "localhost"
PORT      = 1883

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


def __established_connection(client, userdata, flags, rc):
    global lock, event
    if rc == 0:
        #This code controls the use of the threat
        while(not lock.locked()):
            sleep(1)
        event = ESTABLISHED_CONEX
        lock.release()


def __fail_connection(client, *args):
    global lock, event
    #This code controls the use of the threat
    while(not lock.locked()):
        sleep(1)
    event = FAIL_CONEX
    lock.release()


def __publication_made(client, *args):
    global lock, event
    #This code controls the use of the threat
    while(not lock.locked()):
        sleep(1)
    event = PUBLICATION_MADE
    lock.release()


def __active_subscription(client, *args):
    global lock, event
    #This code controls the use of the threat
    while(not lock.locked()):
        sleep(1)
    event = ACTIVE_SUBSCRIPTION
    lock.release()


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
            RegEx_Its_Me = "^"+ "/APPLICATION_CONTEXT/ID-" + str(test.get_MY_ID())
            if re.search(RegEx_Its_Me, message.topic):
                #check stage
                n_stage = [1,2]
                data_out = message.payload
                matches = [value for value in n_stage if value in test.get_Stages()]
                if matches != 0:
                    for stage in matches:
                        print("Processing Stage " + str(stage) + "...")
                        data_out = Stages(stage, data_out)
                    for i in test.get_Topics_Publish():
                        conex_to_Server.publish(i, data_out)

start_new_thread(__eventsProcessor,())

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

#stages examples
def Stages(stage, message):
    if stage == 0:
        buffer_length = 100
        buffer = []
        for i in range(0, buffer_length):
            buffer.append(randint(0, 1000) / 100.0)
        data_out = json.dumps(buffer)
        return data_out
    elif stage == 1:
        data_in = json.loads(message)
        mean = 0.0
        for n in data_in:
            mean += n
        mean = mean/len(data_in)
        print("Mean")
        print(round(mean, 2))
        data_in.append(round(mean, 2))
        data_out = json.dumps(data_in)
        return data_out
    elif stage == 2:
        data_in=json.loads(message)
        n = len(data_in) -1
        mean = data_in[n]
        variance = 0
        data_in.pop(n)
        for data in data_in:
            variance += math.pow((data - mean), 2)
        variance = variance/(n-1)
        data_in.clear()
        print("Mean")
        print(mean)
        data_in.append(mean)
        print("Variance")
        print (round(variance, 2))
        data_in.append(round(variance, 2))
        data_out = json.dumps(data_in)
        return data_out

#library usage example
test = STRPLibrary.Slave(0, IP_SERVER, PORT)
print("="*40, "Slave, Initial Info", "="*40)
print("My Id->" + str(test.get_MY_ID()))
print("My Pipeline->" + str(test.get_Pipeline()))
print("My Stages->" + str(test.get_Stages()))
print("IP_SERVER->" + str(test.get_IP_SERVER()))
print("Port->" + str(test.get_PORT()) + "\n")
print("="*80)

Suscribers = []

#main program
while(True):
    print("="*40, "Slave, I'm living", "="*40)
    print("ID->" + str(test.get_MY_ID()))
    print("Pipeline->" + str(test.get_Pipeline()))
    print("Stages->" + str(test.get_Stages()))
    print("Previus_Node->" + str(test.get_Previus_Node()))
    print("Next_Node->" + str(test.get_Next_Node()))
    """Update_Suscribers = test.get_Topics_Subscribe()
    Update_Suscribers.sort()
    Suscribers.sort()
    if Update_Suscribers != Suscribers:
        if len(Suscribers) != 0:
            try:
                for i in Suscribers:
                    conex_to_Server.unsubscribe(Suscribers)
            except:
                print("Error unsuscribe")
        for i in Update_Suscribers:
            Suscribers.append(i)
            print("Suscribe to ->" + i)
            MQTT_TOPIC = [(i,0)]
            print(MQTT_TOPIC)
            conex_to_Server.subscribe(MQTT_TOPIC)"""
    Topic = "/APPLICATION_CONTEXT/ID-" + str(test.get_MY_ID())
    print("Suscribe to ->")
    print(Topic)
    MQTT_TOPIC = [(Topic,0)]
    conex_to_Server.subscribe(MQTT_TOPIC)
    print()
    """print("Current Suscribers ->")
    for i in Suscribers:
        print(i)
    print()"""
    print("Current Publishers ->")
    for i in test.get_Topics_Publish():
        print(i)
    print()

    #check if i'm the first stage
    n_stage = [0,1,2]
    data_out=json.dumps([])
    if len(test.get_Stages()) != 0:
        matches = [value for value in n_stage if value in test.get_Stages()]
        if len(matches) != 0:
            if 0 in matches:
                for stage in matches:
                    print("Processing Stage " + str(stage) + "...")
                    data_out = Stages(stage, data_out)
                for i in test.get_Topics_Publish():
                    conex_to_Server.publish(i, data_out)

    print("="*80)

    sleep(30)

    
