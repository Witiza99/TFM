#####################################################################################################
#                       Code Example Node conex to cloud                                            #
#####################################################################################################

# Libraries
import paho.mqtt.client as libmqtt
from _thread import allocate_lock, start_new_thread
from time import sleep
import json

# Global variables
# Server
IP_SERVER = "localhost"
PORT      = 1883
conex_to_Server= libmqtt.Client()
conex = False
mqtt_ubidots_client = libmqtt.Client()
conex_ubidots = False

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

# Stage Pipelines
N_PIPELINES = 1

# String application context
STR_APPLICATION_CONTEXT = "/APPLICATION_CONTEXT/"

#test variables
accum_packets_delay = []
accum_packets_throughput = []
end = False


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


# Ubidots code
# Global variables for ubidots conex
BROKER_ENDPOINT = "industrial.api.ubidots.com"
TLS_PORT = 1883  # Secure port
MQTT_USERNAME = "BBFF-m54xl604oKGS6Z3lMqNVGrXLfkENIJ"  # Put here your Ubidots TOKEN
MQTT_PASSWORD = "BBFF-m54xl604oKGS6Z3lMqNVGrXLfkENIJ"  # Leave this in blank
TOPIC = "/v1.6/devices/prueba-stream-processing"

# Func for established connection
def on_connect_ubidots(client, userdata, flags, rc):
    global conex_ubidots  # Use global variable
    if rc == 0:
        conex_ubidots = True  

# Func for publication made
def on_publish_ubidots(client, userdata, result):
    print ("PUBLICATION_MADE")

# While ubidots is not connected (server MQTT), try to connect
def connect(mqtt_ubidots_client, mqtt_username, mqtt_password, broker_endpoint, port):
    global conex_ubidots

    if not conex_ubidots:
        mqtt_ubidots_client.username_pw_set(mqtt_username, password=mqtt_password)
        # Events ubidots for mqtt
        mqtt_ubidots_client.on_connect = on_connect_ubidots
        mqtt_ubidots_client.on_publish = on_publish_ubidots
        mqtt_ubidots_client.connect(broker_endpoint, port=port)
        mqtt_ubidots_client.loop_start()

        counter = 0

        while not conex_ubidots and counter < 5:  # Wait for connection
            print("connecting... (ubidots)")
            sleep(1)
            counter += 1

    if not conex_ubidots:
        print("Fail to connect... (ubidots)")
        return False

    return True

# Func for publish data on ubidots
def publish(mqtt_ubidots_client, topic, payload):

    try:
        mqtt_ubidots_client.publish(topic, payload)

    except Exception as e:
        print("[ERROR] Can't publish, error: {}".format(e))

# Func main for ubidots
def main(mqtt_ubidots_client, Dict):
    payload = json.dumps(Dict)
    topic = TOPIC
    
    if not connect(mqtt_ubidots_client, MQTT_USERNAME,
                   MQTT_PASSWORD, BROKER_ENDPOINT, TLS_PORT):
        return False

    publish(mqtt_ubidots_client, topic, payload)
    return True
# End ubidots code

# Func to process the events (previously modified)
def __eventsProcessor():
    global conex_to_Server, lock, conex, message, accum_packets_delay, mean_thoughput, end
    while True:
        lock.acquire()
        if event == FAIL_CONEX:
            print ("Error, can't connect to the server")
        elif event == ESTABLISHED_CONEX:
            conex = True
            # conex to the last slave
            for i in range(0, N_PIPELINES):
                STR_N_PIPELINE = "PIPELINE-" + str(i)
                topic = STR_APPLICATION_CONTEXT + STR_N_PIPELINE + "/RESULT"
                MQTT_TOPIC = [(topic,0)]
                conex_to_Server.subscribe(MQTT_TOPIC)
            print("ESTABLISHED_CONEX")
        elif event == PUBLICATION_MADE:
            print ("PUBLICATION_MADE")
        elif event == ACTIVE_SUBSCRIPTION:
            print ("ACTIVE_SUBSCRIPTION")
        elif event == DATA_RECEIVED:
            print (f"DATA_RECEIVED: {message.topic}={message.payload}")
            data_in=json.loads(message.payload)
            # message processing to send to the cloud
            print(data_in[0])
            accum_packets_delay.append(data_in[1])
            accum_packets_throughput.append(data_in[2])
            
            if len(accum_packets_delay) == 10:
                mean_delay = sum(accum_packets_delay)/10
                mean_thoughput = sum(accum_packets_throughput)/10
                print(mean_delay)
                print(mean_thoughput)
                end = True
                print(end)
            

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


# Loop with the main program
while(not end):

    sleep(15)