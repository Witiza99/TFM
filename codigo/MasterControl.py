#libraries
import paho.mqtt.client as libmqtt
from time import sleep
from _thread import start_new_thread

import STRPLibrary

IP_SERVER = "localhost"
PORT      = 1883
PIPELINES = 3
STAGES = 3

test = STRPLibrary.Master(PIPELINES, STAGES, IP_SERVER, PORT)
print("="*40, "Master, Initial Info", "="*40)
print("Number of Stages->" + str(test.get_Stages()))
print("Number of Pipelines->" + str(test.get_Pipelines()))
print("Structure->" + str(test.get_Structure()))
print("IP_SERVER->" + str(test.get_IP_SERVER()))
print("Port->" + str(test.get_PORT()) + "\n")
print("="*80)


#create a thread with a beacon for request info from slaves
def beacon_request_info():
    while(True):
        test.request_info()
        sleep(15)

start_new_thread(beacon_request_info,())

#main program
while(True):
    print("="*40, "Master, I'm living", "="*40)
    structure = test.get_Structure()
    for i in range(len(structure)):
        print("Pipeline:" + str(i))
        for w in structure[i]:
            print("ID " + str(w['ID']) + " has the following stages " + str(w['List_Stages']))
        print()

    if len(test.get_Status_IDS()) != 0:
        for i in test.get_Status_IDS():
            if bool(i[1]):#check if status is empty
                print("Status ID -> " + str(i[1]["ID"]))
                print("Connection attempt counter -> " + str(i[0]))#higher number means worse
                print("MAX_FREQUENCY -> " + str(i[1]["MAX_FREQUENCY(MHZ)"]) + "MHZ")
                print("MIN_FREQUENCY -> " + str(i[1]["MIN_FREQUENCY(MHZ)"]) + "MHZ")
                print("CURRENT_FREQUENCY -> " + str(i[1]["CURRENT_FREQUENCY(MHZ)"]) + "MHZ")
                print("TOTAL_CPU_USAGE -> " + str(i[1]["TOTAL_CPU_USAGE(%)"]) + "%")
                print("MEMORY_PERCENTAGE -> " + str(i[1]["MEMORY_PERCENTAGE(%)"]) + "%")
                print("NETWORK_SPEED ->")
                for w in i[1]["NETWORK_SPEED(MB)"]:
                    print("NIC -> " + str(w["NIC"]))
                    print("Speed -> " + str(w["Speed"]))
                print("BATTERY_PERCENTAGE -> " + str(i[1]["BATTERY_PERCENTAGE(%)"]) + "%")
                print()

        #Here would go the code based on the decision rules system
        #If slave dont response or we can improve the performance of the network
        #create methods for modify the network (delete, etc)
                if i[0] > 5:
                    test.delete_Id_status(i[1]["ID"])#what is the next step? Adjunto el stage o stage sin id a reservas o a los vecinos
    print("="*80)
    sleep(30)