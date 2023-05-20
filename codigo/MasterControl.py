#libraries
import paho.mqtt.client as libmqtt
from time import sleep

import STRPLibrary

IP_SERVER = "localhost"
PORT      = 1883

test = STRPLibrary.Master(3, 3, IP_SERVER, PORT)
print("Number of Stages->" + str(test.get_Stages()))
print("Number of Pipelines->" + str(test.get_Pipelines()))
print("Structure->" + str(test.get_Structure()))
print("IP_SERVER->" + str(test.get_IP_SERVER()))
print("Port->" + str(test.get_PORT()) + "\n")

while(True):
    print("I'm living")
    structure = test.get_Structure()
    for i in range(len(structure)):
        print("Pipeline:" + str(i))
        for w in structure[i]:
            print("ID " + str(w['ID']) + " has the following stages " + str(w['List_Stages']))
        print()
    sleep(30)