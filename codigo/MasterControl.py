#####################################################################################################
#                       Code Example to use the Master Class (STRPLibrary)                          #
#####################################################################################################

# Libraries
import paho.mqtt.client as libmqtt
from time import sleep
from _thread import start_new_thread
from random import randint
import STRPLibrary

# Global variables
IP_SERVER = "localhost"
PORT      = 1883
PIPELINES = 3
STAGES = 3

# Call the master class
test = STRPLibrary.Master(PIPELINES, STAGES, IP_SERVER, PORT)

# Initial print with some info
print("="*40, "Master, Initial Info", "="*40)
print("Number of Stages->" + str(test.get_Stages()))
print("Number of Pipelines->" + str(test.get_Pipelines()))
print("Structure->" + str(test.get_Structure()))
print("IP_SERVER->" + str(test.get_IP_SERVER()))
print("Port->" + str(test.get_PORT()) + "\n")
print("="*80)
print()

# Create a thread with a beacon for request info from slaves
def beacon_request_info():
    while(True):
        test.request_info()
        sleep(15)

start_new_thread(beacon_request_info,())


# Loop with the main program
while(True):
    # Print with some info (structure, status, etc)
    print("="*40, "Master, I'm living", "="*40)
    structure = test.get_Structure()
    for i in range(len(structure)):
        print("Pipeline:" + str(i))
        for w in structure[i]:
            print("ID " + str(w['ID']) + " has the following stages " + str(w['List_Stages']))
        print()

    if len(test.get_Status_IDS()) != 0:# check if get_Status_IDS is empty
        for i in test.get_Status_IDS():
            if bool(i[1]):# check if status (inside ID) is empty
                print("Status ID -> " + str(i[1]["ID"]))
                print("Connection attempt counter -> " + str(i[0]))# higher number means worse
                print("MAX_FREQUENCY -> " + str(i[1]["MAX_FREQUENCY(MHZ)"]) + "MHZ")
                print("MIN_FREQUENCY -> " + str(i[1]["MIN_FREQUENCY(MHZ)"]) + "MHZ")
                print("CURRENT_FREQUENCY -> " + str(i[1]["CURRENT_FREQUENCY(MHZ)"]) + "MHZ")
                print("TOTAL_CPU_USAGE -> " + str(i[1]["TOTAL_CPU_USAGE(%)"]) + "%")
                print("MEMORY_PERCENTAGE -> " + str(i[1]["MEMORY_PERCENTAGE(%)"]) + "%")
                print("NETWORK_SPEED ->")
                for w in i[1]["NETWORK_SPEED(MB)"]:# print info of all NIC
                    print("NIC -> " + str(w["NIC"]))
                    print("Speed -> " + str(w["Speed"]))
                print("BATTERY_PERCENTAGE -> " + str(i[1]["BATTERY_PERCENTAGE(%)"]) + "%")
                print()

        # Here would go the code based on the decision rules system
        # If slave dont response or we can improve the performance of the network
                if i[0] > 5:# check if slave is dead, if true, the status for that id is deleted
                    test.delete_Id_status(i[1]["ID"])

    # check if some status is not assigned, trying to assign to some id
    for i in range(len(structure)):
        list_Stages_Without_Id = test.get_Stages_Without_ID(i)
        print("Status without Id for Pipeline " + str(i) + " -> " + str(list_Stages_Without_Id))
        for w in list_Stages_Without_Id:
            list_free_slaves = test.get_Free_Slaves(i)# check the free slaves
            print("There are the following slaves free -> " + str(list_free_slaves))
            n_free_slaves = len(list_free_slaves)
            if n_free_slaves > 0: # if there are some slaves, status is assigned to a free id
                test.set_Stage_To_Id(w, list_free_slaves[randint(0, n_free_slaves - 1)])
            else:
                # decision to merge with other id with status
                # test.set_Stage_To_Id()
                print("Do code for merge status with others, if exists")
            print()
    print("="*80)
    sleep(30)