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
MAX_LIMIT_STAGE_DEAD = 3
N_NODES_TO_START_CHECK = 4

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
                print("*"*20)
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
                print("*"*20)
                print()
                if i[0] <= MAX_LIMIT_STAGE_DEAD: # id is live, check if it is working
                    if len(test.get_stages_from_Id(i[1]["ID"])) != 0:# check if status is empty
                        if -1 in test.get_stages_from_Id(i[1]["ID"]):
                            new_stages = []
                            test.set_stages_from_Id(i[1]["ID"], new_stages)# id is ready to work again

                # Here would go the code based on the decision rules system
                # If slave dont response or we can improve the performance of the network
                else:# check if slave is dead, if true, the status for that id is deleted (5 trys)
                    if len(test.get_stages_from_Id(i[1]["ID"])) != 0:# check if status is empty
                        if (-1 in test.get_stages_from_Id(i[1]["ID"])) == False:# check if status is empty or working
                            test.delete_Id_status(i[1]["ID"])

    # Check if some status is not assigned, trying to assign to some id
    for i in range(len(structure)):
        list_Stages_Without_Id = test.get_Stages_Without_ID(i)
        print("Status without Id for Pipeline " + str(i) + " -> " + str(list_Stages_Without_Id))
        if len(list_Stages_Without_Id) != 0:#try to assign status to empty id or merge with others
            for w in list_Stages_Without_Id:
                list_free_slaves = test.get_Free_Slaves(i)# check the free slaves
                print("Trying to assign id to status -> " + str(w))
                print("There are the following slaves free -> " + str(list_free_slaves))
                n_free_slaves = len(list_free_slaves)
                if n_free_slaves > 0: # if there are some slaves, status is assigned to a free id
                    test.set_Stage_To_Id(w, list_free_slaves[randint(0, n_free_slaves - 1)])
                else:
                    # Decision to merge with other id with status
                    # Example code
                    if len(test.get_Status_IDS()) >= N_NODES_TO_START_CHECK - 1:
                        next_id = test.ID_Next_Stage(i,w)
                        if next_id != -1:
                            test.set_Stage_To_Id(w, next_id)
                        elif test.ID_Previus_Stage(i,w) != -1:
                            previus_id = test.ID_Previus_Stage(i,w)
                            test.set_Stage_To_Id(w, previus_id)
                        else:
                           print("Can't merge stages") 
                print()
        else:# try to split status in differents id
            list_id_with_differents_id = []
            for w in structure[i]:# check if there are some id with different stages
                if len(w["List_Stages"]) > 1:
                    list_id_with_differents_id.append(w)
            if len(list_id_with_differents_id) != 0:# check if it's empty
                list_free_slaves = test.get_Free_Slaves(i)# check the free slaves
                n_free_slaves = len(list_free_slaves)
                for id_value in list_id_with_differents_id:
                    if n_free_slaves != 0:
                        while(len(id_value["List_Stages"]) > 1):
                            print("primero")
                            print(id_value["List_Stages"])
                            list_free_slaves = test.get_Free_Slaves(i)# check the free slaves
                            print("There are the following slaves free -> " + str(list_free_slaves))
                            n_free_slaves = len(list_free_slaves)
                            if n_free_slaves > 0: # if there are some slaves, status is assigned to a free id
                                n_stage = id_value["List_Stages"][-1]
                                print("stage" + str(n_stage))
                                print(test.get_Stages_Without_ID(i))
                                print(id_value["List_Stages"])
                                test.delete_Stage_To_Id(n_stage, id_value["ID"])# delete stage from id
                                print(id_value["List_Stages"])
                                print(test.get_Stages_Without_ID(i))
                                print("stage" + str(n_stage))
                                test.set_Stage_To_Id(n_stage, list_free_slaves[randint(0, n_free_slaves - 1)])# put stage to free id
                                print(id_value["List_Stages"])
                                id_value["List_Stages"]
                                print("segundo")
                                print(id_value["List_Stages"])
                            else:
                                break
                    else:
                        print("There are the following slaves free -> " + str(list_free_slaves))
            else:
                print("Can't split stages")



            print()
    print("="*80)
    sleep(30)