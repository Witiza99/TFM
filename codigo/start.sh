#!/bin/bash

#for py_file in $(find *.py)
#do
    #echo "$py_file"
    #gnome-terminal --execute python3.8  "$py_file" 

#done
# 1 MASTER
gnome-terminal --execute python3.8 MasterControl.py
sleep 1

# 4 SLAVES PIPELINE 0
gnome-terminal --execute python3.8 Slave0Control.py
sleep 1
gnome-terminal --execute python3.8 Slave0Control.py
sleep 1
gnome-terminal --execute python3.8 Slave0Control.py
sleep 1
gnome-terminal --execute python3.8 Slave0Control.py
sleep 1

# 2 SLAVES PIPELINE 1
gnome-terminal --execute python3.8 Slave1Control.py
sleep 1


# 3 SLAVES PIPELINE 2
gnome-terminal --execute python3.8 Slave2Control.py
sleep 1
gnome-terminal --execute python3.8 Slave2Control.py
sleep 1
gnome-terminal --execute python3.8 Slave2Control.py
sleep 1

# 1 NODECONEXCLOUD
gnome-terminal --execute python3.8 NodeConexCloud.py
sleep 1