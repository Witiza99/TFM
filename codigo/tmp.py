
#Code get from https://www.thepythoncode.com/article/get-hardware-system-information-python
#Doc https://psutil.readthedocs.io/en/latest/
from __future__ import print_function

import socket

import psutil
from psutil._common import bytes2human

import psutil
import platform
from datetime import datetime

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

print("="*40, "System Information", "="*40)
uname = platform.uname()
print(f"System: {uname.system}")
print(f"Node Name: {uname.node}")
print(f"Release: {uname.release}")
print(f"Version: {uname.version}")
print(f"Machine: {uname.machine}")
print(f"Processor: {uname.processor}")

# Boot Time
print("="*40, "Boot Time", "="*40)
boot_time_timestamp = psutil.boot_time()
bt = datetime.fromtimestamp(boot_time_timestamp)
print(f"Boot Time: {bt.year}/{bt.month}/{bt.day} {bt.hour}:{bt.minute}:{bt.second}")

# let's print CPU information
print("="*40, "CPU Info", "="*40)
# number of cores
print("Physical cores:", psutil.cpu_count(logical=False))
print("Total cores:", psutil.cpu_count(logical=True))
# CPU frequencies
cpufreq = psutil.cpu_freq()
print(f"Max Frequency: {cpufreq.max:.2f}Mhz")
print(f"Min Frequency: {cpufreq.min:.2f}Mhz")
print(f"Current Frequency: {cpufreq.current:.2f}Mhz")
# CPU usage
print("CPU Usage Per Core:")
for i, percentage in enumerate(psutil.cpu_percent(percpu=True, interval=1)):
    print(f"Core {i}: {percentage}%")
print(f"Total CPU Usage: {psutil.cpu_percent()}%")

# Memory Information
print("="*40, "Memory Information", "="*40)
# get the memory details
svmem = psutil.virtual_memory()
print(f"Total: {get_size(svmem.total)}")
print(f"Available: {get_size(svmem.available)}")
print(f"Used: {get_size(svmem.used)}")
print(f"Percentage: {svmem.percent}%")
print("="*20, "SWAP", "="*20)
# get the swap memory details (if exists)
swap = psutil.swap_memory()
print(f"Total: {get_size(swap.total)}")
print(f"Free: {get_size(swap.free)}")
print(f"Used: {get_size(swap.used)}")
print(f"Percentage: {swap.percent}%")

# Memory Information
print("="*40, "Memory Information", "="*40)
# get the memory details
svmem = psutil.virtual_memory()
print(f"Total: {get_size(svmem.total)}")
print(f"Available: {get_size(svmem.available)}")
print(f"Used: {get_size(svmem.used)}")
print(f"Percentage: {svmem.percent}%")
print("="*20, "SWAP", "="*20)
# get the swap memory details (if exists)
swap = psutil.swap_memory()
print(f"Total: {get_size(swap.total)}")
print(f"Free: {get_size(swap.free)}")
print(f"Used: {get_size(swap.used)}")
print(f"Percentage: {swap.percent}%")

# Disk Information
print("="*40, "Disk Information", "="*40)
print("Partitions and Usage:")
# get all disk partitions
partitions = psutil.disk_partitions()
for partition in partitions:
    print(f"=== Device: {partition.device} ===")
    print(f"  Mountpoint: {partition.mountpoint}")
    print(f"  File system type: {partition.fstype}")
    try:
        partition_usage = psutil.disk_usage(partition.mountpoint)
    except PermissionError:
        # this can be catched due to the disk that
        # isn't ready
        continue
    print(f"  Total Size: {get_size(partition_usage.total)}")
    print(f"  Used: {get_size(partition_usage.used)}")
    print(f"  Free: {get_size(partition_usage.free)}")
    print(f"  Percentage: {partition_usage.percent}%")
# get IO statistics since boot
disk_io = psutil.disk_io_counters()
print(f"Total read: {get_size(disk_io.read_bytes)}")
print(f"Total write: {get_size(disk_io.write_bytes)}")

# Network information
print("="*40, "Network Information", "="*40)
# get all network interfaces (virtual and physical)
if_addrs = psutil.net_if_addrs()
for interface_name, interface_addresses in if_addrs.items():
    for address in interface_addresses:
        print(f"=== Interface: {interface_name} ===")
        if str(address.family) == 'AddressFamily.AF_INET':
            print(f"  IP Address: {address.address}")
            print(f"  Netmask: {address.netmask}")
            print(f"  Broadcast IP: {address.broadcast}")
        elif str(address.family) == 'AddressFamily.AF_PACKET':
            print(f"  MAC Address: {address.address}")
            print(f"  Netmask: {address.netmask}")
            print(f"  Broadcast MAC: {address.broadcast}")
# get IO statistics since boot
net_io = psutil.net_io_counters()
print(f"Total Bytes Sent: {get_size(net_io.bytes_sent)}")
print(f"Total Bytes Received: {get_size(net_io.bytes_recv)}")

#battery
battery = psutil.sensors_battery()
print(f"Battery percentage: {battery.percent}")

#temperature
temps = psutil.sensors_temperatures()
if not temps:
	print("Can't read any temperature")

else:
	for name, entries in temps.items():
		print(name)
		for entry in entries:
			print("    %-20s %s °C (high = %s °C, critical = %s °C)" % (
			entry.label or name, entry.current, entry.high,
			entry.critical))
		print()

#stats Network
"""NetSpeed = psutil.net_if_stats()
for nic in NetSpeed:
	print()
	st = NetSpeed[nic]
	print(f"Total Bytes Received: {st.speed}")"""

NetSpeed = psutil.net_if_stats()
for nic, addrs in psutil.net_if_addrs().items():
    print("%s:" % (nic))
    if nic in NetSpeed:
        st = NetSpeed[nic]
        print("speed=%sMB, mtu=%s, up=%s" % (
            st.speed, st.mtu,
            "yes" if st.isup else "no"))
    print("")


"""af_map = {
    socket.AF_INET: 'IPv4',
    socket.AF_INET6: 'IPv6',
    psutil.AF_LINK: 'MAC',
}

duplex_map = {
    psutil.NIC_DUPLEX_FULL: "full",
    psutil.NIC_DUPLEX_HALF: "half",
    psutil.NIC_DUPLEX_UNKNOWN: "?",
}

stats = psutil.net_if_stats()
io_counters = psutil.net_io_counters(pernic=True)
for nic, addrs in psutil.net_if_addrs().items():
    print("%s:" % (nic))
    if nic in stats:
        st = stats[nic]
        print("    stats          : ", end='')
        print("speed=%sMB, duplex=%s, mtu=%s, up=%s" % (
            st.speed, duplex_map[st.duplex], st.mtu,
            "yes" if st.isup else "no"))
    if nic in io_counters:
        io = io_counters[nic]
        print("    incoming       : ", end='')
        print("bytes=%s, pkts=%s, errs=%s, drops=%s" % (
            bytes2human(io.bytes_recv), io.packets_recv, io.errin,
            io.dropin))
        print("    outgoing       : ", end='')
        print("bytes=%s, pkts=%s, errs=%s, drops=%s" % (
            bytes2human(io.bytes_sent), io.packets_sent, io.errout,
            io.dropout))
    for addr in addrs:
        print("    %-4s" % af_map.get(addr.family, addr.family), end="")
        print(" address   : %s" % addr.address)
        if addr.broadcast:
            print("         broadcast : %s" % addr.broadcast)
        if addr.netmask:
            print("         netmask   : %s" % addr.netmask)
        if addr.ptp:
            print("      p2p       : %s" % addr.ptp)
    print("")"""


    """
    Master
    /CONTROL/MASTER/GET_MY_ID
    /CONTROL/MASTER/ID-X/GET_CONNECT_NODES
    /CONTROL/MASTER/ID-X/GIVE_INFO

    Slave
    /CONTROL/SLAVE/SET_MY_ID/X
    /CONTROL/SLAVE/ID-X/NEW_SUSCRIBER
    /CONTROL/SLAVE/ID-X/NEW_PUBLISHER
    /CONTROL/SLAVE/REQUEST_INFO
    /CONTROL/SLAVE/ID-X/UPDATE_STAGE

    App
    /APPLICATION_CONTEXT/ID-X

    """