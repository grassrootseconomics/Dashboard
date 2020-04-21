#!/usr/bin/python
#

import csv
import time
import subprocess
import logging
from datetime import datetime

time_stamp = datetime.now().strftime('%H_%M_%S_%d_%m_%Y')
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(filename="batch_disburse"+time_stamp+".log", level=logging.INFO)
#disbursement = python sempo-cli.py --host admin.sarafu.network --port 443 -o 2 tx #idnum #amountx100
#reclamation = python sempo-cli.py --host admin.sarafu.network --port 443 reclaim 12152 10

command_dis = ['python','sempo-cli.py', '--host', 'admin.sarafu.network', '--port', '443', '-o', '2', 'tx']
command_rec = ['python','sempo-cli.py', '--host', 'admin.sarafu.network', '--port', '443', 'reclaim']
command = command_dis

# read csv file
csv_file = open("test.csv", "r")
my_csv_reader = csv.reader(csv_file, delimiter=",")
my_data_list = []
for row in my_csv_reader:
    my_data_list.append(row)
csv_file.close()

for txn in my_data_list[1:]:


    new_cmd= list(command)
    new_cmd.append(txn[0])
    new_cmd.append(txn[1])

    #new_cmd = 'tx '+txn[0]+" "+txn[1]
    time_stamp = datetime.now().strftime('%H_%M_%S_%d_%m_%Y')
    print('execute:',new_cmd, time_stamp)
    logging.info(time_stamp)
    logging.info("execute: "+str(new_cmd))
    #cmd = ['echo', new_cmd]
    #cmd = ['python',new_cmd]
    cmd = new_cmd
    #cmd = [new_cmd]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    o, e = proc.communicate()

    outputa = 'Output: ' + o.decode('ascii')
    logging.info(outputa)


    try:
        if e is not None:
            if len(e.decode('ascii')) > 1:
                errora = 'Error: ' + e.decode('ascii')
                logging.info(errora)
    except UnicodeDecodeError:
        continue



    #time.sleep(1)

