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
logging.basicConfig(filename="tx_reader"+time_stamp+".log", level=logging.INFO)

# read csv file
csv_file = open("tx_all_private_all_time.csv", "r")
my_csv_reader = csv.reader(csv_file, delimiter=",")
my_data_list = []
for row in my_csv_reader:
    my_data_list.append(row)
csv_file.close()

foundWeights = 0
notFoundWeights = 0

found = 0
foundNone = 0
notFound = 0
notFoundUUIDNone = 0
print(my_data_list[0])

with open("tx_hash_review.csv", 'w', newline='') as csvfile:
    writerT = csv.writer(csvfile)

    writerT.writerow(["date", "uuid", "token_amt"])

    for txn in my_data_list[1:]:
        txhash=txn[30]
        # print('hash:', txn[30])

        if txhash.startswith("Found hash of None for uuid ="):
            foundNone+=1
        elif txhash.startswith("no hash found "):
            notFound+=1
            notFoundWeights += float(txn[29])
            print(txn[1],txn[30],txn[29])
            writerT.writerow([txn[1], txn[30], txn[29]])
        elif txhash.startswith("no hash found and uuid = None"):
            notFoundUUIDNone+=1
        elif txhash.startswith("0x"):
            found+=1
            foundWeights += float(txn[29])
        else:
            print('hash:', txn[30])


    #time.sleep(1)

print('found:',found)
print('found None:',foundNone)
print('not_found:',notFound)
print('not_found_UUIDNone:',notFoundUUIDNone)
print('found_weights:',foundWeights)
print('not_found_weights:',notFoundWeights)