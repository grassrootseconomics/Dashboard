# -*- coding: utf-8 -*-

import os
import psycopg2
import getopt
import sys
import copy
import csv
import time
from toolkit import Date
from datetime import timedelta
from network_viz import output_Network_Viz
# process input params
opts, _ = getopt.getopt(sys.argv[1:], 'a:h:u:p:', ['public'])

start_date = None 
end_date = None
dbname=os.environ.get('DBNAME')
dbuser=os.environ.get('DBUSER')
dbpass=os.environ.get('DBPASS')
#dbuser="postgres" #os.environ.get('DBUSER')
#dbpass="password" #os.environ.get('DBPASS')



private=True

days_ago = 30
days_ago_str = None#"Feb"
start_date = None#Date().n_days_ago(days=22+31)
end_date = None#Date().n_days_ago(days=(22))

if start_date == None:
    days_ago_str = "all_time"

#to run: python3 sarafu_user_db_test.py -a 30 -h localhost -u read_only
for o, a in opts:
    if o == '--public':
        private=False
    if o == '-a' and start_date == None:
        days_ago = int(a)
        days_ago_str = str(days_ago) + "days"
        start_date = Date().n_days_ago(days=days_ago)
        end_date = Date().today()
    if o == '-h':
        dbname=a
    if o == '-u':
        dbuser = a
    if o == '-p':
        dbpass=a

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
mpl.rcParams['figure.figsize'] = [15,10] # for square canvas
mpl.rcParams['figure.subplot.left'] = 0
mpl.rcParams['figure.subplot.bottom'] = 0
mpl.rcParams['figure.subplot.right'] = 1
mpl.rcParams['figure.subplot.top'] = 1



daysL = mdates.DayLocator()

plt.style.use('seaborn-whitegrid')

#################################################################################################################



GE_community_token_id_map = {
    1: "Gatina", 2: "Bangla", 3: "Lindi", 4: "Kangemi", 5: "Ng'ombeni", 6: "Mombasa", 7: "Grassroots",
    8: "Miyani", 9: "Olympic", 10: "Takaungu", 11: "Congo", 12: "Sarafu", 13: "Nairobi", 14: "Mkanyeni",
    15: "Mnyenzeni", 16: "Grassroots", 17: "Chigato"}

def generate_user_and_transaction_data_github_csv(txnData,userData,unique_txnData,private=False):

    headersTxPriv = ['id', 'timeset', 'transfer_subtype', 'transfer_use', 'source', 's_email', 's_first_name', 's_last_name', 's_phone', 's_comm_tkn', 's_gender', 's_location_path', 's_location_lat','s_location_lon',
               's_business_type', 's_directory', 'target', 't_email', 't_first_name', 't_last_name', 't_phone', 't_comm_tkn', 't_gender',
               't_location_path', 't_location_lat', 't_location_lon', 't_business_type', 't_directory', 't_url1','t_url2', 'tx_token', 'weight', 'tx_hash', 'type','token_name', 'token_address']


    headersTxPub = ['id', 'timeset', 'transfer_subtype', 'transfer_use','source', 's_comm_tkn', 's_gender', 's_location_path', 's_location_lat','s_location_lon',
                     's_business_type', 'target', 't_comm_tkn', 't_gender',
                    't_location_path', 't_location_lat', 't_location_lon',
                     't_business_type', 'tx_token', 'weight', 'type','token_name', 'token_address']


    headersUserPriv = ['id', 'start', 'label', 'first_name', 'last_name', 'phone', 'comm_tkn','old_POA_comm_tkn',
                       'old_POA_blockchain_address','xDAI_blockchain_address', 'gender', 'location', 'lat','lon', 'held_roles',
                       'business_type', 'directory', 'bal']


    headersUserPriv.extend(['ovol_in','ovol_out','otxns_in','otxns_out','ounique_in','ounique_out'])
    headersUserPriv.extend(['svol_in','svol_out','stxns_in','stxns_out','sunique_in','sunique_out'])


    headersUserPub = ['id', 'start', 'label', 'gender', 'location', 'lat','lon', 'held_roles',
                       'business_type', 'bal', 'xDAI_blockchain_address']

    headersUserPub.extend(['ovol_in','ovol_out','otxns_in','otxns_out','ounique_in','ounique_out'])
    headersUserPub.extend(['svol_in','svol_out','stxns_in','stxns_out','sunique_in','sunique_out'])


    headersUser=headersUserPriv
    headersTx = headersTxPriv
    filenameTx = 'tx_all_private_'+days_ago_str+'.csv'
    filenameUser = 'users_all_private_'+days_ago_str+'.csv'

    if not private:
        headersUser = headersUserPub
        headersTx = headersTxPub
        filenameTx = 'tx_all_pub_'+days_ago_str+'.csv'
        filenameUser = 'users_all_pub_'+days_ago_str+'.csv'

    print("saving all transactions to: ", filenameTx)
    print("saving all users to: ", filenameUser)



    timestr = time.strftime("%Y%m%d-%H%M%S")

    token_transactions = txnData#db_cache.select_token_transactions(start_date, end_date)

    indexR = 0

    seen_users = []
    exclude_list = []

    #if filter_staff:
    #    exclude_list = ge_wids()
    if True:
        numberTx = 1
        numberUsers = 0
        # if not os.path.exists(filename):

        with open(filenameTx, 'w',newline='') as csvfileTx, open(filenameUser, 'w',newline='') as csvfileUser:
            spamwriterTx = csv.writer(csvfileTx)
            # , delimiter=',',lineterminator='\n',                    quotechar='', quoting=csv.QUOTE_MINIMAL)
            spamwriterTx.writerow(headersTx)

            spamwriterUser = csv.writer(csvfileUser)
            # , delimiter=',',lineterminator='\n',                    quotechar='', quoting=csv.QUOTE_MINIMAL)
            spamwriterUser.writerow(headersUser)


            for user_id, user_info in userData.items():

                user_data1 = {'start': user_info['created']}
                user_data1['id'] = user_info['id']
                user_data1['label'] = user_info['id']
                user_data1['xDAI_blockchain_address'] = user_info['blockchain_address']

                user_data1['comm_tkn'] = user_info['default_currency']


                user_data1['bal'] = user_info['_balance_wei']
                if private:
                    user_data1['first_name'] = user_info['first_name']
                    user_data1['last_name'] = user_info['last_name']
                    user_data1['phone'] = user_info['_phone']
                    user_data1['directory'] = user_info.get('bio','').strip('"')
                    user_data1['old_POA_blockchain_address'] = user_info.get('GE_wallet_address','').strip('"')
                    user_data1['old_POA_comm_tkn'] = user_info.get('GE_community_token_id','')


                user_data1['gender'] = user_info.get('gender', '').strip('"')
                user_data1['location'] = user_info.get('location_path')
                user_data1['lat'] = user_info.get('location_lat')
                user_data1['lon'] = user_info.get('location_lon')
                user_data1['held_roles'] = user_info['_held_roles']
                user_data1['business_type'] = user_info['_name']
                user_data1['ovol_in'] = user_info['ovol_in']
                user_data1['ovol_out'] = user_info['ovol_out']
                user_data1['otxns_in'] = user_info['otxns_in']
                user_data1['otxns_out'] = user_info['otxns_out']
                user_data1['ounique_in'] = user_info['ounique_in']
                user_data1['ounique_out'] = user_info['ounique_out']
                user_data1['svol_in'] = user_info['svol_in']
                user_data1['svol_out'] = user_info['svol_out']
                user_data1['stxns_in'] = user_info['stxns_in']
                user_data1['stxns_out'] = user_info['stxns_out']
                user_data1['sunique_in'] = user_info['sunique_in']
                user_data1['sunique_out'] = user_info['sunique_out']
                #user_data1['confidence'] = user_info['confidence']


                numberUsers+=1

                spamwriterUser.writerow([str(user_data1[k]) for k in headersUser])

            c_idx = 0
            chunks = 10000
            tx_hash = []

            #for tnsfer_acct__id, transactions in txnData.items():
            if len(unique_txnData)>0:
                for t in unique_txnData:
                    #if t['id'] in tx_hash:  # only looking at unique data: note that without this there will be double counting on transactions
                    #    print("Error: duplicate transaction found!", t['id'])
                    #    time.sleep(1.5)
                    #    continue
                    #tx_hash.append(t['id'])

                    sender_user_id = t['sender_user_id']
                    recipient_user_id = t['recipient_user_id']

                    if sender_user_id == None:
                        sender_user_id = 1

                    if recipient_user_id == None:
                        recipient_user_id = 1

                    row_data = {'timeset': t['created']}

                    row_data['weight'] = t['_transfer_amount_wei']
                    row_data['type'] = 'directed'
                    row_data['transfer_subtype'] = t['transfer_subtype']
                    row_data['id'] = t['id']
                    row_data['label'] = t['id']
                    if private:
                        row_data['tx_hash'] = t['blockchain_task_uuid']
                    row_data['token_name'] = t['token.name']
                    row_data['authorising_user_id'] = t.get('authorising_user_id') #t['authorising_user_id']
                    row_data['token_address'] = t['token.address']
                    if t['transfer_use'] != None:
                        row_data['transfer_use'] = t['transfer_use'][0].strip('"[]')
                    else:
                        row_data['transfer_use'] = ''


                    row_data['source'] = userData[sender_user_id]['blockchain_address']
                    if True:
                    #if sender_user_id in userData.keys():
                        if private:
                            row_data['tx_token'] = t['token_id']
                            row_data['s_email'] = userData[sender_user_id].get('email')
                            row_data['s_first_name'] = userData[sender_user_id]['first_name']
                            row_data['s_last_name'] = userData[sender_user_id]['last_name']
                            row_data['s_phone'] = userData[sender_user_id]['_phone']
                            row_data['s_directory'] = userData[sender_user_id].get('bio', '').strip('"')
                    row_data['s_location_path'] = userData[sender_user_id].get('location_path')
                    row_data['s_location_lat'] = userData[sender_user_id].get('location_lat')
                    row_data['s_location_lon'] = userData[sender_user_id].get('location_lon')
                    row_data['t_location_path'] = userData[recipient_user_id].get('location_path')
                    row_data['t_location_lat'] = userData[recipient_user_id].get('location_lat')
                    row_data['t_location_lon'] = userData[recipient_user_id].get('location_lon')
                    row_data['s_gender'] = userData[sender_user_id].get('gender', '').strip('"')
                    row_data['s_comm_tkn'] = userData[sender_user_id]['default_currency']
                    row_data['s_business_type'] = userData[sender_user_id]['_name']

                    row_data['target'] = userData[recipient_user_id]['blockchain_address']
                    if True:
                    #if recipient_user_id in userData.keys():
                        if private:
                            row_data['t_email'] = userData[recipient_user_id]['email']
                            row_data['t_first_name'] = userData[recipient_user_id]['first_name']
                            row_data['t_last_name'] = userData[recipient_user_id]['last_name']
                            row_data['t_phone'] = userData[recipient_user_id]['_phone']
                            row_data['t_url1'] = userData[recipient_user_id].get('user_url')
                            row_data['t_url2'] = userData[recipient_user_id].get('user_accounts_url')
                        row_data['t_gender'] = userData[recipient_user_id].get('gender','').strip('"')
                        row_data['t_comm_tkn'] = userData[recipient_user_id]['default_currency']
                        row_data['t_business_type'] = userData[recipient_user_id]['_name']

                    #if row_data['authorising_user_id'] is not None and row_data['authorising_user_id'] != 6:
                    #    print("Row,", row_data['s_email'], row_data['authorising_user_id'], row_data['t_first_name'], row_data['t_last_name'], row_data['t_phone'], row_data['weight'] )

                    if True: #Admin ONLY row_data['authorising_user_id'] is not None and row_data['authorising_user_id'] != 6: #Admin ONLY
                        rowString = []
                        for k in headersTx:
                            if k in row_data.keys():
                                #spamwriterTx.writerow([str(row_data[k]) for k in headersTx])
                                rowString.append(str(row_data[k]))
                            else:
                                rowString.append('')

                        spamwriterTx.writerow(rowString)
                        if indexR < 3:
                            #print(row_data) #debug
                            indexR+=1
                        if c_idx >= chunks:
                            c_idx = 0
                            csvfileTx.flush()  # whenever you want
                            print("chunk: ", numberTx)
                        else:
                            c_idx += 1

                        numberTx += 1

        print("****saved all transactions to csv", filenameTx, " number of tx:", numberTx, timestr)
        print("****saved all users to csv", filenameUser, " number of User:", numberUsers, timestr)

def cumulate(y_values):
    for y in y_values.keys():
        xi = len(y_values[y]) - 2
        while xi >= 0:
            y_values[y][xi] += y_values[y][xi + 1]
            xi -= 1
    return y_values


def find_weekend_indices(datetime_array):
    indices = []
    for i in range(len(datetime_array)):
        if datetime_array[i].weekday() >= 5:
            indices.append(i)
    return indices

def find_occupied_hours(datetime_array):
    indices = []
    for i in range(len(datetime_array)):
        if datetime_array[i].weekday() < 5:
            if datetime_array[i].hour >= 7 and datetime_array[i].hour <= 19:
                indices.append(i)
    return indices

def highlight_datetimes(dates,indices, ax):
    i = 0
    while i < len(indices) - 1:
        ax.axvspan(dates[indices[i]], dates[indices[i] + 1], facecolor='blue', edgecolor='none', alpha=.2)
        i += 1

#The goal here is to identify people whoa re sending to chamas (possibly put this as a filter inside the User table)
def generate_chama_data(txnData, userData, start_date=None, end_date=None):

    return None

def generate_transaction_data_svg(txnData, userData, unique_txnData, start_date=None, end_date=None):

    days = 0
    days_str = ""
    if start_date == None:
        earlyDate = Date().n_days_ahead(days=2)
        lateDate = Date().n_days_ago(days=2000)
        nend_date = Date().today()
        for t in unique_txnData:
            if t['created'].date() < earlyDate:
                earlyDate = t['created'].date()
            if t['created'].date() > lateDate:
                lateDate = t['created'].date()
        start_date = earlyDate
        end_date = lateDate
        days = (end_date - start_date).days + 1
        days_str = "all_time"

    else:

        days = (end_date - start_date).days + 1
        days_str = str(days - 1)+"days"
    fileName = "trade_txdata_" + days_str + ".svg"

    cumu = False

    communities = list()
    token_names = ['STANDARD','DISBURSEMENT','AGENT_OUT','RECLAMATION']
    idx = 1
    communities.insert(0, 'Total')
    for to in token_names:
        communities.insert(idx, to)
        idx = idx+1
    #db = RDSDev()

    token_transactions = txnData#db_cache.select_token_transactions(start_date, end_date)

    x_values = [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days+1)]



    voltx_data = []
    numtx_data = []

    #x_values = list(range(1 + (end_date - start_date).days))
    y_numtx_values = {community: [0 for _ in x_values]
                    for community in communities
                    if community}

    y_voltx_values = {community: [0 for _ in x_values]
                      for community in communities
                      if community}

    y_reg_values = {community: [0 for _ in x_values]
                    for community in communities
                    if community}

    y_st_values = {community: [0 for _ in x_values]
                    for community in communities
                    if community}

    unknowns = set()
    tx_hash = list()

    tx_hash = []

    numToday = 0

    # get registered users
    for user_id, user_data in userData.items():

        created_date = user_data['created'].date()
        if created_date >= start_date and created_date <= end_date:
            idx = (end_date - created_date).days
            token_name = 'STANDARD'

            for sto in communities:
                if token_name == sto:
                    y_reg_values['Total'][idx] += 1  # tokens
                    y_reg_values[token_name][idx] += 1  # tokens


    # get hist stats volume and number
    if False:
        for tnsfer_acct__id, transactions in txnData.items():
            sumTx = 0
            sumVol = 0


            addedTx = False
            type_std = True
            for t in transactions:
                #token_name = t['transfer_subtype']
                token_name = 'STANDARD'
                if t['transfer_subtype'] != 'STANDARD':
                    continue

                if t['sender_transfer_account_id'] != tnsfer_acct__id:
                    continue

                date = t['created'].date()  # Date.from_timestamp(t['created'])
                idx = (end_date - date).days

                amount = int(t['_transfer_amount_wei'])



                for sto in communities:
                    if token_name == sto:
                        sumVol += amount

                        sumTx += 1

                        addedTx = True

            if addedTx:
                if sumVol> 0 and sumVol < 2000:
                    voltx_data.append(sumVol)
                if sumTx >0 and sumTx < 20:
                    numtx_data.append(sumTx)


    # get volume and number
    # for tnsfer_acct__id, transactions in txnData.items():
    if True:
        for t in unique_txnData:
            #if t['id'] in tx_hash: #no longer needed
            #    continue
            #tx_hash.append(t['id'])

            token_name = t['transfer_subtype']
            if token_name not in token_names:
                token_names.append(token_name)
            #token_name = 'STANDARD'
            #if t['transfer_subtype'] != 'STANDARD':
                #print(t['transfer_subtype'])
                #continue

            date = t['created'].date() #Date.from_timestamp(t['created'])
            idx = (end_date - date).days

            amount = int(t['_transfer_amount_wei'])



            for sto in communities:
                if token_name == sto:# and idx in y_voltx_values['Total']:
                    y_voltx_values['Total'][idx] += amount  # tokens
                    y_voltx_values[token_name][idx] += amount  # tokens

                    y_numtx_values['Total'][idx] += 1  # tokens
                    y_numtx_values[token_name][idx] += 1  # tokens

                    addedTx = True

        #if cumu:
            #y_values = cumulate(y_values)

    plt.figure(1)
    fig, axs = plt.subplots(nrows=2, ncols=1, sharex=False)

    ax0, ax2 = axs.flatten()

    #df = df[::-1]

    ax0.set_title('Transaction Volume')
    for tname in token_names:
        if tname != 'RECLAMATION':
            ax0.plot(x_values, y_voltx_values[tname][::-1], 'o-', label=tname)
        #ax0.plot(x_values, y_voltx_values["DISBURSEMENT"][::-1], 'o-', label='Volume')
    ax0.legend()
    ax0.xaxis.set_minor_locator(daysL)
    #ax0.autofmt_xdate()

    #ax1.set_title('Volume Histogram')
    #ax1.hist(voltx_data, bins=50, facecolor='g', alpha=0.75, label='Volume Hist')
    #ax1.xaxis.set_visible(True)

    ax2.set_title('Number of Standard Transactions & Number of New Users')
    ax2.plot(x_values, y_numtx_values["STANDARD"][::-1], 'o-', label='# Txns')
    ax2.plot(x_values, y_reg_values["STANDARD"][::-1], 'o-', label='# New Users')
    ax2.legend()
    ax2.xaxis.set_minor_locator(daysL)
    #ax2.autofmt_xdate()

    #branch
    #ax3.set_title('Transaction Historgram')
    #ax3.hist(numtx_data, bins=50, facecolor='g', alpha=0.75, label='Volume Hist')

    # find to be highlighted areas, see functions
    weekend_indices = find_weekend_indices(x_values)

    # highlight areas
    highlight_datetimes(x_values, weekend_indices, ax0)
    highlight_datetimes(x_values, weekend_indices, ax2)
    #highlight_datetimes(x_values, weekend_indices, axs[2])

    plt.tight_layout()


    plt.savefig(fileName)
    plt.close()
    print("****num transactions svg saved to ", fileName)


#return a list of every users transactions keyed by user
def get_txns_acct_txns(conn, eth_conn,start_date=None,end_date=None):

    offset = 0
    step = 10000
    get_hashes = False
    hashDict = {}
    if get_hashes == True:
        rows_eth_trans_all = []
        rows_eth_task_all = []

        while True:

            cmd_eth = "SELECT tran.blockchain_task_id, tran.hash, tran.updated FROM blockchain_transaction as tran"
            cmd_eth += " WHERE tran._status = 'SUCCESS' ORDER BY tran.id LIMIT " + str(step) + " OFFSET " + str(offset)

            cur_eth = eth_conn.cursor()
            cur_eth.execute(cmd_eth)
            rows_eth = cur_eth.fetchall()

            if len(rows_eth) == 0:
                break

            #print("eth bulk offset: " + str(offset) + " results " + str(len(rows_eth)))
            rows_eth_trans_all += rows_eth
            offset += step



        offset = 0
        step = 5000

        while True:  #get task uuid blockchain_taks

            cmd_eth = "SELECT task.id, task.uuid FROM blockchain_task as task"
            cmd_eth += " WHERE task.status_text = 'SUCCESS' ORDER BY task.id LIMIT " + str(step) + " OFFSET " + str(offset)

            cur_eth = eth_conn.cursor()
            cur_eth.execute(cmd_eth)
            rows_eth = cur_eth.fetchall()

            if len(rows_eth) == 0:
                break

            #print("eth bulk offset: " + str(offset) + " results " + str(len(rows_eth)))
            rows_eth_task_all += rows_eth

            offset += step



        for rows_task in rows_eth_task_all:
            amatch = False
            aMatchDate = None
            for rows_trans in rows_eth_trans_all:
                #print(rows_task[0], rows_trans[0])
                if rows_task[0] == rows_trans[0]:

                    if amatch == True:
                        print("Found a match: (",rows_task[0],") ", rows_trans[2], aMatchDate)
                        if rows_trans[2].date() > aMatchDate.date():
                            hashDict[rows_task[1]] = rows_trans[1] #matching blockchain_task.id and blockchain_transaction.blockchain_task_id
                    else:
                        hashDict[rows_task[1]] = rows_trans[1]
                        amatch = True
                        aMatchDate = rows_trans[2]

        #for row in rows_eth_all:
        #    hashDict[row[0]] = row[1]

    txDBheaders = ['id','created','sender_transfer_account_id','recipient_transfer_account_id','sender_user_id','recipient_user_id',
                   '_transfer_amount_wei','transfer_type','transfer_subtype','transfer_use','token_id', 'blockchain_task_uuid',
                   'transfer_status','authorising_user_id']

    txItems = ', '.join(["credit_transfer." + s for s in txDBheaders])
    cur = conn.cursor()

    txnDict = {}
    txDBheaders.extend(['token.name', 'token.address'])
    offset = 0
    step = 6000
    unique_tx_hash = []

    while True:#rows_eth.len()>0:

        cmd = "SELECT "+txItems+ ", token.name, token.address FROM credit_transfer JOIN token ON token.id = credit_transfer.token_id WHERE credit_transfer.transfer_status = 'COMPLETE'"
        cmd += " ORDER BY credit_transfer.id LIMIT " + str(step) +" OFFSET "+ str(offset)

        cur.execute(cmd)
        rows = cur.fetchall()
        for row in rows:
            tDict = {}
            date_good = True
            for h, r in zip(txDBheaders, row):
                if h == '_transfer_amount_wei':
                    r = r / 10 ** 18

                if h == 'blockchain_task_uuid':
                    br = r
                    if r in hashDict.keys():
                        r = hashDict[r]
                        if r == None:
                            r = "Found hash of None for uuid = " + br
                    else:
                        if r is not None:
                            r = "no hash found " + br
                        else:
                            r = "no hash found and uuid = None"

                if h == 'created' and start_date != None and end_date != None:
                    # print(r) #2020-01-25 19:13:17.731529
                    if r.date() > end_date or r.date() < start_date:
                        date_good = False

                tDict[h] = r

            if not date_good:
                continue
            unique_tx_hash.append(tDict)
            if tDict['sender_user_id'] in txnDict.keys():
                txnDict[tDict['sender_user_id']].append(tDict)
            else:
                txnDict.update({tDict['sender_user_id']: [tDict]})

            if tDict['recipient_user_id'] in txnDict.keys():
                txnDict[tDict['recipient_user_id']].append(tDict)
            else:
                txnDict.update({tDict['recipient_user_id']: [tDict]})

        if len(rows) == 0:
            break

        print(".", end=" ", flush=True)
        #print("credit bulk offset: " + str(offset) + " results " + str(len(rows)))
        offset += step

    # get all unique transactions
    if False:
        tx_hash = []
        unique_tx_hashb = []
        totalTxns = 0
        uniqueTxns = 0
        printDotsChunks = 10000
        printDots = 0
        print("Finding unique transactions . = ", printDotsChunks)
        for tnsfer_acct__id, transactions in txnDict.items():
            for t in transactions:
                totalTxns += 1
                if t['id'] not in tx_hash:  # note that without this there will be double counting on transactions
                    tx_hash.append(t['id'])
                    unique_tx_hashb.append(t)
                    uniqueTxns += 1
                printDots += 1
                # print(printDots, end=" ")
                if printDots > printDotsChunks:
                    print(".", end=" ", flush=True)
                    printDots = 0

        print("Found Original: ", len(unique_tx_hashb))
        #print("Found: ", totalTxns, " Unique: ", uniqueTxns)

    #GEt from public views
    if False:
        txPubDBheaders = ['source', 'target', 'weight', 'transfer_subtype', 'transfer_status', 'timeset', 'task_uuid']

        txPubItems = ', '.join(["renderers.tx_meta_view." + s for s in txPubDBheaders])
        curPub = conn.cursor()

        txnPubDict = {}
        offset = 0
        step = 6000

        while True:  # rows_eth.len()>0:

            cmd = "SELECT "+txPubItems+" FROM renderers.tx_meta_view "
            cmd += "WHERE renderers.tx_meta_view.transfer_status = 'COMPLETE' "
            cmd += "ORDER BY renderers.tx_meta_view.id LIMIT " + str(step) + " OFFSET " + str(offset)




            curPub.execute(cmd)
            rows = curPub.fetchall()
            for row in rows:
                tDict = {}
                date_good = True
                for h, r in zip(txPubDBheaders, row):
                    if h == 'timeset' and start_date != None and end_date != None:
                        # print(r) #2020-01-25 19:13:17.731529
                        if r.date() > end_date or r.date() < start_date:
                            date_good = False
                    tDict[h] = r
                if not date_good:
                    continue

                if tDict['source'] in txnPubDict.keys():
                    txnPubDict[tDict['source']].append(tDict)
                else:
                    txnPubDict.update({tDict['source']: [tDict]})

                if tDict['target'] in txnPubDict.keys():
                    txnPubDict[tDict['target']].append(tDict)
                else:
                    txnPubDict.update({tDict['target']: [tDict]})

            if len(rows) == 0:
                break

            print("p", end=" ", flush=True)
            #print("Public credit bulk offset: " + str(offset) + " results " + str(len(rows)))
            offset += step

        # get all unique transactions
        tx_hash = []
        unique_pub_tx_hash = []
        totalTxns = 0
        uniqueTxns = 0
        printDotsChunks = 10000
        printDots = 0
        print("Public Views Finding unique transactions . = ", printDotsChunks)
        for tnsfer_acct__id, transactions in txnPubDict.items():
            #print("transactions", transactions)
            for t in transactions:
                #print("t", t)
                totalTxns += 1
                if t['task_uuid'] not in tx_hash:  # note that without this there will be double counting on transactions
                    tx_hash.append(t['task_uuid'])
                    unique_pub_tx_hash.append(t)
                    uniqueTxns += 1
                printDots += 1
                # print(printDots, end=" ")
                if printDots > printDotsChunks:
                    print(".", end=" ", flush=True)
                    printDots = 0

        print("Found in public Views: ", totalTxns, " Unique: ", uniqueTxns)

    return {'headers':txDBheaders, 'data': txnDict, 'unique_txns': unique_tx_hash}



def get_user_info(conn,private=False):

    cur = conn.cursor()

    private_userDBheaders = ['id', 'email', 'first_name', 'last_name', '_phone', 'business_usage_id',
                             'created', '_location', 'lat', 'lng',
                             'preferred_language',
                             'is_market_enabled', '_last_seen', '_held_roles',
                             'failed_pin_attempts', 'default_currency', 'terms_accepted', 'primary_blockchain_address']

    private_custUserDBheaders = ['bio','GE_community_token_id','gender','GE_wallet_address',]


    public_userDBheaders = ['id','business_usage_id', '_location', '_held_roles',
                     'created', 'default_currency','primary_blockchain_address']
#'_location',
    public_custUserDBheaders = ['gender']


    userDBheaders =  public_userDBheaders
    custUserDBheaders = public_custUserDBheaders

    if private == True:
        userDBheaders =  private_userDBheaders
        custUserDBheaders = private_custUserDBheaders

    # select count(sender_user_id) as c, sender_user_id, u.email from credit_transfer t inner join public.user u on t.sender_user_id = u.id group by sender_user_id, u.email order by c desc ;

    userItems = ', '.join(["u." + s for s in userDBheaders])
    #custUserItems = ', '.join(["custom_attribute_user_storage."+c for c in custUserDBheaders])

    cmd = "SELECT " + userItems + ", user_transfer_account_association_table.transfer_account_id , transfer_account._balance_wei, transfer_account.blockchain_address, transfer_usage._name"
    cmd +=" FROM public.user as u"
    cmd +=" INNER JOIN user_transfer_account_association_table"
    cmd +=" ON u.id = user_transfer_account_association_table.user_id"
    cmd +=" JOIN transfer_account"
    cmd +=" ON user_transfer_account_association_table.transfer_account_id = transfer_account.id"
    cmd += " LEFT JOIN transfer_usage"
    cmd += " ON u.business_usage_id = transfer_usage.id"

    #print("\n"+cmd+"\n") #debug
    cur.execute(cmd)
    rows = cur.fetchall()

    #get user info
    userDict = {}
    userDBheaders.append("transfer_account_id")
    userDBheaders.append("_balance_wei")
    userDBheaders.append("blockchain_address")
    userDBheaders.append("_name")
    for row in rows:
        uDict = {}
        for h, r in zip(userDBheaders,row):
            if h == '_balance_wei':
                r = r / 10 ** 18
            elif h == '_held_roles':
                if r is not None:
                    r = list(r.keys())[0]
            uDict[h]=r
        userDict[uDict['id']]=uDict



    #get attributes from custom_attribute_user_storage
    for h in custUserDBheaders:
        cmd = "SELECT custom_attribute_user_storage.user_id, custom_attribute_user_storage.value FROM custom_attribute_user_storage"
        cmd += " WHERE custom_attribute_user_storage.name='"+h +"';"

        #print("get ",h," execute: ",cmd)
        cur.execute(cmd)

        rows = cur.fetchall()
        for row in rows:
            if row[0] != None:
                if row[0] in userDict.keys():
                    tDict = userDict[row[0]]
                    if h == 'GE_community_token_id':
                        tkn_name = {h: GE_community_token_id_map[int(row[1].strip('"'))]}
                        tDict.update(tkn_name)
                    else:
                        tDict.update({h: row[1]})
                    userDict[row[0]] = tDict

    headers = userDBheaders[:5]+custUserDBheaders+userDBheaders[5:]

    # get user location
    cmdLoc = "SELECT u.id, e.external_reference, lr.child_id as location_id, lr.pat as path, l.latitude, l.longitude"
    cmdLoc += " FROM user_extension_association_table ue"
    cmdLoc += " INNER JOIN location_recursive_tmp lr"
    cmdLoc += " ON ue.location_id = lr.child_id"
    cmdLoc += " INNER JOIN public.user u"
    cmdLoc += " ON u.id = ue.user_id, location l, location_external e"
    cmdLoc += " WHERE e.location_id = lr.parent_id and l.id = lr.child_id"

    cur.execute(cmdLoc)

    rows = cur.fetchall()
    for row in rows:

        if row[0] in userDict.keys():
            tDict = userDict[row[0]]
            lPath = row[3].split(", ")[1:]
            lPathStr = ",".join(lPath)
            #lPath = row[3].split(", ")[1:]
            pathParent = lPathStr
            if not private: #public stuff
                tDict.update({"location_path": lPathStr})
            else:
                tDict.update({"osmid": row[1]})
                tDict.update({"location_path": row[3]})
            tDict.update({"location_lat": row[4]})
            tDict.update({"location_lon": row[5]})
            tDict.update({"location_parent": pathParent})
            userDict[row[0]] = tDict
        else:
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!#####User not found!")


    headers = headers+['osmid','location_parent','location_path','location_lat','location_lon']
    return {'headers':headers, 'data': userDict}



##############################################################################################

if dbpass == None:
    conn = psycopg2.connect(
            f"""
            dbname=sarafu_app
            user={dbuser}
            host={dbname}
            """)

    eth_conn = psycopg2.connect(
            f"""
            dbname=eth_worker
            user={dbuser}
            host={dbname}
            """)
else:
    conn = psycopg2.connect(
        f"""
                dbname=sarafu_app
                user={dbuser}
                host={dbname}
                password={dbpass}
                """)

    eth_conn = psycopg2.connect(
            f"""
            dbname=eth_worker
            user={dbuser}
            host={dbname}
            password={dbpass}
            """)

tResult = get_txns_acct_txns(conn, eth_conn, start_date, end_date)
txHeaders = tResult['headers']
txnData = tResult['data']
unique_txnData = tResult['unique_txns']

uResult = get_user_info(conn,private=private)
userHeaders = uResult['headers']
userData = uResult['data']

#total_unique_in_out_atleast = 0
stotal_unique_txns_out_atleast = 0
stotal_unique_txns_out_atleast_group = 0

for user in userData.keys():
    volume_in = 0
    volume_out = 0
    txns_out = 0
    txns_in = 0
    unique_txns_out = 0
    unique_txns_in = 0

    svolume_in = 0
    svolume_out = 0
    stxns_out = 0
    stxns_in = 0
    sunique_txns_out = 0
    sunique_txns_in = 0

    sunique_txns_out_atleast = 0
    #stotal_unique_txns_out_atleast_group += 1
    sunique_txns_out_atleast_group = 0
    sunique_txns_in_atleast = 0



    min_size = 20

    sseenUsers = []
    seenUsers = []

    if(userData[user]['id'] in txnData.keys()):
        for trans in txnData[userData[user]['id']]:
               if trans['sender_user_id'] == userData[user]['id']:
                   if trans['transfer_subtype'] != 'STANDARD':
                        volume_out+=trans['_transfer_amount_wei']
                        txns_out+=1
                        if trans['recipient_user_id'] not in seenUsers:
                            seenUsers.append(trans['recipient_user_id'])
                            unique_txns_out+=1
                   else:
                        svolume_out+=trans['_transfer_amount_wei']
                        stxns_out+=1
                        if trans['recipient_user_id'] not in sseenUsers:
                            sseenUsers.append(trans['recipient_user_id'])
                            sunique_txns_out+=1
                            if(trans['_transfer_amount_wei']>=min_size):
                                sunique_txns_out_atleast += 1
                                stotal_unique_txns_out_atleast += 1
                                if userData[user]['_held_roles'] == "GROUP_ACCOUNT":
                                    sunique_txns_out_atleast_group += 1
                                    stotal_unique_txns_out_atleast_group += 1


               else:
                    if trans['transfer_subtype'] != 'STANDARD':
                        volume_in+=trans['_transfer_amount_wei']
                        txns_in+=1
                        if trans['sender_user_id'] not in seenUsers:
                            seenUsers.append(trans['sender_user_id'])
                            unique_txns_in+=1
                    else:
                        svolume_in+=trans['_transfer_amount_wei']
                        stxns_in+=1
                        if trans['sender_user_id'] not in sseenUsers:
                            sseenUsers.append(trans['sender_user_id'])
                            sunique_txns_in+=1
                            if(trans['_transfer_amount_wei']>=min_size):
                                sunique_txns_in_atleast+=1



    #total_unique_out_atleast += stotal_unique_txns_out_atleast


    txData = {'ovol_in':volume_in, 'ovol_out':volume_out,'otxns_in':txns_in,'otxns_out':txns_out,
              'ounique_in':unique_txns_in, 'ounique_out':unique_txns_out,
    'svol_in':svolume_in, 'svol_out':svolume_out,'stxns_in':stxns_in,'stxns_out':stxns_out,
              'sunique_in':sunique_txns_in,'sunique_out':sunique_txns_out,'sunique_in_at':sunique_txns_in_atleast,
              'sunique_out_at':sunique_txns_out_atleast,'sunique_out_at_group':sunique_txns_out_atleast_group}

    uDict = userData[user]
    uDict.update(txData)
    userData[user]=uDict

userHeaders.extend(['ovol_in','ovol_out','otxns_in','otxns_out','ounique_in','ounique_out'])
userHeaders.extend(['svol_in','svol_out','stxns_in','stxns_out','sunique_in','sunique_out','sunique_in_at','sunique_out_at','sunique_out_at_group'])

for user, data in userData.items():
    userPercentage = 0
    groupPercentage = 0
    if stotal_unique_txns_out_atleast > 0:
        userPercentage = data['sunique_out_at'] / stotal_unique_txns_out_atleast
    if stotal_unique_txns_out_atleast_group > 0:
        groupPercentage = data['sunique_out_at_group'] / stotal_unique_txns_out_atleast_group

    tDict = data
    tDict.update({'ptot_out_unique_at': userPercentage})
    tDict.update({'ptot_out_unique_at_group': groupPercentage})

    tDict.update({'user_url': "https://admin.sarafu.network/users/"+str(data['id'])})
    tDict.update({'user_accounts_url': "https://admin.sarafu.network/accounts/"+str(data['transfer_account_id'])})

    userData[user] = tDict

    in_and_out = 0
    in_and_out = data['svol_out']

    max_out = 0
    max_out = min([int(data['_balance_wei'] / 2), data['svol_out']])
    tDict = data
    tDict.update({'max_out':max_out})
    userData[user]=tDict

userHeaders.extend(['ptot_out_unique_at'])
userHeaders.extend(['ptot_out_unique_at_group'])
userHeaders.extend(['max_out'])
userHeaders.extend(['user_url'])
userHeaders.extend(['user_accounts_url'])


'''
userConfidenceDict = {}
for tid, tdata in txnData.items():
    userConfidence = 0
    numTrans=0
    userConfidencePer = 0
    for t in tdata:
        if t['transfer_subtype'] == 'STANDARD':
            recipient_tuser_id = t['recipient_transfer_account_id']
            if recipient_tuser_id==None:
                recipient_tuser_id = 1

            recipient_user_id = t['recipient_user_id']
            if recipient_user_id==None:
                recipient_user_id = 1

            if recipient_tuser_id == tid: #incomming trade
                numTrans+=1
                txnUse = t['transfer_use'][0].strip('"[]')
                reportedUse = userData[recipient_user_id]['_name']
                if txnUse==reportedUse:
                    userConfidence +=1
    if numTrans>0:
        userConfidencePer = userConfidence/numTrans
    userConfidenceDict.update({tid:userConfidencePer})

for user, data in userData.items():
    confidence = 0
    if(data['transfer_account_id'] in userConfidenceDict.keys()):
        confidence = userConfidenceDict[data['transfer_account_id']]

    tDict = data
    tDict.update({'confidence':confidence})
    userData[user]=tDict
userHeaders.extend(['confidence'])
'''

generate_user_and_transaction_data_github_csv(txnData,userData,unique_txnData,private=private)

nstart_date = start_date
nend_date = end_date


if True:
    kept_headers = []
    filename = 'sarafu_user_data_all_admin_private_'+days_ago_str+'.csv'
    if private == False:
        filename = 'sarafu_user_data_all_admin_pub_'+days_ago_str+'.csv'
    with open(filename, 'w',newline='') as csvfile:
        writerT = csv.writer(csvfile)

        writerT.writerow(userHeaders)
        #print(userHeaders) #debug
        for user_id, user_data in userData.items():
            zRow = list()
            for attr in userHeaders:
                zRow.append(str(user_data.get(attr, '')).strip('"'))
            writerT.writerow(zRow)
                #writerT.writerow([str(user_data.get(attr, '')).strip('"') for attr in userHeaders])



generate_transaction_data_svg(txnData, userData, unique_txnData, nstart_date, nend_date)
#output_Network_Viz(txnData, userData,nstart_date, nend_date,private)
