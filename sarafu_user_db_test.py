# -*- coding: utf-8 -*-

import os
import psycopg2
import getopt
import sys
import math
import decimal
import copy
import csv
import progressbar
import time
from toolkit import Date
from datetime import timedelta
import networkx as nx
from network_viz import toGraph

from network_viz import output_Network_Viz
from network_viz import get_Network_Viz_Monthly

area_names = {
    'Mukuru Nairobi': ['kayaba', 'kambi', 'mukuru', 'masai', 'hazina', 'south', 'tetra', 'tetrapak', 'ruben',
                       'kingston', 'korokocho','kingstone', 'kamongo', 'lungalunga', 'sinai', 'lungu', 'lunga lunga','owino road','seigei'],
    'Kinango Kwale': ['amani', 'bofu', 'chibuga', 'chikomani', 'chilongoni','chigojoni','chinguluni', 'chigato', 'chigale', 'chikole','chilongoni'
                      'chigojoni', 'chikomani', 'chizini','chikomeni', 'chidzuvini', 'chidzivuni', 'chikuyu', 'doti', 'dzugwe', 'dzivani',
                      'dzovuni','hanje', 'kasemeni', 'katundani', 'kibandaogo', 'kibandaongo', 'kwale', 'kinango', 'kidzuvini', 'kalalani',
                      'kafuduni', 'kaloleni', 'kilibole','lutsangani','peku', 'gona', 'guro', 'gandini',
                      'mkanyeni', 'myenzeni', 'miyenzeni','miatsiani', 'mienzeni', 'mnyenzeni', 'minyenzeni', 'miyani','mioleni',
                      'makuluni', 'mariakani','makobeni', 'madewani', 'mwangaraba', 'mwashanga', 'miloeni', 'mabesheni', 'mazeras','mazera', 'mlola',
                      'muugano', 'mabesheni', 'miatsani', 'miatsiani', 'mwache', 'mwangani', 'miguneni','nzora','nzovuni',
                      'vikinduni', 'vikolani', 'vitangani', 'viogato', 'vyogato', 'vistangani', 'yapha', 'yava', 'yowani',
                      'ziwani','majengo','matuga','vigungani','ukunda','kokotoni','mikindani'],
    'Misc Nairobi': ['nairobi', 'west', 'lindi', 'kibera', 'kibira', 'kibra', 'makina', 'soweto', 'olympic', 'kangemi','ruiru',
                'congo', 'kawangware','kwangware', 'donholm', 'dagoreti','dandora','kabete', 'sinai', 'donhom','donholm', 'huruma', 'kitengela', 'makadara',',mlolongo','kenyatta','mlolongo',
                'tassia','tasia','gatina', '56', 'industrial', 'kasarani', 'kayole', 'mathare', 'pipe', 'juja', 'uchumi','jogoo', 'umoja','thika', 'kikuyu','stadium','buru buru', 'ngong','starehe',
                'mwiki', 'fuata', 'kware', 'kabiro', 'embakassi','embakasi', 'kmoja', 'east', 'githurai', 'landi', 'langata','limuru','mathere','dagoretti','kirembe','muugano','mwiki'],
    'Misc Mombasa': ['mombasa', 'likoni', 'bangla', 'bangladesh','ngombeni','ngómbeni', 'ombeni', 'magongo', 'miritini', 'changamwe',
                        'jomvu','ohuru'],
    'Kisauni': ['bamburi','kisauni','mworoni','nyali','shanzu','bombolulu','mtopanga','mjambere','magogoni','junda','mwakirunge'],
    'Kilifi': ['kilfi','kilifi', 'mtwapa','takaungu', 'makongeni', 'mnarani', 'mnarani', 'office','g.e','ge'],
    'Nyanza': ['busia', 'nyalgunga', 'siaya', 'kisumu', 'hawinga', 'uyoma', 'mumias','homabay','migori','kusumu'],
    'Misc Rural Counties': ['makueni', 'meru', 'kisii', 'bomet', 'machakos', 'bungoma','eldoret','kakamega','kericho','kajiado','nandi','nyeri','kitui','wote','kiambu','mwea','nakuru','narok'],
    'other': ['other', 'none', 'unknown']}



area_types = {'urban': ['urban', 'nairobi', 'mombasa'], 'rural': ['rural', 'kwale', 'kinango', 'nyanza'], 'periurban' : ['kilifi', 'periurban'],
              'other' : ['other']}

# process input params 1
opts, _ = getopt.getopt(sys.argv[1:], 'a:h:u:p:', ['public'])

start_date = None 
end_date = None
dbname=os.environ.get('DBNAME')
dbuser=os.environ.get('DBUSER')
dbpass=os.environ.get('DBPASS')
#dbuser="postgres" #os.environ.get('DBUSER')
#dbpass="password" #os.environ.get('DBPASS')


# read treatment csv file
treatment_csv_file = open("treatment_reb.csv", "r")
my_tcsv_reader = csv.reader(treatment_csv_file, delimiter=",")
treatment_list = []
for row in my_tcsv_reader:
    treatment_list.append(row[0])
treatment_csv_file.close()


private=True

days_ago = 30
days_ago_str = None#"Feb"
start_date = None#Date().n_days_ago(days=22+31)
end_date = None#Date().n_days_ago(days=(2))

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

def generate_user_and_transaction_data_github_csv(txnData,userData,unique_txnData,start_date, end_date, days_ago_str, private=False, ):

    headersTxPriv = ['id', 'timeset', 'transfer_subtype', 'transfer_use', 'source', 's_email', 's_first_name', 's_last_name', 's_phone', 's_comm_tkn', 's_gender',
               's_business_type', 's_directory', 'target', 't_email', 't_first_name', 't_last_name', 't_phone', 't_comm_tkn', 't_gender',
               't_business_type', 't_directory', 't_url1','t_url2', 'tx_token', 'weight', 'tx_hash', 'type','token_name', 'token_address']


    headersTxPub = ['id', 'timeset', 'transfer_subtype', 'source',
                     'target', 'weight', 'token_name', 'token_address']


    headersUserPriv = ['id', 'start', 'first_name', 'last_name', 'phone', 'comm_tkn','old_POA_comm_tkn',
                       'old_POA_blockchain_address','xDAI_blockchain_address', 'gender', 'loc_conf', 'support_net', 'area_name','area_type', 'held_roles',
                       'business_type', 'directory', 'final_bal']


    headersUserPriv.extend(['ovol_in','ovol_out','otxns_in','otxns_out','ounique_in','ounique_out'])
    headersUserPriv.extend(['svol_in','svol_out','stxns_in','stxns_out','sunique_in','sunique_out'])


    headersUserPub = ['id', 'start', 'final_bal', 'gender', 'area_name','area_type', 'held_roles',
                       'business_type', 'old_POA_blockchain_address', 'xDAI_blockchain_address']

    headersUserPub.extend(['ovol_in','ovol_out','otxns_in','otxns_out','ounique_in','ounique_out'])
    headersUserPub.extend(['svol_in','svol_out','stxns_in','stxns_out','sunique_in','sunique_out'])


    headersUser=headersUserPriv
    headersTx = headersTxPriv
    filenameTx = './data/tx_all_private_'+start_date.strftime("%Y%m%d")+"-"+end_date.strftime("%Y%m%d")+"-"+days_ago_str+'.csv'
    filenameUser = './data/users_all_private_'+start_date.strftime("%Y%m%d")+"-"+end_date.strftime("%Y%m%d")+"-"+days_ago_str+'.csv'

    if not private:
        headersUser = headersUserPub
        headersTx = headersTxPub
        filenameTx = './data/public/tx_all_pub_'+start_date.strftime("%Y%m%d")+"-"+end_date.strftime("%Y%m%d")+"-"+days_ago_str+'.csv'
        filenameUser = './data/public/users_all_pub_'+start_date.strftime("%Y%m%d")+"-"+end_date.strftime("%Y%m%d")+"-"+days_ago_str+'.csv'

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
                #user_data1['label'] = user_info['id']
                user_data1['xDAI_blockchain_address'] = user_info['blockchain_address']
                user_data1['old_POA_blockchain_address'] = user_info.get('GE_wallet_address', '').strip('"')

                user_data1['comm_tkn'] = user_info['default_currency']


                user_data1['final_bal'] = user_info['_balance_wei']

                if private == False and user_data1['final_bal'] <0:
                    user_data1['bal'] = 0
                if private:
                    user_data1['first_name'] = user_info['first_name']
                    user_data1['last_name'] = user_info['last_name']
                    user_data1['phone'] = user_info['_phone']
                    user_data1['directory'] = user_info.get('bio','').strip('"')
                    user_data1['old_POA_blockchain_address'] = user_info.get('GE_wallet_address','').strip('"')
                    user_data1['old_POA_comm_tkn'] = user_info.get('GE_community_token_id','')


                user_data1['gender'] = user_info.get('gender', '').strip('"')
                #user_data1['location'] = user_info.get('location_path')
                user_data1['loc_conf'] = user_info.get('loc_conf')
                user_data1['support_net'] = user_info.get('support_net')
                user_data1['area_name'] = user_info.get('area_name')
                user_data1['area_type'] = user_info.get('area_type')
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
                    #row_data['label'] = t['id']
                    if private:
                        row_data['tx_hash'] = t['blockchain_task_uuid']
                        row_data['authorising_user_id'] = t.get('authorising_user_id')  # t['authorising_user_id']
                    row_data['token_name'] = t['token.name']
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
                    #row_data['s_location_path'] = userData[sender_user_id].get('location_path')
                    #row_data['s_location_lat'] = userData[sender_user_id].get('location_lat')
                    #row_data['s_location_lon'] = userData[sender_user_id].get('location_lon')
                    #row_data['t_location_path'] = userData[recipient_user_id].get('location_path')
                    #row_data['t_location_lat'] = userData[recipient_user_id].get('location_lat')
                    #row_data['t_location_lon'] = userData[recipient_user_id].get('location_lon')
                    #row_data['s_gender'] = userData[sender_user_id].get('gender', '').strip('"')
                    #row_data['s_comm_tkn'] = userData[sender_user_id]['default_currency']
                    #row_data['s_business_type'] = userData[sender_user_id]['_name']

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
                        #row_data['t_gender'] = userData[recipient_user_id].get('gender','').strip('"')
                        row_data['t_comm_tkn'] = userData[recipient_user_id]['default_currency']
                        #row_data['t_business_type'] = userData[recipient_user_id]['_name']

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


def generate_transaction_data_svg(txnData, userData, unique_txnData, start_date, end_date, days_ago_str, days_ago):
    #end_date = Date().n_days_ago(days=(2))
    days = days_ago
    days_str = days_ago_str
    fileName = "./data/trade_txdata_" +start_date.strftime("%Y%m%d")+"-"+end_date.strftime("%Y%m%d")+"-"+days_ago_str+'.png'

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

    ax0.set_title('Sarafu Transaction Volume')
    for tname in token_names:
        if tname != 'RECLAMATION' and tname != 'DISBURSEMENT' and tname != 'AGENT_OUT':
            ax0.plot(x_values, y_voltx_values[tname][::-1], 'o-', label=tname)
        #ax0.plot(x_values, y_voltx_values["DISBURSEMENT"][::-1], 'o-', label='Volume')
    ax0.legend()
    ax0.xaxis.set_minor_locator(daysL)
    #ax0.autofmt_xdate()

    #ax1.set_title('Volume Histogram')
    #ax1.hist(voltx_data, bins=50, facecolor='g', alpha=0.75, label='Volume Hist')
    #ax1.xaxis.set_visible(True)

    ax2.set_title('Number of User to User Transactions & Number of New Users')
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


def generate_location_transaction_data_svg(txnData, userData, unique_txnData, start_date, end_date, days_ago_str, days_ago):
    #end_date = Date().n_days_ago(days=(2))
    days = days_ago
    days_str = days_ago_str
    fileName = "./data/trade_loc_txdata_" +start_date.strftime("%Y%m%d")+"-"+end_date.strftime("%Y%m%d")+"-"+days_ago_str+'.png'

    cumu = False

    communities = list()

    idx = 1
    communities.insert(0, 'Total')
    for to in area_names.keys():
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

    site_users = {community: [0 for _ in x_values]
                      for community in communities
                      if community}

    found_users = {community: [[] for _ in x_values]
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
    user_counts = {}
    for user_id, user_data in userData.items():

        created_date = user_data['created'].date()
        if created_date >= start_date and created_date <= end_date:
            idx = (end_date - created_date).days

            token_name = user_data['_location']
            if token_name != None:
                token_name = token_name.lower()
            else:
                token_name = 'other'


            locFinder = get_acct_loc(token_name)

            area_name = locFinder["area_name"]
            area_type = locFinder["area_type"]

            for sto in communities:
                if area_name == sto:
                    #print("this token 3: ",token_name)
                    y_reg_values['Total'][idx] += 1  # tokens
                    y_reg_values[area_name][idx] += 1  # tokens


    # get volume and number
    # for tnsfer_acct__id, transactions in txnData.items():
    if True:
        #site_users = {}
        #site_users.update({"Total": 0})

        for t in unique_txnData:
            #if t['id'] in tx_hash: #no longer needed
            #    continue
            #tx_hash.append(t['id'])

            userId = t['sender_user_id']

            if t['transfer_subtype'] != 'STANDARD':
                continue

            date = t['created'].date()  # Date.from_timestamp(t['created'])
            idx = (end_date - date).days
            amount = int(t['_transfer_amount_wei'])

            userN = None
            if userId in userData.keys():
                userN = userData[userId]
            else:
                continue

            token_name = userN['_location']
            if token_name == None:
                token_name = 'None'
            else:
                token_name = token_name.lower()

            locFinder = get_acct_loc(token_name)

            area_name = locFinder["area_name"]
            area_type = locFinder["area_type"]

            if userId not in found_users[area_name][idx]:
                site_users[area_name][idx] = site_users[area_name][idx] + 1
                site_users['Total'][idx] = site_users['Total'][idx] + 1
                found_users[area_name][idx].append(userId)

            for sto in communities:
                if area_name == sto:# and idx in y_voltx_values['Total']:
                    #print(" adding name token ", token_name)
                    y_voltx_values['Total'][idx] += amount  # tokens
                    y_voltx_values[area_name][idx] += amount  # tokens

                    y_numtx_values['Total'][idx] += 1  # tokens
                    y_numtx_values[area_name][idx] += 1  # tokens

                    addedTx = True
            #print("user number: ",site_users[idx])
            if False:
                for sto in communities:
                        if site_users[sto][idx] > 0:
                            y_voltx_values[sto][idx] = y_voltx_values[sto][idx] / site_users[sto][idx]
                            y_numtx_values[sto][idx] =   y_numtx_values[sto][idx] / site_users[sto][idx]

                        addedTx = True

        #if cumu:
            #y_values = cumulate(y_values)

    plt.figure(1)
    fig, axs = plt.subplots(nrows=4, ncols=1, sharex=True)

    ax0, ax2, ax3 , ax4= axs.flatten()

    #df = df[::-1]

    ax0.set_title('Sarafu Transaction Volume')
    for tname in communities:
        #print("this one1: ", tname)
        if tname != 'Total':
            ax0.plot(x_values, y_voltx_values[tname][::-1], 'o-', label=tname)
        #ax0.plot(x_values, y_voltx_values["DISBURSEMENT"][::-1], 'o-', label='Volume')
    ax0.legend()
    ax0.xaxis.set_minor_locator(daysL)

    ax2.set_title('Number of User to User Transactions')
    for tname in communities:
        # print("this one1: ", tname)
        if tname != 'Total':

            #ax2.plot(x_values, site_users[tname][::-1], 'o-', label=tname)
            ax2.plot(x_values, y_numtx_values[tname][::-1], 'o-', label=tname)
            #ax2.plot(x_values, y_reg_values[tname][::-1], 'o-', label=tname+"reg")

    ax2.legend()
    ax2.xaxis.set_minor_locator(daysL)

    ax3.set_title('Number of Users per Day')
    for tname in communities:
        # print("this one1: ", tname)
        if tname != 'Total':

            ax3.plot(x_values, site_users[tname][::-1], 'o-', label=tname)
            #ax2.plot(x_values, y_numtx_values[tname][::-1], 'o-', label=tname)
            #ax2.plot(x_values, y_reg_values[tname][::-1], 'o-', label=tname+"reg")

    ax3.legend()
    ax3.xaxis.set_minor_locator(daysL)

    ax4.set_title('Number of Registrations per Day')
    for tname in communities:
        # print("this one1: ", tname)
        if tname != 'Total':

            ax4.plot(x_values, y_reg_values[tname][::-1], 'o-', label=tname)

    ax4.legend()
    ax4.xaxis.set_minor_locator(daysL)


    # find to be highlighted areas, see functions
    weekend_indices = find_weekend_indices(x_values)

    # highlight areas
    highlight_datetimes(x_values, weekend_indices, ax0)
    highlight_datetimes(x_values, weekend_indices, ax2)
    highlight_datetimes(x_values, weekend_indices, ax3)
    highlight_datetimes(x_values, weekend_indices, ax4)
    #highlight_datetimes(x_values, weekend_indices, axs[2])

    plt.tight_layout()


    plt.savefig(fileName)
    plt.close()
    print("****num transactions svg saved to ", fileName)


#return a list of every users transactions keyed by user
def get_acct_loc(user_location):
    # get registered users
    user_counts = {}
    loc_name = user_location
    area_name = ''
    area_type = ''
    if loc_name != None:
        area_name = loc_name.lower()
    else:
        area_name = 'other'

    found = False
    for name in area_names.keys():
        if found == False:
            for sub_name in area_names[name]:
                if sub_name in area_name:
                    area_name = name
                    found = True

    if found == False:
        area_name = "other"



    found = False
    for type in area_types.keys():
        if found == False:
            for sub_name in area_types[type]:
                if sub_name.lower() in area_name.lower():
                    # print(name, token_name, "found1")
                    # print("found", sub_name, " in: ", token_name, " assigned to: ", name)
                    area_type = type
                    found = True
    if found == False:
        area_type = "other"

    return {"area_name":area_name,"area_type":area_type}

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
    lastTradeOut = {}
    lastTradeIn = {}
    firstTradeIn = {}

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

            if tDict['sender_user_id'] in txnDict.keys():
                if date_good:
                    txnDict[tDict['sender_user_id']].append(tDict)

                if tDict['sender_user_id'] in lastTradeOut.keys():
                    if tDict['created'].date() > lastTradeOut[tDict['sender_user_id']].date():
                        lastTradeOut[tDict['sender_user_id']] = tDict['created']
                else:
                    lastTradeOut.update({tDict['sender_user_id']: tDict['created']})
            else:
                if date_good:
                    txnDict.update({tDict['sender_user_id']: [tDict]})
                lastTradeOut.update({tDict['sender_user_id']: tDict['created']})


            #else:
            #txnDict.update({tDict['recipient_user_id']: [tDict]})

            if tDict['recipient_user_id'] in txnDict.keys():
                if date_good:
                    txnDict[tDict['recipient_user_id']].append(tDict)
                if tDict['recipient_user_id'] in lastTradeIn.keys():
                    if tDict['created'].date() > lastTradeIn[tDict['recipient_user_id']].date():
                        lastTradeIn[tDict['recipient_user_id']] = tDict['created']
                else:
                    lastTradeIn.update({tDict['recipient_user_id']: tDict['created']})
            else:
                if date_good:
                    txnDict.update({tDict['recipient_user_id']: [tDict]})
                lastTradeIn.update({tDict['recipient_user_id']: tDict['created']})

            if tDict['recipient_user_id'] in firstTradeIn.keys():
                if tDict['created'].date() < firstTradeIn[tDict['recipient_user_id']]['created'].date():
                    if tDict['transfer_subtype'] == 'STANDARD':
                        firstTradeIn[tDict['recipient_user_id']] = tDict
            else:
                if tDict['transfer_subtype'] == 'STANDARD':
                    firstTradeIn.update({tDict['recipient_user_id']: tDict})

            if date_good:
                unique_tx_hash.append(tDict)

                #lastTradeIn.update({tDict['recipient_user_id']: tDict['created']})
                #firstTradeIn.update({tDict['recipient_user_id']: tDict})

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


    days_diff = 0
    days_str = ""
    if start_date == None or end_date == None:
        earlyDate = Date().n_days_ahead(days=2)
        lateDate = Date().n_days_ago(days=2000)
        nend_date = Date().today()
        if True: #for u, trans in txnData.items():
            for t in unique_tx_hash:
                if t['created'].date() < earlyDate:
                    earlyDate = t['created'].date()
                if t['created'].date() > lateDate:
                    lateDate = t['created'].date()
        start_date = earlyDate
        end_date = lateDate
        days_diff = (end_date - start_date).days + 1
        days_str = "all_time"

    else:

        days_diff = (end_date - start_date).days + 1
        days_str = str(days_diff - 1) + "days"

    return {'headers':txDBheaders, 'data': txnDict, 'unique_txns': unique_tx_hash, 'lastTradeOut': lastTradeOut,
            'lastTradeIn': lastTradeIn, 'firstTradeIn': firstTradeIn, 'startDate': start_date, 'endDate': end_date, 'daysStr':days_str, 'days':days_diff}



def get_user_info(conn,private=False):

    cur = conn.cursor()
    private_userDBheaders = ['id', 'email', 'first_name', 'last_name', '_phone', 'business_usage_id',
                             'created', '_location', 'lat', 'lng',
                             'preferred_language',
                             'is_market_enabled', '_deleted','_last_seen', '_held_roles',
                             'failed_pin_attempts', 'default_currency', 'terms_accepted', 'primary_blockchain_address']

    private_custUserDBheaders = ['bio','GE_community_token_id','gender','GE_wallet_address',]


    public_userDBheaders = ['id','business_usage_id', '_location', '_held_roles',
                     'created', 'default_currency','primary_blockchain_address']
#'_location',
    public_custUserDBheaders = ['gender','GE_wallet_address']


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
lastTradeOut = tResult['lastTradeOut']
lastTradeIn = tResult['lastTradeIn']
firstTradeIn = tResult['firstTradeIn']
start_date = tResult['startDate']
end_date = tResult['endDate']
days_ago_str = tResult['daysStr']
days_ago = tResult['days']

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
    sunique_txns_out_group = 0
    sunique_txns_in_group = 0
    sunique_txns_in = 0

    sunique_txns_all = 0

    #stotal_unique_txns_out_atleast_group += 1
    sunique_txns_out_atleast = 0
    sunique_txns_out_atleast_group = 0
    sunique_txns_in_atleast = 0

    min_size = 5

    sseenRecUsers = []
    seenRecUsers = []

    sseenSentUsers = []
    seenSentUsers = []

    sseenAllUsers = []

    location_name = userData[user]['_location']
    if location_name != None:
        location_name = location_name.lower()
    else:
        location_name = 'other'

    locFinder = get_acct_loc(location_name)

    area_name = locFinder["area_name"]
    area_type = locFinder["area_type"]

    if(user in txnData.keys()):
        for trans in txnData[user]:
            if trans['transfer_subtype'] == 'STANDARD':
                if trans['recipient_user_id'] not in sseenAllUsers:
                    sseenAllUsers.append(trans['recipient_user_id'])
                    sunique_txns_all += 1
                if trans['sender_user_id'] not in sseenAllUsers:
                    sseenAllUsers.append(trans['sender_user_id'])
                    sunique_txns_all += 1

            if trans['sender_user_id'] == user:
                   if trans['transfer_subtype'] != 'STANDARD':
                        volume_out+=trans['_transfer_amount_wei']
                        txns_out+=1
                        if trans['recipient_user_id'] not in seenRecUsers:
                            seenRecUsers.append(trans['recipient_user_id'])
                            unique_txns_out+=1
                   else:
                        svolume_out+=trans['_transfer_amount_wei']
                        stxns_out+=1
                        #if (user == 4082):
                        #    print(stxns_out)

                        if trans['recipient_user_id'] not in sseenRecUsers:
                            sseenRecUsers.append(trans['recipient_user_id'])
                            sunique_txns_out+=1
                            if userData[user]['_held_roles'] == "GROUP_ACCOUNT":
                                sunique_txns_out_group += 1
                            if(trans['_transfer_amount_wei']>=min_size):
                                sunique_txns_out_atleast += 1
                                if sunique_txns_out_atleast > 1:
                                    stotal_unique_txns_out_atleast += 1
                                if userData[user]['_held_roles'] == "GROUP_ACCOUNT":
                                    sunique_txns_out_atleast_group += 1
                                    if sunique_txns_out_atleast_group > 1:
                                        stotal_unique_txns_out_atleast_group += 1
            else:
                if trans['transfer_subtype'] != 'STANDARD':
                    #if user == 13488:
                    #    print("<><><><> ",trans['_transfer_amount_wei'], trans['created'])
                    volume_in+=trans['_transfer_amount_wei']
                    txns_in+=1
                    if userData[user]['_held_roles'] == "GROUP_ACCOUNT":
                        sunique_txns_in_group += 1
                    if trans['sender_user_id'] not in seenSentUsers:
                        seenSentUsers.append(trans['sender_user_id'])
                        unique_txns_in+=1
                else:
                    svolume_in+=trans['_transfer_amount_wei']
                    stxns_in+=1
                    if trans['sender_user_id'] not in sseenSentUsers:
                        sseenSentUsers.append(trans['sender_user_id'])
                        sunique_txns_in+=1
                        if(trans['_transfer_amount_wei']>=min_size):
                            sunique_txns_in_atleast+=1

    #total_unique_out_atleast += stotal_unique_txns_out_atleast


    txData = {'ovol_in':volume_in, 'ovol_out':volume_out,'otxns_in':txns_in,'otxns_out':txns_out,
              'ounique_in':unique_txns_in, 'ounique_out':unique_txns_out, 'svol_in':svolume_in, 'svol_out':svolume_out,'stxns_in':stxns_in,'stxns_out':stxns_out,
              'sunique_in':sunique_txns_in,'sunique_out':sunique_txns_out,'sunique_all':sunique_txns_all,'sunique_out_group':sunique_txns_out_group,'sunique_in_group':sunique_txns_in_group,'sunique_in_at':sunique_txns_in_atleast,
              'sunique_out_at':sunique_txns_out_atleast,'sunique_out_at_group':sunique_txns_out_atleast_group,'area_name':area_name,'area_type':area_type}

    uDict = userData[user]
    uDict.update(txData)
    userData[user]=uDict

userHeaders.extend(['area_name','area_type','ovol_in','ovol_out','otxns_in','otxns_out','ounique_in','ounique_out'])
userHeaders.extend(['svol_in','svol_out','stxns_in','stxns_out','sunique_in','sunique_out','sunique_all'])

print("..")
print("..Creating Graph....")
G = toGraph(unique_txnData, userData, start_date, end_date,private)

print("Calculating Clusters....")
Gc = nx.clustering(G)
bar = progressbar.ProgressBar(maxval=len(list(G.nodes)), \
    widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
bar.start()
sindx = 1
if days_ago < 30:
    for usr in list(G.nodes):
    #for usr, val in Gc.items():
        if usr in userData.keys():
            #print("*", end=" ", flush=True)
            Gnode = nx.ego_graph(G, usr, 3)
            clustering = nx.average_clustering(Gnode)
            tDict = userData[usr]
            #tDict.update({'cluster_coef': val})
            tDict.update({'cluster_coef': clustering})
            userData[user] = tDict
            bar.update(sindx)
            sindx+=1
else:
    #for usr in list(G.nodes):
    for usr, val in Gc.items():
        if usr in userData.keys():
            #print("*", end=" ", flush=True)
            #Gnode = nx.ego_graph(G, usr, 3)
            #clustering = nx.average_clustering(Gnode)
            tDict = userData[usr]
            tDict.update({'cluster_coef': val})
            #tDict.update({'cluster_coef': clustering})
            userData[user] = tDict
            bar.update(sindx)
            sindx+=1
bar.finish()

min_group_balance = 0
totalRank = 0
totalRank_group = 0

for user, data in userData.items():
    userPercentage = 0
    groupPercentage = 0
    unique_user_x_clustering = 0
    unique_group_x_clustering = 0

    if 'cluster_coef' in data.keys():
        unique_user_x_clustering = min(data['sunique_out'], data['sunique_in']) * data['cluster_coef']
        unique_group_x_clustering = min(data['sunique_out_group'],data['sunique_in_group']) * data['cluster_coef']
        totalRank += unique_user_x_clustering
        if data['_balance_wei']>=min_group_balance:
            totalRank_group += unique_group_x_clustering

    if stotal_unique_txns_out_atleast > 0:
        if data['sunique_out_at'] > 1:
            userPercentage = data['sunique_out_at'] / stotal_unique_txns_out_atleast

    if stotal_unique_txns_out_atleast_group > 0:
        if data['sunique_out_at_group'] > 1:
            groupPercentage = data['sunique_out_at_group'] / stotal_unique_txns_out_atleast_group



    tDict = data

    days_enrolled = (Date.today() - data['created'].date()).days
    #if days_enrolled < 10:
    #    print("Today: ", Date.today(), " created: ", data['created'], " days: ", days_enrolled)
    tDict.update({'days_enrolled': days_enrolled})

    in_treatment = False
    if data['blockchain_address'] in treatment_list:
        in_treatment = True

    tDict.update({'treat': in_treatment})
    tDict.update({'days_enrolled': days_enrolled})

    tDict.update({'unique_x_clustering': unique_user_x_clustering})
    tDict.update({'unique_out_group_x_clustering': unique_group_x_clustering})

    tDict.update({'user_url': "https://admin.sarafu.network/users/"+str(data['id'])})
    tDict.update({'user_accounts_url': "https://admin.sarafu.network/accounts/"+str(data['transfer_account_id'])})

    userData[user] = tDict

    #in_and_out = 0
    #in_and_out = data['svol_out']


    days_since = 0
    days_since_recieved = 0
    if user in lastTradeOut.keys():
        tDict.update({'last_trade_out': lastTradeOut[user]})
        days_since = (Date.today() - lastTradeOut[user].date()).days
        tDict.update({'last_trade_out_days': days_since})
    else:
        tDict.update({'last_trade_out': userData[user]['created']})
        days_since = (Date.today() - userData[user]['created'].date()).days
        tDict.update({'last_trade_out_days': days_since})
    if user in lastTradeIn.keys():
        tDict.update({'last_trade_in': lastTradeIn[user]})
        # we want the 1st trade into them that is not an admin
    else:
        tDict.update({'last_trade_in': 'None'})



    if user in firstTradeIn.keys():
        if start_date != None:
            if userData[user]['created'].date()>=start_date:
                sender_id = firstTradeIn[user]['sender_user_id']
                tDict.update({'first_trade_in_user': sender_id})
                #tDict.update({'first_trade_in_user': userData[user]['first_name']})
                tDict.update({'first_trade_in_time': firstTradeIn[user]['created']})
                if sender_id in userData.keys():
                    tDict.update({'first_trade_in_role': userData[sender_id]['_held_roles']})
                    tDict.update({'first_trade_in_sphone': userData[sender_id].get('_phone','')})
                    tDict.update({'first_trade_in_tphone': userData[user].get('_phone','')})
                else:
                    print("no way dude", firstTradeIn[user])
        else:
            sender_id = firstTradeIn[user]['sender_user_id']
            tDict.update({'first_trade_in_user': sender_id})
            tDict.update({'first_trade_in_time': firstTradeIn[user]['created']})
            if sender_id in userData.keys():
                tDict.update({'first_trade_in_role': userData[sender_id]['_held_roles']})
                tDict.update({'first_trade_in_sphone': userData[sender_id].get('_phone','')})
                tDict.update({'first_trade_in_tphone': userData[user].get('_phone','')})
            else:
                print("no way dude", firstTradeIn[user])


        # we want the 1st trade into them that is not an admin
    else:
        tDict.update({'first_trade_in_user': 'None'})
        tDict.update({'first_trade_in_time': 'None'})
        tDict.update({'first_trade_in_role': 'None'})
        tDict.update({'first_trade_in_sphone': 'None'})
        tDict.update({'first_trade_in_tphone': 'None'})

    max_fee = 0
    min_fee = 0
    weekly_balance_fee_per = 0.02
    weekly_balance_fee = int(data['_balance_wei'] * decimal.Decimal(weekly_balance_fee_per))

    weekly_dormant_fee = 20
    dormant_fee = 0
    if days_since > 0 :
        dormant_fee = int(weekly_dormant_fee*math.floor(days_since/7))
    # tDict = data
    max_fee = max(weekly_balance_fee,weekly_balance_fee)

    if days_enrolled <=7:
        max_fee = 0

    if data['_balance_wei'] > 0:
        min_fee = min(data['_balance_wei'],max_fee)

    tDict.update({'weekly_balance_fee': weekly_balance_fee})
    tDict.update({'dormant_fee': dormant_fee})
    tDict.update({'min_fee': max_fee})

    userData[user]=tDict

total_user_reward = 300000
total_group_reward = 500000
max_user_reward = 0
max_group_reward = 0
reward_msg_list = []
fee_msg_list = []
disbursment_list = []
reclamation_list = []

for user, data in userData.items():
    max_group_reward = 0
    punique_group_x_clustering = 0
    punique_user_x_clustering = data['unique_x_clustering'] /totalRank
    if data['_balance_wei']>=min_group_balance and totalRank_group >0:
        punique_group_x_clustering = data['unique_out_group_x_clustering'] / totalRank_group
        max_group_reward = int(total_group_reward * punique_group_x_clustering)

    max_user_reward = int(total_user_reward * punique_user_x_clustering)

    reward_only = int(max_user_reward)
    reward_only_msg = ""

    fee_only = int(data['min_fee'])
    fee_only_msg = ""

    if reward_only > 0:
        if 'preferred_language' in data.keys():
            if data['preferred_language'] == 'sw':
                reward_only_msg = "Hongera! Umepokea tuzo la "+str(reward_only)+" Sarafu zaidi. Kumbuka zitapunguzwa kila mwisho wa mwezi! Bonyeza *483*46# au piga 0757628885 usadike"
            else:
                reward_only_msg = "Congratulations! You have received a "+str(reward_only)+" Sarafu bonus. Remember there will be a monthly deduction. Dial *483*46# to use or call 0757628885 for help"
        else:
            reward_only_msg = "Hongera! Umepokea tuzo la "+str(reward_only)+" Sarafu zaidi. Kumbuka zitapunguzwa kila mwisho wa mwezi! Bonyeza *483*46# au piga 0757628885 usadike"
        if data.get('_deleted',None) == None and data.get('_phone',None) != None:
            if len(data['_phone']) == 13:
                reward_msg_list.append({'phone': data['_phone'], 'msg': reward_only_msg})
        if data.get('_deleted',None) == None:
            if data['_held_roles'] == 'GROUP_ACCOUNT' or data['_held_roles'] == 'BENEFICIARY':
                disbursment_list.append({'id': data['id'], 'amt': reward_only*100})
    if fee_only > 0:
        if 'preferred_language' in data.keys():
            if data['preferred_language'] == 'sw':
                fee_only_msg = "2% zitatolewa kutoka kwa salio lako kila mwezi kusaidia wanaohitaji! Umepeana Sarafu "+str(fee_only)+"! Bonyeza *483*46# au piga 0757628885 usadike"
            else:
                fee_only_msg = "2% of your balance is donated monthly to needy users! You've just donated "+str(fee_only)+" Sarafu! Dial *483*46# or call 0757628885 for help"
        else:
            fee_only_msg = "2% zitatolewa kutoka kwa salio lako kila mwezi kusaidia wanaohitaji! Umepeana Sarafu " + str(fee_only) + "! Bonyeza *483*46# au piga 0757628885 usadike"
        if data.get('_deleted',None) == None and data.get('_phone',None) != None:
            if len(data['_phone']) == 13:
                fee_msg_list.append({'phone': data['_phone'], 'msg': fee_only_msg})
        if data['_held_roles'] == 'GROUP_ACCOUNT' or data['_held_roles'] == 'BENEFICIARY':
            reclamation_list.append({'id': data['id'], 'amt': fee_only*100})

    tDict = data

    tDict.update({'punique_x_clustering': punique_user_x_clustering})
    tDict.update({'punique_group_x_clustering': punique_group_x_clustering})
    tDict.update({'user_reward': max_user_reward})
    tDict.update({'group_reward': max_group_reward})
    tDict.update({'user_fee': fee_only})

    userData[user] = tDict

#userHeaders.extend(['ptot_out_unique_at'])
userHeaders.extend(['cluster_coef'])
if private == True:
    userHeaders.extend(['unique_x_clustering'])
    userHeaders.extend(['punique_x_clustering'])
    userHeaders.extend(['punique_group_x_clustering'])
    userHeaders.extend(['user_reward'])
    userHeaders.extend(['user_fee'])
    userHeaders.extend(['punique_group_x_clustering'])
    userHeaders.extend(['group_reward'])
    userHeaders.extend(['user_url'])
    userHeaders.extend(['user_accounts_url'])

userHeaders.extend(['last_trade_out'])
userHeaders.extend(['last_trade_out_days'])
userHeaders.extend(['days_enrolled'])

if private == True:
    userHeaders.extend(['min_fee'])
    userHeaders.extend(['weekly_balance_fee'])
    userHeaders.extend(['dormant_fee'])
    userHeaders.extend(['treat'])

userHeaders.extend(['last_trade_in'])
if private == True:
    userHeaders.extend(['first_trade_in_user'])
    userHeaders.extend(['first_trade_in_role'])
    userHeaders.extend(['first_trade_in_time'])
    userHeaders.extend(['first_trade_in_sphone'])
    userHeaders.extend(['first_trade_in_tphone'])
    userHeaders.extend(['reward_fee'])
    userHeaders.extend(['reward_fee_msg'])



#support network
print(",.,.,.,.,.Calculating Support Network Strength")
supportNet = {} #for each user calculate their support network {user:{trader:amount}}
for tid, tdata in txnData.items(): #or pick a user
    if tid in userData.keys():
        buddyTradeVol = {}
        for t in tdata:
            if t['transfer_subtype'] == 'STANDARD':
                recipient_user_id = t['recipient_user_id']
                sender_user_id = t['sender_user_id']

                if recipient_user_id == None:
                    continue
                if sender_user_id == None:
                    continue

                otherId = sender_user_id
                if sender_user_id == tid:  # outgoing trade
                    otherId = recipient_user_id
                else:
                    continue

                volume = t['_transfer_amount_wei']
                if otherId not in buddyTradeVol.keys():
                    buddyTradeVol[otherId] = volume
                else:
                    buddyTradeVol[otherId] = buddyTradeVol[otherId] + volume
        supportNet[tid]= buddyTradeVol

supportRank = {}

for tid in supportNet.keys():
    volOut = 0
    for bud in supportNet[tid].keys():
        spentOnBud = supportNet[tid][bud]
        volOutBud = userData[bud]['svol_out']
        if volOutBud >= spentOnBud:
            volOut += spentOnBud
        if volOutBud < spentOnBud:
            volOut += volOutBud

        #look at the buddies of the buddy recursive
        #if bud in supportNet.keys():


    supportRank[tid]=volOut

    tDict = userData[tid]
    tDict.update({'support_net': volOut})
    userData[tid] = tDict

userHeaders.extend(['support_net'])

#location confidence
print("Calculating Location Confidence")
locConfidenceDict = {}

for tid, tdata in txnData.items():
    if tid in userData.keys():
        if userData[tid]['area_name'] == 'other':
            locConfidence = 0
            numBuddies=0
            locConfidencePer = 0
            buddyLocs = {}
            for t in tdata:
                if t['transfer_subtype'] == 'STANDARD':

                    recipient_user_id = t['recipient_user_id']
                    sender_user_id = t['sender_user_id']

                    if recipient_user_id==None:
                        continue
                    if sender_user_id==None:
                        continue

                    otherId = sender_user_id
                    if sender_user_id == tid:  # incomming trade
                        otherId = recipient_user_id

                    otherLoc = userData[otherId]["area_name"]
                    otherLocType = userData[otherId]["area_type"]
                    if otherLoc != 'other':
                        numBuddies += 1
                        if (otherLoc,otherLocType) not in buddyLocs.keys():
                            buddyLocs[(otherLoc,otherLocType)]=1
                        else:
                            buddyLocs[(otherLoc,otherLocType)] = buddyLocs[(otherLoc,otherLocType)]+1


            bestBuddy = None

            bestBuddyConf = -1

            if numBuddies > 0:
                for buddy in buddyLocs.keys():
                    buddyLocs[buddy] = buddyLocs[buddy]/numBuddies
                    if buddyLocs[buddy] > bestBuddyConf:
                        bestBuddyConf = buddyLocs[buddy]
                        bestBuddy = buddy


            if bestBuddyConf > 0:
                locConfidenceDict.update({tid:{'area_name':bestBuddy[0],'area_type':bestBuddy[1],'conf':bestBuddyConf}})

for user, data in userData.items():
    confidence = 1

    if(user in locConfidenceDict.keys()):
        confidence = locConfidenceDict[user]['conf']
        area = locConfidenceDict[user]['area_name']
        area_type = locConfidenceDict[user]['area_type']
        data['area_name'] = area
        data['area_type'] = area_type
    else:
        if data['area_name'] == 'other':
            confidence = 0

    tDict = data
    tDict.update({'loc_conf':confidence})
    userData[user]=tDict

userHeaders.extend(['loc_conf'])

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

generate_user_and_transaction_data_github_csv(txnData,userData,unique_txnData,start_date,end_date,days_ago_str,private=private)
stff_list = []
#stff_list = ['+254727806655']

stff_hash = {}
stff_bal_hash = {}
airtime_reward = []

if True:

    newHeaders = ['id', 'first_name', 'last_name', '_phone', 'bio', '_name', 'gender', 'created', '_location', 'loc_conf', 'support_net', 'trade_bal', 'area_name','area_type','_held_roles',
                  'preferred_language', 'is_market_enabled', 'failed_pin_attempts', '_balance_wei', 'ovol_in', 'ovol_out', 'otxns_in',
                  'otxns_out', 'ounique_in', 'ounique_out', 'svol_in', 'svol_out',
                  'stxns_in', 'stxns_out', 'sunique_in', 'sunique_out','sunique_all', 'cluster_coef', 'punique_x_clustering', 'user_reward', 'punique_group_x_clustering', 'group_reward', 'last_trade_out',
                  'last_trade_out_days', 'days_enrolled', 'first_trade_in_user', 'first_trade_in_time',
                  'first_trade_in_sphone', 'user_url', 'user_accounts_url']

    kept_headers = []
    filename = './data/sarafu_user_data_all_admin_private_'+start_date.strftime("%Y%m%d")+"-"+end_date.strftime("%Y%m%d")+"-"+days_ago_str+'.csv'
    if private == False:
        filename = './data/sarafu_user_data_all_admin_pub_'+start_date.strftime("%Y%m%d")+"-"+end_date.strftime("%Y%m%d")+"-"+days_ago_str+'.csv'



    with open(filename, 'w',newline='') as csvfile:
        writerT = csv.writer(csvfile)

        writerT.writerow(newHeaders)
        #print(userHeaders) #debug
        for user_id, user_data in userData.items():

            zRow = list()
            for attr in newHeaders:
                zRow.append(str(user_data.get(attr, '')).strip('"'))
            writerT.writerow(zRow)



            trades_out = user_data.get('stxns_out',0)
            user_phone = user_data.get('_phone', str('a'))
            user_balance = user_data.get('_balance_wei', 0)
            user_id = user_data.get('id', 0)
            if user_phone != None:
                if len(user_phone) == 13 and trades_out >= 5:
                    airtime_amt = trades_out + 5
                    if airtime_amt > 50:
                        airtime_amt = 50
                    lang = user_data.get('preferred_language', '')
                    trade_out_msg = ''
                    if lang == 'sw':
                        trade_out_msg= "Happy December Airtime bonus! Umepokea tuzo la "+str(airtime_amt)+" bob airtime sababu umetumia Sarafu. Bonyeza *483*46# au piga 0757628885 usadike"
                    else:
                        trade_out_msg = "Furahia Decemba na Airtime bonus! You have received "+str(airtime_amt)+" bob Airtime because of your Sarafu usage. Dial *483*46# to use or call 0757628885 for help"

                    airtime_reward.append({"id":user_id, "user_phone":user_phone, "airtime_amt":airtime_amt, "trade_out_msg":trade_out_msg, "balance":user_balance})

            # Give the list of stats for all users that have traded with that user in or out.
            # use the users list
            # Give the stats for each user that stff member has ever traded with
            userFnd = False
            if user_data.get('_phone',None) != None:

                if user_data['_phone'] in stff_list:
                    #print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>in the list")
                    userFnd = True
                    user_found = []
                    user_found_data = []
                    user_trade_bal = {}
                    for t in txnData.get(user_data['id'],[]):
                        sender_user_id = t.get('sender_user_id',4)
                        recipient_user_id = t.get('recipient_user_id',4)

                        if sender_user_id not in user_found:
                            if sender_user_id in userData.keys():
                                if userData[sender_user_id]['_held_roles'] != 'ADMIN':
                                    user_found.append(sender_user_id)
                                    user_found_data.append(userData.get(sender_user_id,None))
                                    user_trade_bal[sender_user_id] = -1*t['_transfer_amount_wei']
                                    #print(userData.get(sender_user_id, None))
                                    #print("sender", sender_user_id, user_trade_bal[sender_user_id])
                        else:
                            user_trade_bal[sender_user_id] = user_trade_bal[sender_user_id] - t['_transfer_amount_wei']
                            #print("sender2", sender_user_id, user_trade_bal[sender_user_id])

                        if recipient_user_id not in user_found:
                            if recipient_user_id in userData.keys():
                                    user_found.append(recipient_user_id)
                                    user_found_data.append(userData.get(recipient_user_id,None))
                                    user_trade_bal[recipient_user_id] = t['_transfer_amount_wei']
                                    #print(userData.get(recipient_user_id,None))
                                    #print("recipent", recipient_user_id, user_trade_bal[recipient_user_id])

                        else:
                            user_trade_bal[recipient_user_id] = user_trade_bal[recipient_user_id] + t['_transfer_amount_wei']
                            #print("recipent2", recipient_user_id, user_trade_bal[recipient_user_id])


                    for usra_data in user_found_data:
                        #print(usr)
                        #print("test <><><><",user_trade_bal[usr])
                        #user_found_data[usr]['trade_balance'] = user_trade_bal[usr]
                        usra = usra_data['id']
                        tDicta = usra_data
                        if usra in user_trade_bal.keys():
                            tDicta.update({'trade_bal': user_trade_bal[usra]})
                        else:
                            tDicta.update({'trade_bal': 0})
                        #print(usra_data)
                        #print("fianal", usra, user_trade_bal[usra])
                        user_found_data[user_found_data.index(usra_data)] = tDicta
                    stff_hash[user_data['id']] = user_found_data




if private == True:

    for stff in stff_hash.keys():
        filename = './data/staff/stff_' + userData[stff]['first_name'] + '_' + userData[stff]['last_name'] + '_' + start_date.strftime("%Y%m%d") + "-" + end_date.strftime(
            "%Y%m%d") + "-" + days_ago_str + '.csv'
        print("*** staff data saved to: ", filename)
        with open(filename, 'w', newline='') as csvfile:
            writerT = csv.writer(csvfile)

            writerT.writerow(newHeaders)
            # print(userHeaders) #debug
            #print("stff_hash[stff] ", stff_hash[stff])
            for stff_data_list in stff_hash[stff]:
                #print("stff_data_list ", stff_data_list)
                zRow = list()
                if stff_data_list != None:
                    for attr in newHeaders:
                        zRow.append(str(stff_data_list.get(attr, '')).strip('"'))
                        #zRow.append(str(sud))
                    writerT.writerow(zRow)
        # writerT.writerow([str(user_data.get(attr, '')).strip('"') for attr in userHeaders])

    if False:
        filename = './data/sarafu_rewards_msg_'+start_date.strftime("%Y%m%d")+"-"+end_date.strftime("%Y%m%d")+"-"+days_ago_str+'.csv'
        with open(filename, 'w',newline='') as csvfile:
            writerT = csv.writer(csvfile)
            zRow = list()
            zRow.append("phone")
            zRow.append("msg")
            writerT.writerow(zRow)
            for item in reward_msg_list:
                zRow = list()
                zRow.append(item['phone'])
                zRow.append(item['msg'])
                writerT.writerow(zRow)

        filename = './data/sarafu_rewards_'+start_date.strftime("%Y%m%d")+"-"+end_date.strftime("%Y%m%d")+"-"+days_ago_str+'.csv'
        with open(filename, 'w',newline='') as csvfile:
            writerT = csv.writer(csvfile)
            zRow = list()
            zRow.append("id")
            zRow.append("reward_amt")
            writerT.writerow(zRow)
            for item in disbursment_list:
                zRow = list()
                zRow.append(item['id'])
                zRow.append(item['amt'])
                writerT.writerow(zRow)




    if False:
        filename = './data/sarafu_fees_'+start_date.strftime("%Y%m%d")+"-"+end_date.strftime("%Y%m%d")+"-"+days_ago_str+'.csv'
        with open(filename, 'w',newline='') as csvfile:
            writerT = csv.writer(csvfile)
            zRow = list()
            zRow.append("id")
            zRow.append("fee_amt")
            writerT.writerow(zRow)
            for item in reclamation_list:
                zRow = list()
                zRow.append(item['id'])
                zRow.append(item['amt'])
                writerT.writerow(zRow)

    if False:
        filename_rew = './data/treatment/treatment_sarafu_rewards' + start_date.strftime("%Y%m%d") + "-" + end_date.strftime(
            "%Y%m%d") + "-" + days_ago_str + '.csv'
        filename_msg = './data/treatment/treatment_sarafu_messages' + start_date.strftime("%Y%m%d") + "-" + end_date.strftime(
            "%Y%m%d") + "-" + days_ago_str + '.csv'

        with open(filename_rew, 'w', newline='') as csvfile_rew:
            with open(filename_msg, 'w', newline='') as csvfile_msg:
                writer_rew = csv.writer(csvfile_rew)
                writer_msg = csv.writer(csvfile_msg)
                zRow = list()
                zRow.append("id")
                zRow.append("reward_treatment_msg")
                writer_rew.writerow(zRow)
                writer_msg.writerow(zRow)
                totalt = 0
                for txn in treatment_list:
                    #print(txn)
                    foundTreatment = False
                    for user_id, user_data in userData.items():
                        if user_data.get('blockchain_address','') == txn:
                            zRow = list()
                            zRow.append(user_data['id'])
                            zRow.append(400*100)
                            writer_rew.writerow(zRow)

                            zRowa = list()
                            zRowa.append(user_data['_phone'])
                            if user_data['preferred_language'] == 'sw':
                                zRowa.append("Hongera! Umechaguliwa kutuzwa Sarafu 400! Bonyeza *483*46# ama flash/pigia 0757628885")
                            else:
                                zRowa.append("Congrats! You've been chosen to receive 400 Sarafu! Dial *483*46# or flash/call 0757628885")
                            writer_msg.writerow(zRowa)


                            foundTreatment = True
                            totalt+=1
                    if foundTreatment == False:
                        print("Could not find ", txn[0])
                print("Total in Treatment group: ",totalt)

    if False:
        filename_rew = './data/rewards/airtime_sarafu_rewards' + start_date.strftime("%Y%m%d") + "-" + end_date.strftime(
            "%Y%m%d") + "-" + days_ago_str + '.csv'
        filename_msg = './data/rewards/airtime_sarafu_messages' + start_date.strftime("%Y%m%d") + "-" + end_date.strftime(
            "%Y%m%d") + "-" + days_ago_str + '.csv'
        filename_dis = './data/rewards/airtime_dis_sarafu_messages' + start_date.strftime(
            "%Y%m%d") + "-" + end_date.strftime(
            "%Y%m%d") + "-" + days_ago_str + '.csv'

        with open(filename_dis, 'w', newline='') as csvfile_dis:
            with open(filename_rew, 'w', newline='') as csvfile_rew:
                with open(filename_msg, 'w', newline='') as csvfile_msg:
                    writer_rewa = csv.writer(csvfile_rew)
                    writer_msga = csv.writer(csvfile_msg)
                    writer_dis = csv.writer(csvfile_dis)

                    zRow = list()
                    zRow.append("phone_number")
                    zRow.append("amount")
                    zRow.append("currency_code")
                    writer_rewa.writerow(zRow)

                    wRow = list()
                    wRow.append("phone_number")
                    wRow.append("msg")
                    writer_msga.writerow(wRow)

                    dRow = list()
                    dRow.append("id")
                    dRow.append("sarafu-top-up")
                    writer_dis.writerow(dRow)

                    totalt = 0
                    for item in airtime_reward: #airtime_reward.append({"user_phone":user_phone,"airtime_amt":airtime_amt,"trade_out_msg":trade_out_msg})
                        if item["balance"] < 50:
                            diburse = 50-item["balance"]
                            writer_dis.writerow([item["id"], diburse])
                        writer_rewa.writerow([item["user_phone"],item["airtime_amt"],"KES"])
                        writer_msga.writerow([item["user_phone"], item["trade_out_msg"]])




generate_transaction_data_svg(txnData, userData, unique_txnData, start_date, end_date, days_ago_str,days_ago)
generate_location_transaction_data_svg(txnData, userData, unique_txnData, start_date, end_date, days_ago_str,days_ago)
#output_Network_Viz(G, userData, start_date, end_date, days_ago_str)
#output_User_Viz(G, userDllata, start_date, end_date, days_ago_str)
#4082

#get_Network_Viz_Monthly(G, unique_txnData, userData, start_date,end_date,days_ago_str,days_ago)