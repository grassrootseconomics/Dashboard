#!/usr/bin/env python3

# First ensure there's a softlink in 'impacts-sarafu-aws' (poa_puller -> analysis)
#
#     $ cd impacts-sarafu-aws
#     $ ln -s analysis poa_puller
#
# That's enough to trick pickle.

import pickle
import csv
import os
import networkx as nx
import dash_html_components as html
import plotly.graph_objs as go
import matplotlib.pyplot as plt
import psycopg2
from datetime import timedelta
from datetime import datetime
#import chart_studio.plotly as py
import plotly.io as pio
#import datetime
import collections
from analysis.date import Date, DateTime
from datetime import timezone
from analysis.community_token_map import ge_wids
from analysis.community_token_map import poa_to_ussd_token_map
from analysis.community_token_map import used_poa_to_ussd_token_map
from analysis.community_token_map import trans_stats


# Uncomment the following lines to pull from S3
# from poa_puller.db_cache import DBCache
# dbc = DBCache()
# dbc.download_from_S3()
tz = timezone(timedelta(hours=3))

def get_poa_balance(wid,db_cache): #fmt(int(db_cache.poa_balances[wid]) / 10 ** 18, 5)
    balance = 0
    v = db_cache.poa_balances[wid]
    if v == "" or v == None:
        print(wid, " POA empty string!")
    else:
        balance = float(v)

    balance = int(balance / 10 ** 18)
    return balance

def last_transaction_out(wid, transactions, grassroots_wids, exclude_staff):

    exclude_wids = list()
    if exclude_staff:
        exclude_wids = grassroots_wids

    z_dates = []
    time_diff = []

    if transactions:
        for t in transactions:
            if t['from'] == wid and t['to'] not in exclude_wids:
                z_dates.append(int(t['timeStamp']))

    if len(z_dates) > 0:
        idx = 0
        last_trans_date = Date.from_timestamp(max(z_dates))

        while idx < len(z_dates)-1:

            zdiff = Date.from_timestamp(z_dates[idx+1]) - Date.from_timestamp(z_dates[idx])
            zminutes = 0
            if Date.from_timestamp(z_dates[idx+1]) == Date.from_timestamp(z_dates[idx]):
                zminutes = 0
            else:
                zminutes = float(zdiff.days)

            time_diff.append(zminutes)
            idx+=1

        if len(time_diff)>0:
            avgTradeTimeDiff = sum(time_diff) / len(time_diff)
            return avgTradeTimeDiff, last_trans_date

        return 'None', last_trans_date
    else:
        return 'None', 'None'

z_wids = ['0xA','0xB','0xC','0xD','0xE','0xF','0xG']

zall_transactions = [
                    {'from':'0xA', 'to':'0xB', 'value' : '20000000000000000000'},
                    {'from':'0xA', 'to':'0xB', 'value' : '20000000000000000000'},
                    {'from':'0xA', 'to':'0xC', 'value' : '20000000000000000000'},
                    {'from':'0xA', 'to': '0xD', 'value': '20000000000000000000'},
                    {'from':'0xA', 'to': '0xD', 'value': '20000000000000000000'},
                    {'from':'0xA', 'to': '0xE', 'value': '20000000000000000000'},
                    {'from':'0xA', 'to': '0xE', 'value': '20000000000000000000'},
                    {'from':'0xB', 'to': '0xA', 'value': '20000000000000000000'},
                    {'from':'0xB', 'to': '0xD', 'value': '20000000000000000000'},
                    {'from':'0xB', 'to': '0xA', 'value': '20000000000000000000'},
                    {'from':'0xC', 'to': '0xB', 'value': '20000000000000000000'},
                    {'from':'0xD', 'to': '0xA', 'value': '20000000000000000000'},
                    {'from':'0xE', 'to': '0xC', 'value': '20000000000000000000'},
                    {'from':'0xE', 'to': '0xD', 'value': '20000000000000000000'},
                    {'from':'0xE', 'to': '0xD', 'value': '20000000000000000000'},
                    {'from':'0xE', 'to': '0xF', 'value': '20000000000000000000'},
                    {'from':'0xF', 'to': '0xG', 'value': '20000000000000000000'},
                    {'from':'0xF', 'to': '0xB', 'value': '20000000000000000000'},
                    {'from':'0xG', 'to': '0xF', 'value': '20000000000000000000'},

                    ]
'''

A: B2,B2,C2,D2,D2,E2,E2,B2
B: A2,D2,A2
C: B2
D: A2
E: C2, D2, D2, F2
F: G2, B2, B2
G: F2

zall_transactions = [
                    {'from':'0xA', 'to': '0xE', 'value': 2},
                    {'from':'0xA', 'to': '0xE', 'value': 1},
                    {'from': '0xE', 'to': '0xF', 'value': 100},
                    {'from': '0xF', 'to': '0xE', 'value': 100},
                    {'from': '0xF', 'to': '0xA', 'value': 100}
                    ]
'''
def token_transactions(transactions):
    results = {}
    #trans[wid] for t in trans

    for t in transactions:
        if t['from'] in results.keys():
            results[t['from']].append(t)
        else:
            results.update({t['from']:[t]})
    return results


def cumulate(y_values):
    for y in y_values.keys():
        xi = len(y_values[y]) - 2
        while xi >= 0:
            y_values[y][xi] += y_values[y][xi + 1]
            xi -= 1
    return y_values

def convert_to_str(list): #list of characters
    str1 = ""
    return str1.join(list)


def find_wid_amount_all(transactions,wid_to_find):
    amount = 0
    for t in transactions: #these are the transactions of a user
        #we are looking at a users wid_base's transactions and seeing if  wid_to_find appears
        to = t['to']
        if to == wid_to_find:
            #amount+=t['value']#int(float(t['value']) / 10 ** 18)
            amount+=int(float(t['value']) / 10 ** 18)

    return amount

def find_wid_amount_all_raw(transactions,wid_to_find):
    amount = 0
    for t in transactions: #these are the transactions of a user
        #we are looking at a users wid_base's transactions and seeing if  wid_to_find appears
        to = t['to']
        if to == wid_to_find:
            #amount+=t['value']#int(float(t['value']) / 10 ** 18)
            amount+=int(float(t['value']) / 10 ** 18)

    return amount

global_found_list={}

def find_wid_amount(all_transactions,transactions,wid_base,wid_to_find,breadcrumbs,amount):
    global global_found_list
    breadcrumbs.append(wid_base)
    bread_str= convert_to_str(breadcrumbs)
    #print(bread_str)
    results = {}#{bread_str:0}
    found_list=[]
    for t in transactions: #these are the transactions of a user
        #we are looking at a users wid_base's transactions and seeing if  wid_to_find appears
        to = t['to']
        if to in ge_wids():
            continue
        if to == wid_to_find and to not in found_list:
            nc_bread = breadcrumbs.copy()
            nc_bread.append(wid_to_find)
            n_bread=convert_to_str(nc_bread)
            if n_bread in results.keys():
                results[n_bread]+=t['value']
                #results[n_bread]+=int(float(t['value']) / 10 ** 18)
            else:

                #new_result={n_bread: int(float(t['value']) / 10 ** 18) + amount}
                new_result={n_bread: t['value'] + amount}
                results.update(new_result)
                #print("New Result1: ", len(results.keys()),new_result)

        else:
            #last_bread = breadcrumbs[-2]
            if to not in breadcrumbs and to in all_transactions.keys() and to not in found_list :#!=last_bread:
                sub_amount = t['value']#find_wid_amount_all(transactions, to)
                new_trans = all_transactions[t['to']]
                #print(len(breadcrumbs),"       Sub_Searching: ",to, len(new_trans), " for: ", wid_to_find, len(transactions) )
                #print("Sub_Searching: ",to, " to find: ", wid_to_find)
                sub_result = {}
                if (to,wid_to_find) in global_found_list.keys():
                    for k in global_found_list[(to,wid_to_find)].keys():
                        o_bread = breadcrumbs.copy()
                        on_bread=convert_to_str(o_bread)
                        new_key = on_bread+k
                        new_amt = global_found_list[(to,wid_to_find)][k]+sub_amount+amount
                        sub_result.update({new_key:new_amt})
                else:
                    sub_result = find_wid_amount(all_transactions, new_trans, to, wid_to_find,breadcrumbs.copy(),sub_amount+amount)
                    test_result = {}
                    for k in sub_result.keys():
                        new_amount =sub_result[k]-(sub_amount+amount)
                        new_key = k[k.find(to):]
                        test_result.update({new_key:new_amount})

                    global_found_list.update({(to,wid_to_find):test_result}) #might have amounts wrong for next itteration
                    #for sr in sub_result.keys():
                #    sub_result[sr] += t['value']
                found_list.append(to)
                for sr in sub_result.keys():
                    if sr in results.keys():
                        results[sr] += sub_result[sr]
                    else:
                        new_result = {sr: sub_result[sr]}
                        results.update(new_result)
                        #print("New Result2: ",len(results.keys()), new_result)

                #results.update(sub_result)

    return results

def wid_string_to_list(k):
    result = []
    delimiter = "0x"
    data = k.split(delimiter)
    for d in data:
        if len(d)>0:
            result.append(delimiter+d)
    return result

def removeFromGraph(G,users):
    remove = []
    for znode in list(G.nodes):
        if znode not in users:
            remove.append(znode)
    G.remove_nodes_from(remove)
    return G

def get_balance(wid,db_cache):
    balance = 0
    if wid in db_cache.token_balances.keys():
        for v in db_cache.token_balances[wid].values():
            if v != "":
                balance += float(v)

        balance = int(balance / 10 ** 18)
    return balance


def get_permutations(M):
    k = wid_string_to_list(M)
    length = len(k)
    p = 0
    permutations = []

    while p < length-2:
        permu = ""
        if len(permutations)==0:
            permu = k[1:]
        else:
            permu = permutations[-1][1:]
        fst = permu[0]
        permu.append(fst)
        permutations.append(permu)
        p=p+1

    results = []
    for p in permutations:
        sperm = ""
        for s in p:
            sperm+=s
        results.append(sperm)
    return results

def reduce_transactions(all_transactions):
    results = {}
    min_amount = 1000
    for wid in all_transactions.keys():
        found_list = []
        if wid in ge_wids():
            continue
        transactions = all_transactions[wid]
        for t in transactions:
            to = t['to']
            if to in ge_wids():
                continue
            if to not in found_list:
                amount = find_wid_amount_all_raw(transactions,to)
                if amount < min_amount:
                    continue
                if wid not in results.keys():
                    results.update({wid:[{'to':to,'from':wid,'value':amount}]})
                else:
                    results[wid].append({'to':to,'from':wid,'value':amount})
            found_list.append(to)
    return results


def loop_finder(db_cache,start_date=None,end_date=None): #Start with All Transactions

    startDate = start_date#Date().n_days_ago(days=30)
    endDate = end_date #Date().today()
    all_transactions_orig = db_cache.select_token_transactions(startDate, endDate)
    #all_transactions_orig = token_transactions(zall_transactions)

    all_transactions = reduce_transactions(all_transactions_orig)
    result = {}

    num_mem = len(all_transactions.keys())
    wid_int = 1
    for wid in all_transactions.keys():
    # for wid in db_cache.member_info.keys():
        print("wid: ",wid_int,"of",num_mem)
        wid_int+=1
        if wid not in all_transactions:
            continue
        if wid in ge_wids():
            continue

        transactions = all_transactions[wid]
        found_list = []
        for t in transactions:
            to = t['to']
            if to in ge_wids():
                continue
            if to != wid and to in all_transactions.keys() and to not in found_list:
                new_trans = all_transactions[t['to']]
                print("Searching: ",to, len(new_trans), " for: ", wid, len(transactions) )
                amount = t['value']#find_wid_amount_all(transactions,to)
                sub_result = find_wid_amount(all_transactions, new_trans , t['to'], wid, [wid],amount)
                found_list.append(to)
                #for sr in sub_result.keys():
                #    sub_result[sr]+=t['value']
                #print("")
                #print("")
                #print("")
                #print("Found: ", len(sub_result))
                for sr in sub_result.keys():
                    if sr not in result.keys():

                        result.update({sr:sub_result[sr]})
                        #result[sr]+= sub_result[sr]
                    else:
                        if sub_result[sr] != result[sr]:
                            print("Match off: ", sr, sub_result[sr], result[sr])
                        else:
                            print("Match: ", sr, sub_result[sr], result[sr])
                #sub_loop_finder(all_transactions, all_transactions[t['to']], t['to'], wid, [wid], 1)
                #print(sub_result)
                #print("End")
    print("STOP:::::::::::::::::::::!")
    #for k in sorted(result, key=len, reverse=True):
    #    print(k,result[k])
    #sortednames = sorted(result.keys(), key=lambda x: x.lower())
    new_results = result.copy()
    permutations = []

    length_result = len(result.keys())
    print("Finding permutations of ", length_result, " results")
    pp_int = 1
    for k in result.keys():
        pp_int+=1
        zmun_perms = 0
        if k not in permutations:
            da_perms = get_permutations(k)
            zmun_perms = len(da_perms)
            for p in da_perms:
                permutations.append(p)
        print(pp_int,"/", length_result, " results, permutations: ", zmun_perms)

    print(len(permutations), " permutations found",sep="")
    for p in permutations:
        if p in result.keys():
            del result[p]

        #for kl in result.keys():
        #    if kl!=k and len(kl)==length and kl in permutations: #check if it is a permutation
        #        del new_results[kl]
                #remove the 1st character and put the 2nd character on the end
    sortednames = sorted(result, key=result.get, reverse=True)
    with open('loops.csv', 'w', newline='') as csvfile:
        token_writer = csv.writer(csvfile)
        to_print=[]
        to_print.append('volume')
        to_print.append('loop_count')
        to_print.append('users')
        token_writer.writerow(to_print)
        for k in sortednames:
            to_print = []
            perms = wid_string_to_list(k)
            to_print.append(result[k])
            to_print.append(len(perms))
            print(result[k], ", ",len(perms),end = "",sep="")
            for wid in perms:
                to_print.append(db_cache.member_info[wid]['name']+" "+db_cache.member_info[wid]['community_token']+" "+db_cache.member_info[wid]['phone'])
                #to_print.append(wid)

                print(", ",db_cache.member_info[wid]['name'],"(",db_cache.member_info[wid]['community_token'],")",end = '',sep="")
                #print(", ",wid,end = ', ',sep=" ")
            print("")
            token_writer.writerow(to_print)




def trade_partners(wid, transactions, grassroots_wids,db_cache, exclude_staff):

    exclude_wids = list()
    if exclude_staff:
        exclude_wids = grassroots_wids

    out_unique = []
    in_unique = []

    for t in reversed(transactions) :
        if t['to'] == wid and t['from'] not in exclude_wids and t['from'] in db_cache.member_info:
            found = False
            if len(in_unique)>0:
                for fooiq in in_unique:
                    if t['from'] == fooiq['wid']:
                        found = True
                        fooiq['num_trades'] += 1
            if not found or len(in_unique)<1:
                foo = {'wid': t['from'], 'num_trades': 1}
                in_unique.append(foo)


        elif t['from'] == wid and t['to'] not in exclude_wids and t['to'] in db_cache.member_info:
            found = False
            if len(out_unique) > 0:
                for fooiq in out_unique:
                    if t['to'] == fooiq['wid']:
                        found = True
                        fooiq['num_trades'] += 1
            if not found or len(out_unique) < 1:
                foo = {'wid': t['to'], 'num_trades': 1}
                out_unique.append(foo)

    sorted_out_unique = sorted(out_unique, key=lambda k: k['num_trades'],reverse=True)
    sorted_in_unique = sorted(in_unique, key=lambda k: k['num_trades'],reverse=True)

    all_trade_partners = {'out':  sorted_out_unique[:5], 'in': sorted_in_unique[:5]}

    return all_trade_partners



def calc_centrality(transactions, grassroots_wids,db_cache, exclude_staff):

    exclude_wids = list()
    if exclude_staff:
        exclude_wids = grassroots_wids

    G = nx.DiGraph()

    #num_weighs = len(line_weights.values())
    #all_weights = sum(line_weights.values())

    node_names = list()
    tx_hash = list()

    edges = dict()

    for wid, trans in transactions.items():
        for t in trans:

            if t['hash'] not in tx_hash:
                tx_hash.append(t['hash'])
            else:
                continue

            if t['from'] not in exclude_wids and t['from'] in db_cache.member_info and t['to'] not in exclude_wids and t['to'] in db_cache.member_info:


                key = (t['from'],t['to'])
                if key not in edges:
                    edges[key] = int(t['value']) / 10 ** 18
                edges[key] += int(t['value']) / 10 ** 18


                #edges.append({'from':t['from'],'to':t['to']})

                if t['to'] not in node_names:
                    node_names.append(t['to'])
                    G.add_node(t['to'])
                if t['from'] not in node_names:
                    node_names.append(t['from'])
                    G.add_node(t['from'])

    for edge in edges.keys():
        G.add_edge(edge[0],edge[1],weight=edges[edge])

    #degree_cen = nx.degree_centrality(G)

    degree_cen = G.degree(weight='weight')
    instrenght = G.in_degree(weight='weight')
    # expenditure distribution
    outstrenght = G.out_degree(weight='weight')

    load = nx.load_centrality(G, normalized=True, weight='weight')
    btw = nx.betweenness_centrality(G, k=None, normalized=True, weight=None, endpoints=False, seed=None)

    #nx.betweenness_centrality_subset(G, sources, targets, normalized=False, weight=None)

    # measure of hierarchy
    #lrc = nx.local_reaching_centrality(G, v, paths=None, weight=None, normalized=True)

    #degree_cen = sorted(degree_cen, key=lambda x: x[1], reverse=True)

    centrality = {"centrality": degree_cen, "instrenght": instrenght, "outstrenght": outstrenght, "load": load, "btw": btw}
    #print("centrality: ", centrality["centrality"])
    #print("instrenght: ", centrality["instrenght"])
    #print("outstrenght: ", centrality["outstrenght"])
    #print("load: ", centrality["load"])
    #print("btw: ", centrality["btw"])

    return centrality




def generate_community_user_table(db_cache, simplify=None, hubs=None, communities=None, phone_filter=None,
                                  ambassador_filter=None, business_types=None, publics=None,start_date=None,end_date=None):
    '''
    This function generates and returns a list of table elements,
    which are the children of an html.Table object.

    The list elements are html.Tr (table rows).
    '''
    from copy import deepcopy


    headers = []
    if simplify:
        headers = ['rank',
                   'community',
                   'token',
                   'name',
                   'gender',
                   'n_tok',
                   'atbt',
                   'phone',
                   'business_type',
                   'directory',
                   'tier', 'p_foreign_trades',
                   'trade_volume_out',
                   'n_trades_out',
                   'n_partners_out',
                   'top_out_trader',
                   'last_balance',
                   'avg_balance',
                   'std_balance',
                   'min_balance',
                   'max_balance',
                   'days_enrolled',
                   'days_since_trade_out',
                   'new_member',
                   'last_transaction_out',
                   'ambassador_name',
                   'centrality',
                   'instrength',
                   'outstrength',
                   'load',
                   'btw'
                   ]
    elif publics:
        headers = ['rank',
                   'Id',
                   'community',
                   'token',
                   'business_type',
                   'gender',
                   'trade_volume',
                   'n_trades',
                   'n_partners',
                   'last_balance',
                   'avg_balance',
                   'std_balance',
                   'min_balance',
                   'max_balance',
                   'days_enrolled',
                   'days_since_trade_out',
                   'new_member',
                   'first_transaction',
                   'group_created',
                   'agent_created',
                   'last_transaction_out']
    else:
        headers = ['rank',
                   'Id',
                   'community',
                   'token',
                   'location',
                   'name',
                   'first_name',
                   'gender',
                   'lang',
                   'n_tok',
                   'atbt',
                   'phone',
                   'business_type',
                   'directory',
                   'tier', 'p_foreign_trades',
                   'vol_sarafu_in',
                   'vol_sarafu_out',
                   'trade_volume_in',
                   'trade_volume_out',
                   'n_trades_in',
                   'n_trades_out',
                   'n_partners_in',
                   'n_partners_out',
                   'top_out_trader',
                   'last_balance',
                   'avg_balance',
                   'std_balance',
                   'min_balance',
                   'max_balance',
                   'poa',
                   'days_enrolled',
                   'days_since_trade_out',
                   'new_member',
                   'first_transaction',
                   'group_created',
                   'agent_created',
                   'last_transaction_out',
                   'ambassador_name',
                   'status',
                   'auto_convert_enabled',
                   'referred_by_id',
                   'id',
                   'pin_failed_attempts',
                   'centrality',
                   'instrength',
                   'outstrength',
                   'load',
                   'btw'
                   ]

    table_elements = [html.Tr([html.Th(h) for h in headers])]
    poa_link_url = 'https://blockscout.com/poa/core/address/'

    keys, table_data = generate_community_user_table_data(db_cache,
                                                          simplify,
                                                          hubs,
                                                          communities,
                                                          phone_filter,
                                                          ambassador_filter,
                                                          business_types,
                                                          publics,start_date,end_date)
    for rank, row in enumerate(table_data):

        r = deepcopy(row)

        if ((simplify == False or simplify == None) and (publics == False or publics == None)):

            r['ambassador_name'] = html.Strong(row['ambassador_name'], style={'color': 'blue'})
            if float(row['poa']) < 0.0005:
                r['poa'] = html.Strong(row['poa'], style={'color': 'red'})

        if ((simplify == False or simplify == None) or (publics == True)):
            wid = row['Id']
            r['Id'] = html.A(wid, href=poa_link_url + wid)

        data = [rank + 1]
        data.extend([r[key] for key in keys])
        table_row = html.Tr([html.Td(v) for v in data])
        table_elements.append(table_row)

    c_idx = 0
    number = 1
    with open('oldfull_users-table.csv', 'w', newline='') as csvfile:
        token_writer = csv.writer(csvfile)
        header_printed = False
        # The token_transactions public member of DBCache is just a dictionary { wallet_id: list(transactions) }
        # Each transaction is the dictionary pulled from POA, but with only the fields we think we need:
        # i.e. value,tokenSymbol,tokenName,tokenDecimal,to,timeStamp,hash,from,contractAddress,blockNumber,blockHash
        # Here I don't use the wallet_id so it's sufficient to just iterate over the values (list of transactions)
        header_printed = False
        if True:
            for datain in table_data:
                #print("d:: ", datain.values())
                if not header_printed:
                    token_writer.writerow(datain.keys())
                    header_printed = True

                token_writer.writerow(datain.values())
                if c_idx >= chunks:
                    c_idx = 0
                    csvfile.flush()  # whenever you want
                    print("chunk: ", number)
                else:
                    c_idx += 1

                number += 1

                #token_writer.writerow(token_transaction.values())

    return table_elements


def generate_full_user_table(db_cache, start_date=None,end_date=None):

    headers = ['id','community','token','location','name','first_name','gender','lang','n_tok','atbt','phone','business_type','directory','tier', 'p_foreign_trades',
           'vol_sarafu_in','vol_sarafu_out','trade_volume_in','trade_volume_out','n_trades_in','n_trades_out','n_partners_in','n_partners_out','top_out_trader','last_balance',
           'avg_balance','std_balance','min_balance','max_balance','days_enrolled','days_since_trade_out',
           'new_member','first_transaction','group_created','agent_created','last_transaction_out',
           'ambassador_name','status','auto_convert_enabled','referred_by_id','pin_failed_attempts']

    token_transactions = db_cache.select_token_transactions(start_date, end_date)
    all_token_transactions = db_cache.select_token_transactions(None, None)

    grassroots_wids = ge_wids()
    exclude_staff = False

    with open('full_users-table.csv', 'w', newline='') as csvfileU:
        spamwriterU = csv.writer(csvfileU)
        spamwriterU.writerow(headers)

        for wid in db_cache.member_info.keys():

            if wid in grassroots_wids and exclude_staff == True:
                continue

            if wid not in all_token_transactions.keys():
                continue

            transactions = token_transactions[wid]

            if len(transactions) == 0 :
                print('<><><> Found communityFund: with no transactions in that time period')

            all_transactions = all_token_transactions[wid]

            community_token = poa_to_ussd_token_map[db_cache.member_info[wid]['community_token']]
            community_token_raw = getAtt(db_cache, wid, 'community_token')
            preferred_language = getAtt(db_cache, wid, 'preferred_language')


            trade_p = trade_partners(wid, transactions, grassroots_wids, db_cache, exclude_staff)
            top_out_trader = ""
            if len(trade_p['out']) > 0:
                topp = trade_p['out'][0]
                pName = db_cache.member_info[topp['wid']]['name'].split(' ')[0]
                pDirectory = db_cache.member_info[topp['wid']]['directory']
                pPhone = "0" + db_cache.member_info[topp['wid']]['phone'][4:]

                top_out_trader = pName + " " + pPhone + " " + pDirectory

            top_in_trader = ""
            if len(trade_p['in']) > 0:
                topp = trade_p['in'][0]
                pName = db_cache.member_info[topp['wid']]['name'].split(' ')[0]
                pDirectory = db_cache.member_info[topp['wid']]['directory']
                pPhone = "0" + db_cache.member_info[topp['wid']]['phone'][4:]
                top_in_trader = pName + " " + pPhone + " " + pDirectory

            t_stats = trans_stats(db_cache, wid, transactions, grassroots_wids)

            n_trading_partners_in = t_stats['n_in_unique']
            n_trading_partners_out = t_stats['n_out_unique']
            trade_volume_in = t_stats['vol_trans_in']
            trade_volume_out = t_stats['vol_trans_out']
            n_transactions_in = t_stats['n_trans_in']
            n_transactions_out = t_stats['n_trans_out']

            avg_balance = t_stats['bal_avg']
            std_balance = t_stats['bal_std']
            min_balance = t_stats['bal_min']
            max_balance = t_stats['bal_max']

            n_trading_partners_in_foreign = t_stats['n_in_unique_foreign']
            n_trading_partners_out_foreign = t_stats['n_out_unique_foreign']
            trade_volume_in_foreign = t_stats['vol_trans_in_foreign']
            trade_volume_out_foreign = t_stats['vol_trans_out_foreign']
            trade_volume_in_sarafu = t_stats['sarafu_in']
            trade_volume_out_sarafu = t_stats['sarafu_out']
            n_transactions_in_foreign = t_stats['n_trans_in_foreign']
            n_transactions_out_foreign = t_stats['n_trans_out_foreign']
            cc_balance = t_stats['cc_balance']  # get_balance(wid, db_cache)

            p_foreign = 0
            if (n_transactions_in + n_transactions_out) > 0:
                p_foreign = int(100 * (n_transactions_in_foreign + n_transactions_out_foreign) / (
                        n_transactions_in + n_transactions_out))

            n_tok = 0
            for con_id in db_cache.token_balances[wid]:
                if db_cache.token_balances[wid][con_id] == "":
                    print(wid, " empty string on token")
                else:
                    tok = int(float(db_cache.token_balances[wid][con_id]) / 10 ** 18)
                    if tok >= 1:
                        n_tok = n_tok + 1

            if n_trading_partners_out >= 6 and trade_volume_out >= 400 and n_trading_partners_in >= 6 and trade_volume_in >= 400:
                tier = 4
            elif n_trading_partners_out >= 3 and trade_volume_out >= 200 and n_trading_partners_in >= 3 and trade_volume_in >= 200:
                tier = 3
            elif trade_volume_out >= 100:
                tier = 2
            elif trade_volume_out > 0:
                tier = 1
            else:
                tier = 0

            first_transaction = 'None'
            last_transaction = 'None'
            days_enrolled = 9999
            new_member = 0

            if all_transactions:
                first_transaction = Date.from_timestamp(min([int(t['timeStamp']) for t in all_transactions]))

                foo = last_transaction_out(wid, all_transactions, grassroots_wids, exclude_staff)
                avg_time_between_trades = foo[0]
                if isinstance(avg_time_between_trades, float):
                    avg_time_between_trades = format(avg_time_between_trades, '.2f')
                last_transaction = foo[1]
                delta_f = Date.today() - first_transaction
                if last_transaction != 'None':
                    delta_l = Date.today() - last_transaction
                else:
                    delta_l = delta_f
                days_enrolled = delta_f.days
                days_since_trade_out = delta_l.days
                # print("<>1st tran 1: ",first_transaction)
                first_transaction = first_transaction.isoformat()
                # print("1st tran 2: ", first_transaction)
                if not isinstance(last_transaction, str):
                    last_transaction = last_transaction.isoformat()

                # if days_enrolled < day_span and last_transaction != 'None':
                #    new_member = 1


            row_data = {'id': wid}
            row_data['community'] = community_token
            row_data['token'] = community_token_raw
            row_data['business_type'] = getAtt(db_cache, wid, 'business_type')
            row_data['gender'] = getAtt(db_cache, wid, 'gender')
            row_data['directory'] = getAtt(db_cache, wid, 'directory')

            row_data['name'] = db_cache.member_info[wid]['name']
            row_data['first_name'] = row_data['name'].split(' ')[0]
            row_data['lang'] = preferred_language
            row_data['n_tok'] = n_tok
            row_data['atbt'] = avg_time_between_trades
            row_data['phone'] = getAtt(db_cache, wid, 'phone')
            row_data['location'] = getAtt(db_cache, wid, 'location')
            row_data['status'] = getAtt(db_cache, wid, 'status')
            row_data['tier'] = tier
            row_data['p_foreign_trades'] = p_foreign
            row_data['vol_sarafu_in'] = int(trade_volume_in_sarafu)
            row_data['vol_sarafu_out'] = int(trade_volume_out_sarafu)
            row_data['trade_volume_in'] = int(trade_volume_in)
            row_data['trade_volume_out'] = int(trade_volume_out)
            row_data['n_trades_in'] = n_transactions_in
            row_data['n_trades_out'] = n_transactions_out
            row_data['n_partners_in'] = n_trading_partners_in
            row_data['n_partners_out'] = n_trading_partners_out
            row_data['top_out_trader'] = top_out_trader
            row_data['poa'] = get_poa_balance(wid, db_cache)  # fmt(int(db_cache.poa_balances[wid]) / 10 ** 18, 5)
            row_data['auto_convert_enabled'] = getAtt(db_cache, wid, 'auto_convert_enabled')
            row_data['referred_by_id'] = getAtt(db_cache, wid, 'referred_by_id')

            row_data['pin_failed_attempts'] = getAtt(db_cache, wid, 'pin_failed_attempts')
            row_data['last_balance'] = cc_balance  # get_balance(wid, db_cache)
            row_data['avg_balance'] = avg_balance
            row_data['std_balance'] = std_balance
            row_data['min_balance'] = min_balance
            row_data['max_balance'] = max_balance

            row_data['first_transaction'] = first_transaction
            row_data['group_created'] = getAtt(db_cache, wid, 'group_accounts_created_at')
            row_data['agent_created'] = getAtt(db_cache, wid, 'token_agents_created_at')
            row_data['days_enrolled'] = days_enrolled
            row_data['days_since_trade_out'] = days_since_trade_out
            row_data['new_member'] = new_member
            row_data['last_transaction_out'] = last_transaction

            row_data['ambassador_name'] = db_cache.member_info[wid]['ambassador_name']
            spamwriterU.writerow([str(row_data[k]) for k in headers])





def generate_transaction_data_csv(db_cache,days=None,start_date=None,end_date=None):
    #db_cache = DBCache()
    #db = RDSDev()

    only_known = False
    filter_staff=False#True
    private=False
    result = list()

    tx_hash = list()
    table_data = result

    headersTxPriv = ['id', 'timeset', 'source', 's_name', 's_phone', 's_comm_tkn', 's_gender', 's_location',
               's_business_type', 's_directory', 'target', 't_name', 't_phone', 't_comm_tkn', 't_gender',
               't_location', 't_business_type', 't_directory', 'tx_token', 'weight', 'tx_hash', 'type']

    headersUserPriv = ['id', 'start', 'label', 'name', 'phone', 'comm_tkn', 'gender', 'location',
               'business_type', 'directory', 'bal','vol_trans_in','vol_trans_out','n_trans_in','n_trans_out']

    headersTxPub = ['id', 'timeset', 'source','s_comm_tkn', 's_gender',
                     's_business_type', 'target', 't_comm_tkn', 't_gender',
                    't_business_type', 'tx_token', 'weight', 'tx_hash', 'type']

    headersUserPub = ['id', 'start', 'label', 'comm_tkn', 'gender',
                       'business_type', 'bal', 'vol_trans_in', 'vol_trans_out', 'n_trans_in',
                       'n_trans_out']

    headersUser=headersUserPriv
    if not private:
        headersUser = headersUserPub
    headersTx = headersTxPriv
    if not private:
        headersTx = headersTxPub
    filenameTx = 'POA_tx_all_pub.csv'
    filenameUser = 'POA_users_all_pub.csv'


    print("saving all transactions to: ", filenameTx)
    print("saving all users to: ", filenameUser)

    import csv
    import time
    timestr = time.strftime("%Y%m%d-%H%M%S")

    token_transactions = db_cache.select_token_transactions(start_date, end_date)

    indexR = 0

    seen_users = []
    exclude_list = []

    if filter_staff:
        exclude_list = ge_wids()
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

            c_idx = 0
            chunks = 1000

            for wid, transactions in token_transactions.items():

                if wid == "0xa2bdf6845ca9e811ab2fd7a803e69e3fe527681a":
                    if get_balance(wid, db_cache) == 0:
                        print("0xa2bdf6845ca9e811ab2fd7a803e69e3fe527681a found with no balance")

                if wid not in seen_users:
                    Date.from_timestamp(min([int(t['timeStamp']) for t in transactions]))
                    user_data1 = {'start': DateTime.from_timestamp(min([int(t['timeStamp']) for t in transactions])).isoformat() }
                    user_data1['id'] = wid
                    user_data1['label'] = numberUsers
                    user_data1['bal'] = get_balance(wid, db_cache)
                    if private:
                        user_data1['name'] = getAtt(db_cache, wid, 'name')
                        user_data1['phone'] = getAtt(db_cache, wid, 'phone')
                        user_data1['directory'] = getAtt(db_cache, wid, 'directory')
                    user_data1['comm_tkn'] = getAtt(db_cache, wid, 'community_token')
                    user_data1['gender'] = getAtt(db_cache, wid, 'gender')
                    user_data1['location'] = getAtt(db_cache, wid, 'location')
                    user_data1['business_type'] = getAtt(db_cache, wid, 'business_type')


                    numberUsers+=1
                    t_stats = trans_stats(db_cache, wid, transactions.copy(), exclude_list)
                    user_data1['n_in_unique'] = t_stats['n_in_unique']
                    user_data1['n_out_unique'] = t_stats['n_out_unique']
                    user_data1['vol_trans_in'] = t_stats['vol_trans_in']
                    user_data1['vol_trans_out'] = t_stats['vol_trans_out']
                    user_data1['n_trans_in'] = t_stats['n_trans_in']
                    user_data1['n_trans_out'] = t_stats['n_trans_out']


                    spamwriterUser.writerow([str(user_data1[k]) for k in headersUser])
                    seen_users.append(wid)

                for t in transactions:

                    if t['hash'] in tx_hash:
                        # tx_hash.remove(t['hash']) # there should be only one dup
                        continue
                    tx_hash.append(t['hash'])

                    wid0 = t['to']
                    wid1 = t['from']

                    if only_known:
                        if wid0 not in db_cache.member_info or wid1 not in db_cache.member_info:
                            # tx_hash.remove(t['hash']) # there should be only one dup
                            continue

                    if filter_staff:
                        if wid0 in exclude_list or wid1 in exclude_list:
                            continue

                    row_data = {'timeset': DateTime.from_timestamp(t['timeStamp']).isoformat()}
                    row_data['tx_token'] = t['tokenName']
                    row_data['weight'] = int(float(t['value']) / 10 ** 18)
                    row_data['type'] = 'directed'
                    row_data['id'] = numberTx
                    row_data['label'] = numberTx
                    row_data['tx_hash'] = t['hash']
                    row_data['source'] = wid0
                    row_data['s_bal'] = get_balance(wid0, db_cache)
                    if private:
                        row_data['s_name'] = getAtt(db_cache, wid0, 'name')
                        row_data['s_phone'] = getAtt(db_cache, wid0, 'phone')
                        row_data['s_directory'] = getAtt(db_cache, wid0, 'directory')
                    row_data['s_comm_tkn'] = getAtt(db_cache, wid0, 'community_token')
                    row_data['s_gender'] = getAtt(db_cache, wid0, 'gender')
                    row_data['s_location'] = getAtt(db_cache, wid0, 'location')
                    row_data['s_business_type'] = getAtt(db_cache, wid0, 'business_type')


                    row_data['target'] = wid1
                    row_data['t_bal'] = get_balance(wid1, db_cache)
                    if private:
                        row_data['t_name'] = getAtt(db_cache, wid1, 'name')
                        row_data['t_phone'] = getAtt(db_cache, wid1, 'phone')
                        row_data['t_directory'] = getAtt(db_cache, wid1, 'directory')
                    row_data['t_comm_tkn'] = getAtt(db_cache, wid1, 'community_token')
                    row_data['t_gender'] = getAtt(db_cache, wid1, 'gender')
                    row_data['t_location'] = getAtt(db_cache, wid1, 'location')
                    row_data['t_business_type'] = getAtt(db_cache, wid1, 'business_type')


                    # print("row_data",row_data)
                    # result.append(row_data)

                    # spamwriter.writerow([str(number) + ',' + ','.join([str(row_data[k]) for k in headers])])

                    spamwriterTx.writerow([str(row_data[k]) for k in headersTx])
                    if indexR < 3:
                        print(row_data)
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



def getAtt(db_cache, wid, attribute):
    foo = ''
    if wid in db_cache.member_info:
        if attribute in db_cache.member_info[wid]:
            foo = db_cache.member_info[wid]['community_token'],
            if foo != '' or foo != None:
                foo = db_cache.member_info[wid][attribute]
    return foo

def generate_transaction_data_gephi(db_cache,days=None,start_date=None,end_date=None):
    #db_cache = DBCache()
    #db = RDSDev()

    result = list()

    tx_hash = list()
    table_data = result

    import csv
    import time
    timestr = time.strftime("%Y%m%d-%H%M%S")

    token_transactions = db_cache.select_token_transactions(start_date, end_date)

    indexR = 0

    G = nx.MultiDiGraph()
    number = 1
        # if not os.path.exists(filename):


    seen_nodes = []
    seen_edges = []
    c_idx = 0
    chunks = 1000
    add_node_data = False
    #G = nx.MultiDiGraph(mode='dynamic',defaultedgetype="directed")
    G = nx.DiGraph()
    for wid, transactions in token_transactions.items():

        for t in transactions:

            if t['hash'] in tx_hash:
                # tx_hash.remove(t['hash']) # there should be only one dup
                continue
            tx_hash.append(t['hash'])

            if t['to'] in db_cache.member_info and t['from'] in db_cache.member_info:
                wid0=t['to']
                wid1=t['from']

                weight = int(float(t['value']) / 10 ** 18)

                old_weight = 0
                found_edge = False
                for u,v in G.edges():
                    if u==wid0 and v == wid1:
                        old_weight+=G.edges[u, v]['weight']
                        found_edge=True
                if found_edge == True:
                    weight += old_weight
                    G.edges[u, v]['weight'] = weight

                if found_edge==False:
                    G.add_edge(wid0,wid1,weight=weight,txhash=t['hash'],timeset=DateTime.from_timestamp(t['timeStamp']).isoformat(),start=DateTime.from_timestamp(t['timeStamp']).isoformat())
                    if add_node_data:
                        if wid0 not in seen_nodes:
                            G.add_node(wid0, name=getAtt(db_cache,wid0,'name'),
                                       phone=getAtt(db_cache, wid0, 'phone'),
                                       token=getAtt(db_cache,wid0,'community_token'),
                                       directory=getAtt(db_cache,wid0,'directory'),
                                       gender=getAtt(db_cache,wid0,'gender'),
                                       location=getAtt(db_cache,wid0,'location'),
                                       biz_type=getAtt(db_cache,wid0,'business_type'),
                                       balance= get_balance(wid0, db_cache),
                                       start=DateTime.from_timestamp(t['timeStamp']).isoformat())
                            seen_nodes.append(wid0)
                        if wid1 not in seen_nodes:
                            G.add_node(wid1, name=getAtt(db_cache,wid1,'name'),
                                       phone=getAtt(db_cache, wid1, 'phone'),
                                       token=getAtt(db_cache,wid1,'community_token'),
                                       directory=getAtt(db_cache,wid1,'directory'),
                                       gender=getAtt(db_cache,wid1,'gender'),
                                       location=getAtt(db_cache,wid1,'location'),
                                       biz_type=getAtt(db_cache,wid1,'business_type'),
                                       balance=get_balance(wid1, db_cache),
                                   start=DateTime.from_timestamp(t['timeStamp']).isoformat())
                            seen_nodes.append(wid1)

    Gc = G
    if False:
        d = list(nx.weakly_connected_components(G))
        largest_sub_graph = None
        size_of_largest = 0
        sub_graphs = []
        print_graphs = []
        for sub_graph in d:
            if len(list(sub_graph)) > 3:
                print(len(list(sub_graph)), " sub_graph:", end='')
                print_graphs.append(sub_graph)
                for usr in sub_graph:
                    print(db_cache.member_info[usr]['location'], end=', ')
                print(" ")
            if len(list(sub_graph)) > size_of_largest:
                size_of_largest = len(list(sub_graph))
                largest_sub_graph = list(sub_graph)
        Gc = removeFromGraph(G.copy(), largest_sub_graph)


    gephiName = "test-no-cut_30_2.gexf"
    #dotName = "test10-50.dot"

    totalBalance = 0
    for n in list(Gc.nodes()):
        totalBalance += get_balance(n, db_cache)
    print(" output saved: ", gephiName, " Volume:", Gc.size(weight="weight"), " users:", len(Gc), "totalBal:",
          totalBalance)

    nx.write_gexf(Gc, gephiName)
    #pdot = nx.nx_pydot.to_pydot(Gc)
    #nx.nx_pydot.write_dot(Gc, dotName)

    return Gc

def generate_trade_volume(db_cache,the_start_date,the_end_date):

    phone_filter = None
    ambassador_filter = None
    communities = None
    hubs=None
    cumu = False

    '''
    This function generates and returns a trade volume plot.
    '''


    start_date = the_start_date#Date.from_plotly(the_start_date)

    end_date = the_end_date#Date.from_plotly(the_end_date)

    communities = list()
    token_names = [token for token in used_poa_to_ussd_token_map.keys()]
    idx = 1
    communities.insert(0, 'Total')
    for to in token_names:
        communities.insert(idx, to)
        idx = idx+1
    #db = RDSDev()




    token_transactions = db_cache.select_token_transactions(start_date, end_date)

    x_values = list(range(1 + (end_date - start_date).days))
    y_values = {community : [0 for _ in x_values]
                for community in communities
                if community}


    unknowns = set()
    tx_hash = list()

    grassroots_wids = list()  # ge_wids()
    if hubs != None:
        if 'exclude_staff' in hubs:
            grassroots_wids = ge_wids()
    else:
        grassroots_wids = ge_wids()


    for wid, transactions in token_transactions.items():

        if wid in grassroots_wids:
            continue

        phone_number = db_cache.member_info[wid]['phone']

        if phone_filter != None and phone_filter != "" and phone_filter != phone_number:
            continue
        ambassador = db_cache.member_info[wid]['ambassador_name']

        if ambassador_filter != None and\
           ambassador_filter != "" and\
           ambassador_filter.replace(" ","") != ambassador.replace(" ", ""):
            continue

        for t in transactions:

            if t['to'] in grassroots_wids or t['from'] in grassroots_wids:
                continue
            tx_hash.append(t['hash'])

            # the origin (idx = 0) is the end date
            date = Date.from_timestamp(t['timeStamp'])
            idx = (end_date - date).days
            tokens = int(t['value'])/10**int(t['tokenDecimal'])

            token_name = t['tokenName']

            for sto in communities:
                if token_name == sto:
                    y_values['Total'][idx] += tokens
                    y_values[token_name][idx] += tokens

    if cumu:
        y_values = cumulate(y_values)

    data = [go.Scatter(x = x_values, y = y, name = name)
            for name, y in y_values.items()
            if name in communities]

    tickvals = list(range(0, (end_date - start_date).days, 3))
    ticktext = list([str(end_date - timedelta(days=d)) for d in tickvals])
    xaxis = {
        'title': 'Date',
        'autorange' : 'reversed',
        'tickvals' : tickvals,
        'ticktext' : ticktext
    }

    yaxis_titel = 'Trade Volume'
    if cumu:
        yaxis_titel = 'Trade Volume Cumulative'

    layout = go.Layout(
        xaxis = xaxis,
        height = 600,
        yaxis = {'title': yaxis_titel},
        showlegend=True
    )

    return go.Figure(data=data, layout=layout)



def generate_trade_number(db_cache,the_start_date,the_end_date):
    '''
    This function generates and returns a trade volume plot.
    '''
    phone_filter=None
    ambassador_filter=None
    hubs=None
    cumu=False

    start_date = the_start_date#Date.from_plotly(the_start_date)

    end_date = the_end_date#Date.from_plotly(the_end_date)

    communities = list()
    token_names = [token for token in used_poa_to_ussd_token_map.keys()]
    idx = 1
    communities.insert(0, 'Total')
    for to in token_names:
        communities.insert(idx, to)
        idx = idx+1
    #db = RDSDev()



    x_values = list(range(1 + (end_date - start_date).days))
    y_values = {community: [0 for _ in x_values]
                for community in communities
                if community}

    unknowns = set()
    tx_hash = list()


    grassroots_wids = list()#ge_wids()
    if hubs != None:
        if 'exclude_staff' in hubs:
            grassroots_wids = ge_wids()
    else:
        grassroots_wids = ge_wids()




    token_transactions = db_cache.select_token_transactions(start_date, end_date)

    for wid, transactions in token_transactions.items():

        if wid in grassroots_wids:
            continue

        phone_number = db_cache.member_info[wid]['phone']

        if phone_filter != None and phone_filter != "" and phone_filter != phone_number:
            continue
        ambassador = db_cache.member_info[wid]['ambassador_name']

        if ambassador_filter != None and \
           ambassador_filter != "" and\
           ambassador_filter.replace(" ","") != ambassador.replace(" ", ""):
            continue

        for t in transactions:

            if t['to'] in grassroots_wids or t['from'] in grassroots_wids:
                continue


            if t['hash'] in tx_hash:
                continue

            tx_hash.append(t['hash'])

            # the origin (idx = 0) is the end date
            date = Date.from_timestamp(t['timeStamp'])
            idx = (end_date - date).days

            token_name = t['tokenName']

            for sto in communities:
                if token_name == sto:
                    y_values['Total'][idx] += 1  # tokens
                    y_values[token_name][idx] += 1  # tokens

    if cumu:
        y_values = cumulate(y_values)

    data = [go.Scatter(x=x_values, y=y, name=name)
            for name, y in y_values.items()
            if name in communities]

    tickvals = list(range(0, (end_date - start_date).days, 3))
    ticktext = list([str(end_date - timedelta(days=d)) for d in tickvals])
    xaxis = {
        'title': 'Date',
        'autorange': 'reversed',
        'tickvals': tickvals,
        'ticktext': ticktext
    }

    yaxis_titel = 'Number of Trades'
    if cumu:
        yaxis_titel = 'Number of Trades Cumulative'

    layout = go.Layout(
        xaxis=xaxis,
        height=600,
        yaxis={'title': yaxis_titel},
        showlegend=True
    )


    return go.Figure(data=data, layout=layout)

def generate_reg_number(db_cache,the_start_date,the_end_date):
    phone_filter=None
    ambassador_filter=None
    communities=None
    hubs=None
    cumu=False
    '''
    This function generates and returns a trade volume plot.
    '''

    start_date = the_start_date#Date.from_plotly(the_start_date)

    end_date = the_end_date#Date.from_plotly(the_end_date)

    communities = list()
    token_names = [token for token in used_poa_to_ussd_token_map.keys()]
    idx = 1
    communities.insert(0, 'Total')
    for to in token_names:
        communities.insert(idx, to)
        idx = idx+1
    #db = RDSDev()

    all_token_transactions = db_cache.select_token_transactions(None, None)


    x_values = list(range(1 + (end_date - start_date).days))
    y_values = {community: [0 for _ in x_values]
                for community in communities
                if community}


    for wid, transactions in all_token_transactions.items():

        first_transaction = Date.from_timestamp(min([int(t['timeStamp']) for t in transactions]))
        if first_transaction < start_date:
            continue
        idx = (end_date - first_transaction).days
        token_name = db_cache.member_info[wid]['community_token']

        for sto in communities:
            if token_name == sto:
                y_values['Total'][idx] += 1  # tokens
                y_values[token_name][idx] += 1  # tokens

    if cumu:
        y_values = cumulate(y_values)

    data = [go.Scatter(x=x_values, y=y, name=name)
            for name, y in y_values.items()
            if name in communities]

    # n_ticks = ((end_date - start_date).days)/5
    tickvals = list(range(0, (end_date - start_date).days, 3))
    ticktext = list([str(end_date - timedelta(days=d)) for d in tickvals])
    xaxis = {
        'title': 'Date',
        'autorange': 'reversed',
        'tickvals': tickvals,
        'ticktext': ticktext
    }

    yaxis_titel = 'Number of Registrations'
    if cumu:
        yaxis_titel = 'Number of Registrations Cumulative'

    layout = go.Layout(
        xaxis=xaxis,
        height=600,
        yaxis={'title': yaxis_titel},
        showlegend=True
    )

    return go.Figure(data=data, layout=layout)


def FullTx():
    with open('transactions.csv', 'w', newline='') as csvfile:
        token_writer = csv.writer(csvfile)
        header_printed = False
        # The token_transactions public member of DBCache is just a dictionary { wallet_id: list(transactions) }
        # Each transaction is the dictionary pulled from POA, but with only the fields we think we need:
        # i.e. value,tokenSymbol,tokenName,tokenDecimal,to,timeStamp,hash,from,contractAddress,blockNumber,blockHash
        # Here I don't use the wallet_id so it's sufficient to just iterate over the values (list of transactions)

        if True:
            for token_transactions in db_cache.token_transactions.values():
                for token_transaction in token_transactions:
                    if not header_printed:
                        token_writer.writerow(token_transaction.keys())
                        header_printed =  True

                    token_writer.writerow(token_transaction.values())
                    if c_idx >= chunks:
                        c_idx = 0
                        csvfile.flush()  # whenever you want
                        print("chunk: ", number)
                    else:
                        c_idx += 1

                    number += 1

                    token_writer.writerow(token_transaction.values())


db_cache = pickle.load(open('db_cache_state', 'rb'))

c_idx = 0
chunks = 1000
number = 1

# start_date = Date.from_plotly(start_date)
# end_date = Date.from_plotly(end_date)
days = 30
start_date = None#Date().n_days_ago(days=days)
end_date = None# Date().today()


# start_date = Date.from_plotly(start_date)
# end_date = Date.from_plotly(end_date)
if False:
    print("Generating trade Numbers")
    fig_num = generate_trade_number(db_cache,start_date,end_date)
    pio.write_image(fig_num, "trade_num_"+str(days)+".png",width=1024, height=768)#plt.show(fig)

    print("Generating trade volume")
    fig_vol = generate_trade_volume(db_cache,start_date,end_date)
    pio.write_image(fig_vol, "trade_vol_"+str(days)+".png",width=1024, height=768)#plt.show(fig)

    print("Generating registrations")
    fig_reg = generate_reg_number(db_cache,start_date,end_date)
    #fig.show()#py.plot(fig, filename='pyguide_1')

    pio.write_image(fig_reg, "trade_reg_"+str(days)+".png",width=1024, height=768)  # plt.show(fig)



if True:

    #generate_community_user_table(db_cache,start_date=start_date,end_date=end_date,publics=False)
    generate_full_user_table(db_cache,start_date=start_date,end_date=end_date)
    generate_transaction_data_csv(db_cache,start_date=start_date,end_date=end_date)


    #generate_transaction_data_gephi(db_cache,start_date=start_date,end_date=end_date)
    #loop_finder(db_cache,start_date=start_date,end_date=end_date)  # Start with All Transactions
