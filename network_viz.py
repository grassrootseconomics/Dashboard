import matplotlib.pyplot as plt
from matplotlib import rcParams
from toolkit import Date, DateTime
import networkx as nx
import random
import csv
import math
import operator


rcParams.update({'figure.autolayout': True})
scale = 300.0

class Position:
    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z

    def __add__(self, other):
        self.x += other.x
        self.y += other.y
        self.z += other.z
        return self

class Node:
    def __init__(self, name, phone,
                 color,
                 shift = Position(0,0,0),
                 tier = 0,
                 volume = 0,
                 community = '', biz_type=''):



        # radius of the circle
        circle_r = 1
        # center of the circle (x, y)
        circle_x = 0
        circle_y = 0

        # random angle
        alpha = 2 * math.pi * random.random()
        # random radius
        r = circle_r * math.sqrt(random.random())

        # calculating coordinates
        x = r * math.cos(alpha) + circle_x
        y = r * math.sin(alpha) + circle_y

        self.name = name
        self.phone = phone
        self.color = color
        self.volume = volume
        self.community = community
        self.biz_type = biz_type
        self.position = Position(x, y, 1) + shift
        self.position = Position(pt["x"], pt["y"], pt["z"]) + shift #Alex


class Edge:
    def __init__(self, n0, n1, weight):

        self.p0 = n0.position
        self.p1 = n1.position
        self.weight = weight



business_colors= {
    'Education': 'brown',
    'Food/Water': 'green',
    'Farming/Labour':'gold',
    'Shop': 'blue',
    'Transport': 'orange',
    'Fuel/Energy': 'grey',
    'Savings Group': 'olive',
    'Health':'red'}

business_properties = {
    'Education' : {
        'center' : Position(scale,0,scale),
        'color' : 'deepskyblue'
    },
    'Food' : {
        'center' : Position(scale,scale,0),
        'color' : 'gold'
    },
    'Labour' : {
        'center' : Position(scale,scale,0),
        'color' : 'brown'
    },
    'Water' : {
        'center' : Position(0,scale,scale),
        'color' : 'blue'
    },
    'Rent' : {
        'center' : Position(scale,scale,scale),
        'color' : 'orange'
    },
    'Transport' : {
        'center' : Position(0,0,scale),
        'color' : 'yellow'
    },
    'General shop' : {
        'center' : Position(0,0,scale),
        'color' : 'purple'
    },
    'Energy' : {
        'center' : Position(scale,0,0),
        'color' : 'linen'
    },
    'Environment' : {
        'center' : Position(scale,0,0),
        'color' : 'green'
    },

    'Health': {
        'center': Position(scale, 0, 0),
        'color': 'red'
    },
    'Other' : {
        'center' : Position(0,scale,0),
        'color' : 'gray'
    },
    'Unknown' : {
        'center' : Position(0,scale,0),
        'color' : 'gray'
    },
}

def getPaths(G):
    roots = (v for v, d in G.in_degree() if d == 0)
    leaves = [v for v, d in G.out_degree() if d == 0]
    longestPath=0
    longestPathList = None
    for root in roots:
        paths = nx.all_simple_paths(G, root, leaves)
        for path in paths:
            if len(path) > longestPath:
                longestPath = len(path)
                longestPathList = path

    return longestPathList
    #for path in all_paths:
    #    print(path)

def getCycles(G):
    largestCycleLen = 0
    largestCycle = None
    zcycles = list(nx.simple_cycles(G))
    all_users_in_cycles = []
    for cycle in zcycles:
        #if len(cycle) > 3:
        #    print("cycle: ",cycle)
        if len(cycle)>largestCycleLen:
            largestCycleLen = len(cycle)
            largestCycle = cycle
        #for user in cycle:
        #    if user not in all_users_in_cycles:
        #        all_users_in_cycles.append(user)
    print("largest cycle: ",largestCycle)
    return largestCycle

def removeFromGraph(G,users):
    remove = []
    for znode in list(G.nodes):
        if znode not in users:
            remove.append(znode)
    G.remove_nodes_from(remove)
    return G


def n_trading_partners_calc(wid,atransactions, exclude_list):

    scaleX = 3
    transactions = atransactions[wid]
    result = set()

    for t in transactions:
        if t['to'] != wid and t['to'] not in exclude_list: #!= c_fund:
            result.add(t['to'])
        elif t['from'] != wid and t['from'] not in exclude_list:
            result.add(t['from'])
    #max_tier = 6
    tier = len(result)
    #if tier > max_tier:
    #    tier = max_tier
    return tier*(scaleX/2.0)#/max_tier

def displayGraphs(mainGraph, graphs,userData,private=False,days_str=""):
    figureIndex = 2
    n = 1

    num_graphs = len(graphs)

    nrows = 1
    ncols = 1

    if num_graphs > 1:
        nrows = int(math.sqrt(num_graphs))
        ncols = int(math.sqrt(num_graphs))

        if num_graphs %2 != 0:
            ncols+=1

        area = ncols * nrows

        while area < num_graphs:
            nrows+=1
            area = ncols * nrows



    fig, axes = plt.subplots(nrows=nrows, ncols=ncols)
    ax = ''
    if num_graphs > 1:
        ax = axes.flatten()

    axs_iter = 0

    for graph in graphs:

        G = removeFromGraph(mainGraph.copy(),graph)

        #if len(list(G.nodes()))<5 or len(list(G.nodes()))>200000:
        #    continue

        #print("graphing: ", len(list(graph))," graph:",end='')

        #plt.figure(figureIndex, figsize=(8, 8))
        figureIndex +=1

        pos=nx.spring_layout(G,k=0.8,iterations=50,scale=2)#,k=4,iterations=50,scale=1.5)

        hubLabels = {}
        otherLabels = {}
        color_map = []
        maxDegree = 0
        for node in G.nodes():
            business_type = userData[node].get('_name', '')
            if business_type in business_colors.keys():
                zcolor=business_colors[business_type]
            else:
                zcolor = 'grey'
                #print(business_type)
            color_map.append(zcolor)
            #if nodes[node].volume > maxVol:
            #    maxVol = nodes[node].volume
            if G.degree(node) > maxDegree:
                maxDegree = G.degree(node)
        #print("Largest Degree: ",maxDegree, userData[node].get('first_name', '')+"_"+userData[node].get('last_name', ''))
        for node in G.nodes():
            #if nodes[node].volume>0.5*maxVol:
            if G.degree(node)>2:
                if private:
                    hubLabels[node] = userData[node].get('first_name', '') + " " + userData[node].get('last_name', '')+" "+userData[node].get('_phone', '').replace('+254','0')
                else:
                    hubLabels[node] = userData[node].get('blockchain_address', '')
            else:
                if private:
                    otherLabels[node] = userData[node].get('first_name', '') + " " + userData[node].get('last_name', '')+" "+userData[node].get('_phone', '').replace('+254','0')
                else:
                    otherLabels[node] = userData[node].get('blockchain_address', '')

        if num_graphs > 1:
            nx.draw_networkx_nodes(G, pos, node_color=color_map, node_size=800, alpha=0.35, ax=ax[axs_iter])
            nx.draw_networkx_edges(G, pos, alpha=0.15, ax=ax[axs_iter])
            nx.draw_networkx_labels(G, pos, otherLabels, font_size=8, font_color='b', alpha=0.8, ax=ax[axs_iter])
            nx.draw_networkx_labels(G, pos, hubLabels, font_size=8, font_color='r', font_weight='bold', alpha=0.8,
                                    ax=ax[axs_iter])
            ax[axs_iter].set_axis_off()

            x_values, y_values = zip(*pos.values())
            x_max = max(x_values)
            x_min = min(x_values)
            x_margin = (x_max - x_min) * 0.50
            ax[axs_iter].set_xlim(x_min - x_margin, x_max + x_margin)

        else:
            nx.draw_networkx_nodes(G, pos, node_color=color_map, node_size=800, alpha=0.35)
            nx.draw_networkx_edges(G, pos, alpha=0.15)
            nx.draw_networkx_labels(G, pos, otherLabels, font_size=8, font_color='b', alpha=0.8)
            nx.draw_networkx_labels(G, pos, hubLabels, font_size=8, font_color='r', font_weight='bold', alpha=0.8)
            plt.axis('off')
            plt.tight_layout()
        # plt.show()
        shapes = ['box', 'polygon', 'ellipse', 'oval', 'circle', 'egg', 'triangle', 'exagon', 'star', ]
        colors = ['blue', 'black', 'red', '#db8625', 'green', 'gray', 'cyan', '#ed125b']
        styles = ['filled', 'rounded', 'rounded, filled', 'dashed', 'dotted, bold']
        mng = plt.get_current_fig_manager()
        mng.full_screen_toggle()

        n += 1
        axs_iter += 1
        #plt.show(block=True)
    fileName = "network_graph_public_" + str(n - 1) + "_" + days_str + ".svg"
    if private == True:
        fileName = "network_graph_private_" + str(n - 1) + "_" + days_str + ".svg"
    print("****saved network viz ", fileName)
    plt.savefig(fileName, bbox_inches='tight')

def get_balance(wid,db_cache):
    balance = 0
    if wid in db_cache.token_balances.keys():
        for v in db_cache.token_balances[wid].values():

            if v == "":
                print(wid, " empty string!", db_cache.token_balances[wid])
            else:
                balance += float(v)

        balance = int(balance / 10 ** 18)
    return balance

def getPoint(tier):
    global scale
    u = random.random()
    v = random.random()
    theta = u * 2.0 * math.pi
    phi = math.acos(2.0 * v - 1.0)
    r = scale/150*tier#math.pow(tier,1/3)#math.cbrt()
    sinTheta = math.sin(theta)
    cosTheta = math.cos(theta)
    sinPhi = math.sin(phi)
    cosPhi = math.cos(phi)
    x = r * sinPhi * cosTheta
    y = r * sinPhi * sinTheta
    z = r * cosPhi
    return {'x': x, 'y': y, 'z': z}


def toGraph(txnData, userData, start_date = None, end_date=None,private=False):

    token_transactions = txnData
    transactions = token_transactions

    nodes = dict()

    line_weights = dict()

    tx_hash=[]
    if True: #for tnsfer_acct__id, trans in transactions.items():


        for t in transactions:

            #hash = t['blockchain_task_uuid']
            #if hash in tx_hash:
            #    continue
            #tx_hash.append(hash)

            if t['transfer_subtype'] != 'STANDARD':
                continue

            sender_user_id = t['sender_user_id']
            recipient_user_id = t['recipient_user_id']

            if sender_user_id == None:
                sender_user_id = 1

            if recipient_user_id == None:
                recipient_user_id = 1

            trade_amt = float(t['_transfer_amount_wei'])

            to_key = recipient_user_id
            from_key = sender_user_id

            key = (from_key, to_key)# \
                #if from_key < to_key \
                #else (to_key, from_key)

            if key not in line_weights:
                line_weights[key] = 1#trade_amt
            else:
                line_weights[key] += 1#trade_amt

    G = nx.DiGraph()  # nx.karate_club_graph() #    G = nx.karate_club_graph() #

    for (sender_user_id, recipient_user_id), weight in line_weights.items():
        if(weight>=1):
            G.add_edge(sender_user_id, recipient_user_id,weight=weight)  # * num_weighs/all_weights
            s_first_name = userData[sender_user_id].get('first_name', '')
            s_last_name = userData[sender_user_id].get('last_name', '')
            s_phone = userData[sender_user_id].get('_phone', '')
            s_directory = userData[sender_user_id].get('bio', '').strip('"')
            s_gender = userData[sender_user_id].get('gender', '').strip('"')
            s_business_type = userData[sender_user_id].get('_name', '')
            s_location = userData[sender_user_id].get('_location', '')

            G.add_node(sender_user_id,directory=s_directory,gender=s_gender,biz_type=s_business_type,first_name=s_first_name,last_name=s_last_name,phone=s_phone,label=sender_user_id,location=s_location)

            t_first_name = userData[recipient_user_id].get('first_name', "_")
            t_last_name = userData[recipient_user_id].get('last_name', "_")
            t_phone = userData[recipient_user_id].get('_phone', "_")
            t_directory = userData[recipient_user_id].get('bio', '').strip('"')
            t_gender = userData[recipient_user_id].get('gender', '').strip('"')
            t_business_type = userData[recipient_user_id].get('_name', '')
            t_location = userData[recipient_user_id].get('_location', '')

            G.add_node(recipient_user_id,directory=t_directory,gender=t_gender,biz_type=t_business_type,first_name=t_first_name,last_name=t_last_name,phone=t_phone,label=recipient_user_id,location=t_location)

    return G
#//////////////////////////////////////////////////////////////////////////////////////////////////
def output_Network_Viz(G, userData, start_date,end_date,days_ago_str):
    print("NetworkVis....")
    #G  = toGraph(txnData, userData, start_date, end_date,private)

    #print("Clustering")
    #Gc = nx.clustering(G)
    #print(Gc)
    #G.remove_nodes_from(remove)
    #G = nx.random_geometric_graph(200, 0.125)



    d = list(nx.strongly_connected_components(G))
    largest_sub_graph = None
    size_of_largest = 0
    user_data_new = []
    sub_graphs = []
    print_graphs = []

    filename = 'sarafu_network_data_all_admin_pub_'+start_date.strftime("%Y%m%d")+"-"+end_date.strftime("%Y%m%d")+"-"+days_ago_str+'.csv'
    with open(filename, 'w',newline='') as csvfile:
        writerT = csv.writer(csvfile)
        userHeaders = ["cluster_nodes", "male", "female", "unknown", "volume", "location", "location_strength", "clustering"]
        writerT.writerow(userHeaders)
        #print("cluster_nodes", ",",  "male", ",", " female", ",", " unknown",  ",", "volume", ",", "location", ",", "location_strength", ",", "clustering")
        #print(userHeaders) #debug



        for sub_graph in d:
            if len(list(sub_graph)) > 1:
                Ga = removeFromGraph(G.copy(), sub_graph)
                clustering = nx.average_clustering(Ga)
                num_male = 0
                num_female = 0
                num_unknown = 0
                locations = {}
                for node in Ga.nodes():
                    zgender = userData[node].get('gender', '').strip('"')
                    zlocation = userData[node].get('_location', '')
                    if zlocation not in locations.keys():
                        locations.update({zlocation: 1})
                    else:
                        locations[zlocation] = locations[zlocation]+1

                    if zgender == "female":
                        num_female +=1
                    elif zgender == "male":
                        num_male +=1
                    else:
                        num_female +=1
                total_weight = 0
                #total_txn = 0
                for edge in Ga.edges():
                    #print("test: ",edge)
                    total_weight += Ga.get_edge_data(edge[0],edge[1])['weight']
                    #total_txn += Ga.get_edge_data(edge[0], edge[1])['txn']
                #print("sub_graph size: ", len(list(sub_graph)), " male: ", num_male,  " female: ", num_female,  " unknown: ", num_unknown, " total weight: ", total_weight)
                sorted_locs = sorted(locations.items(), key=lambda x: x[1], reverse=True)

                    # writerT.writerow([str(user_data.get(attr, '')).strip('"') for attr in userHeaders])
                networkData = {"cluster_nodes":len(list(sub_graph)), "male":num_male, "female":num_female, "unknown":num_unknown, "volume":total_weight, "location":sorted_locs[0][0], "location_strength":sorted_locs[0][1], "clustering":clustering}
                #print(len(list(sub_graph)), ",", num_male, ",", num_female, ",", num_unknown,  ",", total_weight,",",
                #      sorted_locs[0][0], "," , sorted_locs[0][1], ",", clustering)
                zRow = list()
                for attr in networkData:
                    zRow.append(str(networkData.get(attr, '')).strip('"'))
                writerT.writerow(zRow)

            if len(list(sub_graph)) > 3 and len(list(sub_graph)) < 1000:
                #print(len(list(sub_graph))," sub_graph:",end='')
                print_graphs.append(sub_graph)
                #for usr in sub_graph:
                #    print(userData[usr].get('first_name', '')+", "+userData[usr].get('last_name', '')+", "+userData[usr].get('_phone', '')+", "+userData[usr].get('bio', '').strip('"')+" ",end='')
                #print(" ")
            if len(list(sub_graph)) > size_of_largest:
                size_of_largest = len(list(sub_graph))
                largest_sub_graph = list(sub_graph)

    print("****saved all network info to csv: ", filename, "***")
        #Gc = removeFromGraph(G.copy(),largest_sub_graph)
    #gephiName= "test5.gexf"


    #print("gephi output saved: ",gephiName, " ", Gc.size(weight="weight"), "users:", len(Gc))

    #nx.write_gexf(Gc, gephiName)

    #displayGraphs(G,print_graphs, userData,False,"_misc")
    #if size_of_largest < 1000:
    #    displayGraphs(G,[largest_sub_graph], userData,False,"_misc")

    #return a dicts of users and their cluster coefs
    return