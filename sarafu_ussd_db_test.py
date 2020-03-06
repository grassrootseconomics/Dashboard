import os
import psycopg2


GE_community_token_id_map = {1:"Sarafu",
                             2:"foo"}

#return a list of every users transactions keyed by user
def get_ussd_txns(conn):
    txDBheaders = ['id','service_code','msisdn','user_input','state','ussd_menu_id','user_id','created','session_id']

    txItems = ', '.join(["ussd_session." + s for s in txDBheaders])
    cur = conn.cursor()
    cmd = "SELECT "+txItems+" FROM ussd_session"
    cur.execute(cmd)
    rows = cur.fetchall()

    txnDict = {}
    for row in rows:
        tDict = {}
        for h, r in zip(txDBheaders,row):
            tDict[h]=r
        if tDict['user_id'] in txnDict.keys():
            txnDict[tDict['user_id']].append(tDict)
        else:
            txnDict.update({tDict['user_id']:[tDict]})

    return {'headers':txDBheaders, 'data': txnDict}



dbname=os.environ.get('DBNAME')
dbuser=os.environ.get('DBUSER')

conn = psycopg2.connect(
        f"""
        dbname=postgres
        user={dbuser}
        host={dbname}
        """)



ussdResult = get_ussd_txns(conn)
ussdHeaders = ussdResult['headers']
ussdData = ussdResult['data']

if True:

    import csv
    filename = 'sarafu_ussd_data_all_private.csv'
    with open(filename, 'w',newline='') as csvfile:
        writerT = csv.writer(csvfile)

        writerT.writerow(ussdHeaders)
        print(ussdHeaders)
        for user_id, user_data in ussdData.items():
            for user in user_data:
                rowIt = []
                for attr in ussdHeaders:
                    rowIt.append(str(user.get(attr, '')).encode('utf8'))
                writerT.writerow(rowIt)
            #writerT.writerow([str(user.get(attr, '')).strip('"') for attr in ussdHeaders])
