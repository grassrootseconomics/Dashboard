import os
import psycopg2
import getopt
import sys
from toolkit import Date

opts, _ = getopt.getopt(sys.argv[1:], 'a:h:u:p:', ['public'])

dbname=os.environ.get('DBNAME')
dbuser=os.environ.get('DBUSER')
dbpass=os.environ.get('DBPASS')

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



GE_community_token_id_map = {1:"Sarafu",
                             2:"foo"}

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
            tDict[h] = r

        date_good = True
        if start_date != None and end_date != None:
                # print(r) #2020-01-25 19:13:17.731529
                if tDict['created'].date() > end_date or tDict['created'].date() < start_date:
                    date_good = False
        if date_good:
            if tDict['user_id'] in txnDict.keys():
                txnDict[tDict['user_id']].append(tDict)
            else:
                txnDict.update({tDict['user_id']:[tDict]})

    return {'headers':txDBheaders, 'data': txnDict}




#conn = psycopg2.connect(
#        f"""
#        dbname=sarafu_app
#        user={dbuser}
#        host={dbname}
#        """)


print(dbpass)
if dbpass == None:
    conn = psycopg2.connect(
            f"""
            dbname=sarafu_app
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
                    #rowIt.append(str(user.get(attr, '')).encode('utf8'))
                    rowIt.append(user.get(attr, ''))
                writerT.writerow(rowIt)
            #writerT.writerow([str(user.get(attr, '')).strip('"') for attr in ussdHeaders])
