# Dashboard
Public dashboard for anonymous CIC user transactions and meta data 






gpg -d sarafu_latest.tar.gz.gpg > sarafu_latest.tar.gz
tar -zxvf sarafu_latest.tar.gz


sudo -s
su -l postgres
createdb postgres

dropdb -U postgres postgres
dropdb -U postgres eth_worker
psql -h 127.0.0.1 -U postgres --password -d postgres -f ge_postgres_1583179201.sql
psql -h 127.0.0.1 -U postgres --password -d eth_worker -f ge_ethworker_1583179201.sql

.
