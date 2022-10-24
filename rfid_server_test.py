# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 00:01:08 2022

@author: groja
"""

import socket
import psycopg2 as pg
import datetime
import pandas.io.sql as psql

d1 = datetime.datetime.now()
Fs = 10
##192.168.1.100

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
s.bind(('0.0.0.0',9002))
s.listen(0)

def database_sending(uid,lector):
    #connect to db
    con = pg.connect(
        host = '152.74.29.160',
        user = 'postgres',
        password = 'somno2019',
        port = '9001',
        database = 'somno')

    #cursor
    cur = con.cursor()
    try:
        df_uid = psql.read_sql('SELECT * FROM uid_assigns', con)
    
        df_lector = psql.read_sql('SELECT * FROM uid_lector', con)
    
        selected_user = list(df_uid['user_id'].loc[df_uid['uid'] == uid])[0]
        type_user = list(df_uid['type_med'].loc[df_uid['uid'] == uid])[0]
        pct = list(df_lector['pct_id'].loc[df_lector['lector_id'] == lector])[0]
        
        insert_script = 'INSERT INTO uid_rfid (uid,user_med,user_pct,type_med,lector_id) VALUES (%s,%s,%s,%s,%s)'
        insert_values = (uid,selected_user,pct,type_user,lector)
        cur.execute(insert_script,insert_values)
        con.commit()
    except:
        print('Tarjeta o lector no ingresado')
    
while True:
    client, addr = s.accept()
    
    while True:
        
        content = client.recv(164*Fs)
        
        if len(content) == 0:
            break
        else:
            
            print(str(content).replace('b',''))
            info = str(content).replace('b','')
            info_list = str(content).replace('b','').replace("'","").replace(":","").split(' | ')
            print(info_list)
            database_sending(info_list[0],info_list[1])
            
    print('closing connection')
    client.close()
    
    
    
    
    
    
    
    
    
    
    
    