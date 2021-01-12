import datetime
from datetime import datetime
from itertools import islice
import os
import re
import logging


class data_manipulation():

    def __init__(self,destination_folder):

        global fl

        print('FIles in folder\n',os.listdir(destination_folder))

        fl = input('Enter the filename\t')

    def parse_csv(self,destination_folder,connection):

        csv_file=open(destination_folder+fl)

        count=0

        for row in csv_file:                 #    for row in islice(fl,1000):
            if count<1:
                columns=row.rstrip().split(',')
                count+=1
                
                print(columns)
            else:

                val=row.rstrip().split(',')

                dt1=datetime.strptime(val[5], '%m/%d/%Y').date()
                dt2=datetime.strptime(val[7], '%m/%d/%Y').date()

                val[5]=dt1
                val[7]=dt2   

                # print(val)
            ### this the part where we use all the parsed csv and insert it to target database
            ### we are not going to make new connection code to execute query, but we call methods from module database_connection to do the job
            ### since we'll call this class in main.py no need to import module connection here
            ###         

                params= val
                
                insert_sql='''INSERT INTO Sales ({},{}, `{}`, `{}`, `{}`, `{}`, `{}`, `{}`,`{}`, `{}`, `{}`, `{}`, `{}`, `{}`)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''.format(*columns) 
                    # Iterable Unpacking
                    #* to unpacked list,tuple,** unpacked dict
                
                os.system('clear')
                print('inserting data to database')

                connection.execute_query(insert_sql,params)

                connection.commit()
