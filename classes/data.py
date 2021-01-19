import datetime
import os
from datetime import datetime
from itertools import islice
from classes.logging import logger

logger=logger()


def csv_load_to_db(filename,destination_folder,connection):
    """
    parse csv file and execute query to load into database.
    
    Arg:
        1. filename = name of csv file
        2. destination_folder= downloaded files directory
        3. connection = connection(HOST,HOST,DATABASE,PASSWORD)
    
    """    
    csv_file=open(destination_folder+filename)

    count_header,count_row=0,0

    for row in csv_file:                 #    for row in islice(fl,1000):
        if count_header<1:
            columns=row.rstrip().split(',')
            count_header+=1
            
            print(columns)
        else:

            val=row.rstrip().split(',')

            dt1=datetime.strptime(val[5], '%m/%d/%Y').date()
            dt2=datetime.strptime(val[7], '%m/%d/%Y').date()

            val[5]=dt1
            val[7]=dt2

            count_row+=1   

            # print(val)
        ### this the part where we use all the parsed csv and insert it to target database
        ### we are not going to make new connection code to execute query, but we call methods from module connection to do the job
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

    logger.info("{}'s rows loaded into database".format(count_row))
