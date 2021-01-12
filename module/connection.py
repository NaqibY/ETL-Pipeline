import mysql.connector
from mysql.connector import errorcode
import logging


class connection():

    USER=None
    HOST=None
    DATABASE=None
    PASSWORD=None

    conn=None


    def __init__(self, USER, HOST, DATABASE, PASSWORD):


        self.USER=USER
        self.HOST=HOST
        self.DATABASE=DATABASE
        self.PASSWORD=PASSWORD


        try:
            conn=mysql.connector.connect(
                user=USER,
                host=HOST,
                database=DATABASE,
                password=PASSWORD,
            )
            self.conn=conn
            print('Connection success')


        except mysql.connector.Error as err:

            if err.errno==errorcode.ER_ACCESS_DENIED_ERROR:
                print('umm something wrong with user name or password')

            
            elif err.errno==errorcode.ER_BAD_DB_ERROR:
                print('database does not exist')

            
            else: 
                print(err)

    
    def init_query(self,query):

        cur=self.conn.cursor()
        cur.execute(query)

    def execute_query(self, query, params):
        cur=self.conn.cursor()
        cur.execute(query,params)

    def commit(self):

        self.conn.commit()

    def close(self):
        self.conn.close()
