import mysql.connector
from mysql.connector import errorcode
from classes.logging import logger

logger=logger()

class connection():
    """
    Established connection to database
    Arg:
        1. USER
        2. HOST
        3. DATABASE
        4. PASSWORD
    """
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
            logger.info('connection database success')
            print('Connection success')


        except mysql.connector.Error as err:

            if err.errno==errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error('umm something wrong with user name or password')
                print('umm something wrong with user name or password')

            
            elif err.errno==errorcode.ER_BAD_DB_ERROR:
                logger.error('database does not exist')
                print('database does not exist')

            
            else:
                logger.error(err)
                print(err)

    
    def init_query(self,query):
        """
        Execute initial query that does not require parameter

        Arg:
            1. query
        """
        cur=self.conn.cursor()
        cur.execute(query)

    def execute_query(self, query, params):
        """
        Execute query

        Arg:
            1. query 
            2. param = values
        """
        cur=self.conn.cursor()
        cur.execute(query,params)

    def commit(self):
        """
        Commits to changes in database
        
        """
        self.conn.commit()

    def close(self):
        """
        Close connection
        
        """
        self.conn.close()