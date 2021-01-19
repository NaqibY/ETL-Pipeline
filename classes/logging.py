import logging

def logger():
    '''
        Basic logging file
            initailise the logger by adding logger=logger() at the top of your script
        '''

    logging.basicConfig(filename='logs.log',level=logging.INFO,format='%(asctime)s %(levelname)-8s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S')
    
    logger=logging.getLogger(__name__)

    return logger