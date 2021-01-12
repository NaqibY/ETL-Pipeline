import datetime
import json
import os

from module.connection import connection# import Module.data_handler
from module.files import file_handler
from module.data import data_manipulation
# import Module.Log_activity


# this the main scrript that will orchestrate our etl

with open('setup/config.json', 'r') as json_file:
    config=json.load(json_file)

# use mysql config 
# add arguments in config file as such

    ''' "mysql": {
                    "USER":mysql_username,
                    "HOST":host_address,
                    "DATABASE": names_of_database,
                    "PASSWORD": mysql_password,
                    "download_url":"http://eforexcel.com/wp/wp-content/uploads/2017/07/50000-Sales-Records.zip",
                    "destination_folder":"/home/naqib/Documents/programming/Coursera/project/ETL Python/setup/data/"
    }'''

# establish target database

db_connection=connection(USER=config['mysql']['USER'],
                            HOST=config['mysql']['HOST'],
                            DATABASE=config['mysql']['DATABASE'],
                            PASSWORD=config['mysql']['PASSWORD'])
# begin extraction

extract=file_handler()

extract.download_data(download_url=config['mysql']['download_url'],
                        destination_folder=config['mysql']['destination_folder'])

extract.extract_file(download_url=config['mysql']['download_url'],
                        destination_folder=config['mysql']['destination_folder'])

# before loading to our database, run this query to create new table

db_connection.init_query('''DROP TABLE IF EXISTS Sales''')

db_connection.init_query(query=(
    '''CREATE TABLE Sales(
        Region varchar(256),
        Country varchar(256),
        `Item Type` varchar(20),
        `Sales Channel` varchar(20),
        `Order Priority` varchar(20),
        `Order Date` DATE NOT NULL,
        `Order ID` int(20) NOT NULL,
        `Ship Date` DATE NOT NULL,
        `Units Sold` int(20) NOT NULL,
        `Unit Price` int(20) NOT NULL,
        `Unit Cost` int(20) NOT NULL,
        `Total Revenue` int(20) NOT NULL,
        `Total Cost` int(20) NOT NULL,
        `Total Profit` int(20) NOT NULL)'''))

# now import data_handler module to perform transform and load
transform_load =data_manipulation(destination_folder=config['mysql']['destination_folder'])

transform_load.parse_csv(destination_folder=config['mysql']['destination_folder'],connection=db_connection)


# finish
db_connection.close()
os.system('clear')

print('ETL process is complete')