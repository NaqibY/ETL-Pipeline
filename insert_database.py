import mysql.connector
from mysql.connector import errorcode
from datetime import date
import datetime
from itertools import islice

fl=open('dataset/500000 Sales Records.csv')

conn=mysql.connector.connect(user='dbeaver_mysql', host='localhost', database='sales_records', password='')

cur=conn.cursor()

# init_query= (
#     'DROP TABLE IF EXISTS Sales'

# "    CREATE TABLE Sales (
#         %s VARCHAR(24),
#         %s VARCHAR(24),
#         %s VARCHAR(24),
#         %s VARCHAR(24),
#         %s VARCHAR(24),
#         %s DATE NOT NULL,
#         %s INT NOT NULL,
#         %s DATE NOT NULL,
#         %s INT NOT NULL,
#         %s INT NOT NULL,
#         %s INT NOT NULL,
#         %s INT NOT NULL,
#         %s INT NOT NULL,
#         %s INT NOT NULL)"
# )

column=['Region','Country']

init_query=((
    'DROP TABLE IF EXISTS Sales'
)
)
query=(
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
        `Total Profit` int(20) NOT NULL)'''
)


column=('Region','Country', 'Item Type', 'Sales Channel', 'Order Priority', 'Order Date', 'Order ID', 'Ship Date',
'Units Sold', 'Unit Price', 'Unit Cost', 'Total Revenue', 'Total Cost', 'Total Profit')


try:
    print('Creating table sales')
    cur.execute(init_query)
    cur.execute(query)

except mysql.connector.Error as err:
    print('Failed creating database {}'.format(err))
    exit(1)

try:
    count=0
    for row in fl:                 #    for row in islice(fl,1000):

        if count<1:
            columns=row.rstrip().split(',')
            count+=1
        else:
            val=row.rstrip().split(',')
            print(row)

            dt1=val[5]
            dt2=val[7]

            dt_split1=dt1.split('/')
            dt_split2=dt2.split('/')

            x =lambda a: datetime.date(int(a[-1]),int(a[0]),int(a[1]))

            val[5]=x(dt_split1)
            val[7]=x(dt_split2)     

            cur.execute(
                '''INSERT INTO Sales(Region,Country, `Item Type`, `Sales Channel`, `Order Priority`, `Order Date`, `Order ID`, `Ship Date`,
                `Units Sold`, `Unit Price`, `Unit Cost`, `Total Revenue`, `Total Cost`, `Total Profit`)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(val)
                )
            conn.commit()

except mysql.connector.Error as err:
    print('Importing data failed {}'.format(err))
    exit(1)

print('transfer complete')
conn.close()
 
