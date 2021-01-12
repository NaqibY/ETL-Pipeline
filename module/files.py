import os
import urllib.request
from zipfile import ZipFile
import logging

class file_handler():

    def create_folder(self,destination_folder):
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)

    def download_data(self,download_url,destination_folder):
        self.create_folder(destination_folder)

        urllib.request.urlretrieve(download_url, destination_folder+download_url.split('/')[-1])

    
    def extract_file(self,download_url,destination_folder):

        with ZipFile(destination_folder+download_url.split('/')[-1]) as zipobj:
            zipobj.extractall(destination_folder)

            print('downlaod and extraction complete')



# download_url='http://eforexcel.com/wp/wp-content/uploads/2017/07/50000-Sales-Records.zip'
# destination_folder='/home/naqib/Documents/programming/Coursera/project/ETL Python/Setup/Data/'
# x=file_handler()
# x.download_data(download_url,destination_folder)
# x.extract_file(download_url,destination_folder)
