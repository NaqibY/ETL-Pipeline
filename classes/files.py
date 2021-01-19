import os
import urllib.request
from zipfile import ZipFile
# from classes.logging import logger

# logger=logger()


def create_folder(destination_folder):
    """
    Create folder if not exist

    Arg:
        1. destination_folder= downloaded files directory
    """
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

def download_data(download_url,destination_folder):
    """
    Get file from url

    Arg:
        1. download_url
        2. destination_folder= downloaded files directory

    """
    create_folder(destination_folder)

    urllib.request.urlretrieve(download_url, destination_folder+download_url.split('/')[-1])
    print('ok')


def extract_file(download_url,destination_folder):
    """
    unzip files

    Arg:
        1. download_url
        2. destination_folder= downloaded files directory
    
    """
    with ZipFile(destination_folder+download_url.split('/')[-1]) as zipobj:
        zipobj.extractall(destination_folder)

        print('downlaod and extraction complete')

def main():
    download_url='http://eforexcel.com/wp/wp-content/uploads/2017/07/50000-Sales-Records.zip'
    destination_folder='/home/naqib/Documents/git/Simple-Python-ETL/setup/data/'

    download_data(download_url=download_url,destination_folder=destination_folder)
    extract_file(download_url,destination_folder)

if __name__=='__main__':
    main()