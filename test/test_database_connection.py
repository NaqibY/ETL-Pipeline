import sys
import os
import fnmatch

sys.path.insert(0,'/home/naqib/Documents/programming/Coursera/project/ETL Python/Module')

import pytest
import unittest
from files_handler import file_handler


# def capitalise(x):
#     return x.split('/')[-1]

download_url='http://eforexcel.com/wp/wp-content/uploads/2017/07/50000-Sales-Records.zip'
destination_folder='/home/naqib/Documents/programming/Coursera/project/ETL Python/Setup/Data/'

def test_capitalise():
    destination_folder='/home/naqib/Documents/programming/Coursera/project/ETL Python/Setup/Data/'

    assert file_handler.create_folder('/home/naqib/Documents/programming/Coursera/project/ETL Python/Setup/Data/')==False