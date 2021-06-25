import sys
from os.path import dirname, abspath
folder_path = dirname(dirname(abspath(__file__)))
sys.path.insert(1, folder_path)


inputs = {"dataset_name":"",

        }

# def make_mce():
#     send request
#     generate_mce_func
#     compare with golde mce that i will create