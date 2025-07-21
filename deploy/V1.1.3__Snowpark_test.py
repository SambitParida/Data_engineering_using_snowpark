#Import os Library
import os

path = r"/Users/sambitparida/Desktop/Sambit/Learning/SnowflakePractice/data/amazon_sales_data/sales/"
#Travers all the branch of a specified path
for (root,dirs,files) in os.walk(path,topdown=True):
  print("Directory path: %s"%root)
  print("Directory Names: %s"%dirs)
  print("Files Names: %s"%files)