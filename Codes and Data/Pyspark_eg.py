# -*- coding: utf-8 -*-
"""
Created on Sun Oct  4 21:24:53 2020

@author: Divya Choudhary
"""

import findspark
findspark.init()
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession

sc = SparkSession.builder.master("local[4]").appName("read_csv").getOrCreate()

t1 = datetime.now()
read_spark = sc.read.csv('D:\Self Projects\MySkillMantra\Big Data Analytics\india-news-headlines.csv')

t2 = datetime.now()
read_pandas = pd.read_csv('D:\Self Projects\MySkillMantra\Big Data Analytics\india-news-headlines.csv')

t3 = datetime.now()

print( 'Read time in PySpark = ', (t2-t1).total_seconds(), 'seconds')
print( 'Read time in Pandas = ', (t3-t2).total_seconds(), 'seconds')

sc.stop()
