######### APENAS EXECUTAR E AGUARDAR #########
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.4.4-bin-hadoop2.7"

import findspark
findspark.init('spark-2.4.4-bin-hadoop2.7')

import pyspark
from pyspark import SparkContext as spark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate() # Creating Session
sc = spark.sparkContext                                        #Initialize Spark

df = spark.read.format('csv').option('header', 'true').load('/content/drive/MyDrive/Colab Notebooks/data_sets/house-prices-advanced-regression-techniques/train.csv')

df = df.drop('Id')
df = df.select('RoofStyle', 'RoofMatl', 'Exterior1st', 'Exterior2nd', 'MasVnrType')

from itertools import combinations

initCols = df.columns

for i in range(len(initCols)+1):
  for c in list(combinations(initCols, i+2)):
    df = df.withColumn(','.join(c), concat_ws(',', *c))

finalCols = df.columns

exprs = [size(collect_set(x)).alias(x) for x in finalCols]

df = df.withColumn("aggCol", lit("a")).groupBy("aggCol").agg(*exprs)

df.show()

df_t = spark.createDataFrame([('', '')], ['key', 'count'])


df_collect = df.collect()
df_collect[0].__fields__[0]

df_collect = df.drop('aggCol')

df_list = df_collect.collect()

df_len = len(df_list.columns)
df_header = df_list[0].__fields__
df_item = list(df_list[0])

list_dfT = []

for i in range(df_len):
  ldf.append((df_header[i], df_item[i]))

#df_t = spark.createDataFrame(ldf, ['key', 'count'])