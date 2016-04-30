import os
import sys
import math

# Path for spark source folder
os.environ['SPARK_HOME']="/Users/anshuman/Downloads/spark-1.6.0/"

# Appending pyspark  to Python Path
sys.path.append("/Users/anshuman/Downloads/spark-1.6.0/python")


from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd

sc = SparkContext(appName = "Loading Data") # if using locally
sql_sc = SQLContext(sc)

pandas_df = pd.read_csv('Collision_Data.csv')  # assuming the file contains a header
# pandas_df = pd.read_csv('file.csv', names = ['column 1','column 2']) # if no header
s_df = sql_sc.createDataFrame(pandas_df)

#s_df.where($"time" like )
s_df.printSchema()


s_df.registerTempTable("s_df");
#for x in range(1,23):
#	print "Time of day is " + str(x)
#	sql_sc.sql("select BOROUGH,COUNT(*) from s_df where TIME like '" + str(x) + ":%' group By BOROUGH").show()


factormap = {}

df1 = sql_sc.sql("select `CONTRIBUTING FACTOR VEHICLE 1` as `CONTRIBUTINGFACTOR`,COUNT(*) as count from s_df Where `NUMBER OF PERSONS KILLED` > 0 group By `CONTRIBUTING FACTOR VEHICLE 1`")
df1.show();
pdf = df1.toPandas();
for index, row in pdf.iterrows():
	if row['CONTRIBUTINGFACTOR'] in factormap:
		factormap[row['CONTRIBUTINGFACTOR']] = factormap[row['CONTRIBUTINGFACTOR']] + row['count'];
	else:
		factormap[row['CONTRIBUTINGFACTOR']] = row['count'];


df2 = sql_sc.sql("select `CONTRIBUTING FACTOR VEHICLE 2` as `CONTRIBUTINGFACTOR`,COUNT(*) as count from s_df Where `NUMBER OF PERSONS KILLED` > 0 group By `CONTRIBUTING FACTOR VEHICLE 2`")
df2.show();
pdf = df2.toPandas();
for index, row in pdf.iterrows():
	if row['CONTRIBUTINGFACTOR'] in factormap:
		factormap[row['CONTRIBUTINGFACTOR']] = factormap[row['CONTRIBUTINGFACTOR']] + row['count'];
	else:
		factormap[row['CONTRIBUTINGFACTOR']] = row['count'];


df3 = sql_sc.sql("select `CONTRIBUTING FACTOR VEHICLE 3` as `CONTRIBUTINGFACTOR`,COUNT(*) as count from s_df Where `NUMBER OF PERSONS KILLED` > 0 group By `CONTRIBUTING FACTOR VEHICLE 3`")
df3.show();
pdf = df3.toPandas();
for index, row in pdf.iterrows():
	if row['CONTRIBUTINGFACTOR'] in factormap:
		factormap[row['CONTRIBUTINGFACTOR']] = factormap[row['CONTRIBUTINGFACTOR']] + row['count'];
	else:
		factormap[row['CONTRIBUTINGFACTOR']] = row['count'];


df4 = sql_sc.sql("select `CONTRIBUTING FACTOR VEHICLE 4` as `CONTRIBUTINGFACTOR`,COUNT(*) as count from s_df Where `NUMBER OF PERSONS KILLED` > 0 group By `CONTRIBUTING FACTOR VEHICLE 4`")
df4.show();
pdf = df4.toPandas();
for index, row in pdf.iterrows():
	if row['CONTRIBUTINGFACTOR'] in factormap:
		factormap[row['CONTRIBUTINGFACTOR']] = factormap[row['CONTRIBUTINGFACTOR']] + row['count'];
	else:
		factormap[row['CONTRIBUTINGFACTOR']] = row['count'];

print factormap

vehiclemap = {}

df1 = sql_sc.sql("select `VEHICLE TYPE CODE 1` as `VEHICLETYPE`,COUNT(*) as count from s_df group By `VEHICLE TYPE CODE 1")
df1.show();
pdf = df1.toPandas();
for index, row in pdf.iterrows():
	if row['VEHICLETYPE'] in vehiclemap:
		vehiclemap[row['VEHICLETYPE']] = vehiclemap[row['VEHICLETYPE']] + row['count'];
	else:
		vehiclemap[row['VEHICLETYPE']] = row['count'];


df2 = sql_sc.sql("select `VEHICLE TYPE CODE 2` as `VEHICLETYPE`,COUNT(*) as count from s_df group By `VEHICLE TYPE CODE 2")
df2.show();
pdf = df2.toPandas();
for index, row in pdf.iterrows():
	if row['VEHICLETYPE'] in vehiclemap:
		vehiclemap[row['VEHICLETYPE']] = vehiclemap[row['VEHICLETYPE']] + row['count'];
	else:
		vehiclemap[row['VEHICLETYPE']] = row['count'];


df3 = sql_sc.sql("select `VEHICLE TYPE CODE 3` as `VEHICLETYPE`,COUNT(*) as count from s_df group By `VEHICLE TYPE CODE 3")
df3.show();
pdf = df3.toPandas();
for index, row in pdf.iterrows():
	if row['VEHICLETYPE'] in vehiclemap:
		vehiclemap[row['VEHICLETYPE']] = vehiclemap[row['CONTRIBUTINGFACTOR']] + row['count'];
	else:
		vehiclemap[row['VEHICLETYPE']] = row['count'];


df4 = sql_sc.sql("select `VEHICLE TYPE CODE 4` as `VEHICLETYPE`,COUNT(*) as count from s_df group By `VEHICLE TYPE CODE 4")
df4.show();
pdf = df4.toPandas();
for index, row in pdf.iterrows():
	if row['VEHICLETYPE'] in vehiclemap:
		vehiclemap[row['VEHICLETYPE']] = vehiclemap[row['CONTRIBUTINGFACTOR']] + row['count'];
	else:
		vehiclemap[row['VEHICLETYPE']] = row['count'];

df5 = sql_sc.sql("select `VEHICLE TYPE CODE 5` as `VEHICLETYPE`,COUNT(*) as count from s_df group By `VEHICLE TYPE CODE 5")
df5.show();
pdf = df1.toPandas();
for index, row in pdf.iterrows():
	if row['VEHICLETYPE'] in vehiclemap:
		vehiclemap[row['VEHICLETYPE']] = vehiclemap[row['VEHICLETYPE']] + row['count'];
	else:
		vehiclemap[row['VEHICLETYPE']] = row['count'];

print vehiclemap



# To group by location, we can join lat and long columns and then do a group by on then
#df5 = sql_sc.sql("select df1.`CONTRIBUTING FACTOR`, df1.count + df2.count + df3.count FROM df1 left outer join df2 on df1.`CONTRIBUTING FACTOR` = df2.`CONTRIBUTING FACTOR` left outer join df3 on df2.`CONTRIBUTING FACTOR`=df3.`CONTRIBUTING FACTOR` ");
#df5.show();


#s_df.filter(s_df['TIME'] == '2:40').show()




#s_df.filter("TIME" like "23:%%").show()
# s_df.select("BOROUGH").show()


# s_df.groupBy("BOROUGH").count().show()
