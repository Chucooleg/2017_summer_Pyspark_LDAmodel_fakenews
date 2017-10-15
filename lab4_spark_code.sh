# Overview [ mp4 ]
# specializes at cache-ing in memory
# Spark can work with data in local files, with data from HDFS, with data from S3
# SQL, Streaming, Machine Learning (Popular), Graph

# RDD - Resilient Distributed Dataset
# provides a programming abstraction based on what is called DataFrames
# distributed SQL query engine

##############################################################
# Step 0: Part 1 - Check installation, setup environment variables, setup path, edit .bash_profile [ mp4 ]

su - w205 # (~)

# (SKIP) check to see where Spark is installed
echo $SPARK_HOME
cd $SPARK_HOME
ls # should see spark-shell, pyspark and other tools of spark

ls -l /data # see spark15 directory
# this dir is where we will work

# let's set the executables in this directory
export SPARK=/data/spark15
export SPARK_HOME=$SPARK
export PATH=$SPARK/bin:$PATH
# check if executables are working
which spark-shell
# /data/spark15/bin/spark-shell
which pyspark
# /data/spark15/bin/pyspark
which spark-sql
# /data/spark15/bin/spark-sql

# (w205 ~)
# put the exports in the bash login
ls -la | grep bash # Read the output
"""
-rw-------  1 w205 w205      8810 Jun 11 20:58 .bash_history
-rw-r--r--  1 w205 w205        18 Oct 16  2014 .bash_logout
-rw-r--r--  1 w205 w205       176 Oct 16  2014 .bash_profile
-rw-r--r--  1 w205 w205       124 Oct 16  2014 .bashrc
"""
# .bash_profile --> run everytime I login
# .bashrc --> run whenever I spawn an regular shell
# lots of x-windows put them in .bashrc

# put export paths in .bash_profile
# load automatically when we login next time
set -o vi
vi .bash_profile
#----------------------#
# Add to End
export SPARK=/data/spark15
export SPARK_HOME=$SPARK
export PATH=$SPARK/bin:$PATH
#-----------------------#
# source the file --> runs it without creating another shell
. .bash_profile # runs it in this shell
# ./bash_profile executes in another shell, not this one --> export paths will not set correctly
env|grep SPARK
"""
SPARK_HOME=/data/spark15
SPARK=/data/spark15
"""
# log out and try again
exit
su - w205
env|grep SPARK # should be same result
env|grep PATH # make sure it has spark in it
which spark-shell
which pyspark
which spark-sql

##############################################################
# Step 0: Part 2 - Clone data files, combine and unzip csv file of crime data [ mp4 ]

# git clone data
cd ~ #(w205)
git clone https://Chucooleg@github.com/UC-Berkeley-I-School/w205-summer-17-labs-exercises
set -o vi
ls -la # shud see w205-labs-exercises
cd w205-labs-exercises
ls -l # total 80
cd data
ls -l # shud see Crimes_-_2001_to_present_data
cd Crimes_-_2001_to_present_data
ls -l # shud see bunch of sub-files
"""
-rw-rw-r-- 1 w205 w205 50000000 Jun 12 05:19 xaa
-rw-rw-r-- 1 w205 w205 50000000 Jun 12 05:19 xab
-rw-rw-r-- 1 w205 w205 50000000 Jun 12 05:19 xac
-rw-rw-r-- 1 w205 w205 50000000 Jun 12 05:19 xad
-rw-rw-r-- 1 w205 w205 50000000 Jun 12 05:19 xae
-rw-rw-r-- 1 w205 w205 50000000 Jun 12 05:19 xaf
-rw-rw-r-- 1 w205 w205 46101006 Jun 12 05:19 xag
"""
# put sub-files into one big file
cat x* > Crimes_-_2001_to_present.csv.gz #gz ~ zip
# unzip this big file
gunzip Crimes_-_2001_to_present.csv.gz
# do word count
wc -l *.csv
# return 5862796 Crimes_-_2001_to_present.csv
# save directory
# /home/w205/w205-summer-17-labs-exercises/data/Crimes_-_2001_to_present_data
# save filename
# Crimes_-_2001_to_present.csv

##############################################################
# Step 1: pyspark basics, Python list, Spark Contexts, converting a Python list to a Spark RDD (Resilient Distributed Dataset) [ mp4 ]
# PySpark!!

# w205 ~
pyspark # similar to interactive python
>>> x = [1,2,3,4,5,6,7,8,9]
>>> len(x)
# look at a datastructure caled SparkContext
>>> sc # object we refer to as Spark Cluster
# <pyspark.context.SparkContext object at 0x26bd4d0>

# create our RDD (resilient distributed dataset)
>>> distData = sc.parallelize(x);
>>> print distData;
# do a count of data
>>> nx = distData.count()
>>> nx
>>> len(distData) #doesn't work on RDD!
>>> exit()

# read more about RDD execution details
env | grep -i spark  #check SPARK_HOME=/data/spark15
cd /data/spark15
ls -l # should see conf
cd conf
ls -l # should see log4j.properties
cat log4j.properties
cat log4j.properties.template

# Note: Now we know Pyspark can do both python and spark clusters

##############################################################
# Step 2: loading a CSV file into an RDD, looking at records, counts, removing the header record [ mp4 ]
# Turn CSV into RDD!!

# LOAD CRIME DATA
pyspark
# replace below with the path where data is
# /home/w205/w205-summer-17-labs-exercises/data
# >>>crimedata=sc.textFile("file:///data/mylab/Crimes_-_2001_to_present.csv")
# note: if we don't specify path, pyspark will assume HDFS 
>>>crimedata=sc.textFile("file:///home/w205/w205-summer-17-labs-exercises/data/Crimes_-_2001_to_present_data/Crimes_-_2001_to_present.csv");
>>>print crimedata.count() # 5862796 (takes a while)
# look at first line
>>>crimedata.first() # return header
>>>crimedata.take(10)
# get rid of header
>>>noHeaderCrimedata = crimedata.zipWithIndex().filter(lambda (row,index): index > 0).keys() #.keys() means I want my row back without the index
>>> noHeaderCrimedata.first()
>>> noHeaderCrimedata.count() # 5862795 (takes a while)
 # define function to remove header
>>>def remove_header(itr_index, itr): return iter(list(itr)[1:]) if itr_index == 0 else itr
>>>noHeaderCrimedata2 = crimedata.mapPartitionsWithIndex(remove_header)
>>>noHeaderCrimedata2.take(5)

# Note: RDDs are immutable --> even to remove one row you need to create a new one

##############################################################
# Step 3: Filter records and structures [ mp4 ]

>>>narcoticsCrimes = noHeaderCrimedata.filter(lambda x: "NARCOTICS" in x)
>>>narcoticsCrimes.count() #663712
>>>narcoticsCrimes.take(20)


# split each row from one long UNICODE string to each field being one UNICODE string
>>> narcoticsCrimeRecords = narcoticsCrimes.map(lambda r : r.split(","))
>>> narcoticsCrimeRecords.first()
>>> narcoticsCrimeRecords.count() #check 663712

##############################################################
# Step 4: Key Values [ mp4 ]
# data structure --> key value pair
# as tuple in python
>>> narcoticsCrimeTuples = narcoticsCrimes.map(lambda x:(x.split(",")[0], x))
>>> narcoticsCrimeTuples.count() #check 663712
>>> narcoticsCrimeTuples.first()
# now we have in each row [ID:"long unicode string"]

>>>firstTuple=narcoticsCrimeTuples.first()
>>>len(firstTuple) #2
>>>firstTuple[0] # u'10184515'
>>>firstTuple[1] # u'10184515....'
# problem: repeating key

# SUBMISSION
# change up this line
# >>> narcoticsCrimeTuples = narcoticsCrimes.map(lambda x:(x.split(",")[0], x))

# SOLUTION 1: Into (key) , (value as one string)
>>> narcoticsCrimeTuples2 = narcoticsCrimes.map(lambda x:(x.split(",")[0], x.split(",",1)[1]))
>>> narcoticsCrimeTuples2.first()
# SOLUTION 2: Into (key) , (value as a list of strings/records)
>>> narcoticsCrimeTuples3 = narcoticsCrimes.map(lambda x:(x.split(",")[0], x.split(",")[1:]))
>>> narcoticsCrimeTuples3.first()


# More operations are possible once we have (key,value) tuples
>>> sorted = narcoticsCrimeTuples2.sortByKey() # takes a while
# print from sorted RDD
>>> sorted.first()
# print from original RDD
>>> narcoticsCrimeTuples2.first()


##############################################################
# Step 5: Spark SQL CLI [ mp4 ] (Beeline interface?)

# get into spark-sql CLI
spark-sql

show tables; # should see lab3 (weblog) and exercise1 (hospitals)

create table dummy (somedata varchar(500));
show tables;
describe dummy;
drop table dummy; # drop table dummy
show tables;

##############################################################
# Step 6: Spark SQL Table loaded with Data from CSV file [ mp4 ]

# get into spark-sql CLI
spark-sql

# create parquet table in spark SQL
create table web_session_Log
(DATETIME varchar(500),
USERID varchar(500),
SESSIONID varchar(500),
PRODUCTID varchar(500),
REFERERURL varchar(500))
row format delimited fields terminated by '\t' stored as textfile;

# double check table is alright
describe web_session_Log;
"""
datetime        varchar(500)    NULL
userid  varchar(500)    NULL
sessionid       varchar(500)    NULL
productid       varchar(500)    NULL
refererurl      varchar(500)    NULL
Time taken: 0.155 seconds, Fetched 5 row(s)
"""

# load our weblog data into the created table
LOAD DATA LOCAL INPATH "/home/w205/w205-summer-17-labs-exercises/data/weblog_lab.csv" INTO TABLE web_session_log;
select count(*) from web_session_log; # 40002

# double check wc in duplicate session
su - w205
cd w205-summer-17-labs-exercises/data
wc -l weblog_lab.csv

# back to spark-sql
select * from web_session_log where refererurl = "http://www.ebay.com" ;
# returned 3943 rows
select count(*) from web_session_log where refererurl ="http://www.ebay.com" ;
# returned 3943 rows

##############################################################
# Step 7: Accessing Spark SQL in Python Code [ mp4 ]

pyspark

>>> from pyspark.sql import SQLContext
>>> from pyspark.sql.types import * 
# Create the Spark SQL Context.
>>> sqlContext = SQLContext(sc)

# read in data from file
>>> lines = sc.textFile('file:///home/w205/w205-summer-17-labs-exercises/data/weblog_lab.csv')
# Create a map of the data so that they can be structured into a table.
# map structure --> break into lines
>>> parts = lines.map(lambda l: l.split('\t'))
# map structure --> break into columns
>>> Web_Session_Log = parts.map(lambda p: (p[0],p[1],p[2], p[3],p[4]))

# Create string with the name of the columns of your table
>>> schemaString = 'DATETIME USERID SESSIONID PRODUCTID REFERERURL'
# Create a data structure of StructFields that can be used to create a table
>>> fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
# Combine the fields into a schema object
>>> schema = StructType(fields)

# Create a table based on a DataFrame using the data that was read 
# and the structure representing the schema.
>>> schemaWebData = sqlContext.createDataFrame(Web_Session_Log, schema)
# return an error but ok to move on
# Register the object as a table with a table name
>>> schemaWebData.registerTempTable('Web_Session_Log')
# now we've done sth similar to "create" statement in sql CLI
# we took a csv file and imposed a schema on it

# Query the table
>>> results = sqlContext.sql('SELECT count(*) FROM Web_Session_Log')
# Use the DataFrame operation show to print the content 
# of the result of the query.
>>> results.show() #40002
# query the number of rows related to eBay
# paste into the sqlContext.sql('<query>') syntax
select count(*) from Web_Session_Log where REFEREURL= "http://www.ebay.com" # corrected syntax
>>> results = sqlContext.sql('select count(*) from Web_Session_Log where REFERERURL= "http://www.ebay.com"')
>>> results.show() #3943


# run a script
# copy script to a local word pad
# paste to instance
cd ~
mkdir lab_4
cd lab_4
vi mysql.py
#---------#
# PAST CODE
#---------#
# run the script in the same directory the .py is stored
spark-submit mysql.py # same output as the above


##############################################################
# Step 8: Caching Tables and Uncaching Tables [ mp4 ]

# cache takes a table and put it in memory
# careful --> only pick table used a lot and small 
# e.g. master data for analytics

spark-sql

show tables;

select count(*) from hospitals;
# 2.8 sec

cache table hospitals;
select count(*) from hospitals;
# <0.5 sec

uncache table hospitals;
# not taking it right the way
# take it out when you run out of memory

