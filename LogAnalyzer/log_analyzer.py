#!/usr/bin/env python

import sys
import glob
from pyspark import SparkContext, SparkConf
import re
from pyspark.sql import SQLContext

from pyspark.sql import Row 
conf = SparkConf().setAppName("wordcount1").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

question_num=sys.argv[1]
input_folder=sys.argv[2]
input_folder2=sys.argv[3]

#4.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846

#Feb 28 03:30:01 iliad systemd: Started Session 4415 of user achille

APACHE_ACCESS_LOG_PATTERN = '^"(\S+) (\d{3}) (\S+)" (\S+) (\S+\:) (\S+) (\S+) (\d{3}) (\S+) (\S+) (\S+)'  

def g(x):
 print x
 #\[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)'

# Returns a dictionary containing the parts of the Apache Access Log.
"""def parse_apache_log_line(logline):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        raise Error("Invalid logline: %s" % logline)
    return Row(
        time_stamp    = match.group(1),
        folder = match.group(2),
        systemd       = match.group(3),
        started     = match.group(4),
        session       = match.group(5),
        session_id      = int(match.group(6)),
        of      = match.group(7),
        user = match.group(8),
        user_name  = match.group(9)
)"""





def ques_1(input_folder,input_folder2):
 file=sc.textFile(input_folder)
 file2=sc.textFile(input_folder2)
 #file = [sc.textFile(filename) for filename in glob.glob(input_folder+ "/*.txt")]
 counts=file.count()
 counts2=file2.count()
 print(input_folder ,":", counts)
 print(input_folder2,":" ,counts2)


 
def ques_2(input_folder,input_folder2):

 file=sc.textFile(input_folder)
 file2=sc.textFile(input_folder2)
 linesStarting1 = file.filter(lambda s: 'Starting' in s)
 linesSession1= linesStarting1.filter(lambda s: 'Session' in s)
 notUniqueUsers1=linesSession1.filter(lambda s: 'achille' in s)
 linesStarting2 = file2.filter(lambda s: 'Starting' in s)
 linesSession2 = linesStarting2.filter(lambda s: 'Session' in s)
 notUniqueUsers2 =linesSession2.filter(lambda s: 'achille' in s)
  
 print(notUniqueUsers1.count())
 print(notUniqueUsers2.count())

def ques_3(input_folder): 
 file=sc.textFile(input_folder)
 file2=sc.textFile(input_folder2)
 linesStarting1 = file.filter(lambda s: 'Starting' in s)
 AllUsers1= linesStarting1.filter(lambda s: 'Session' in s)
 linesStarting2 = file2.filter(lambda s: 'Starting' in s)
 AllUsers2 = linesStarting2.filter(lambda s: 'Session' in s)
 #notUniqueUsers2.foreach(g)
 #words1=notUniqueUsers1.flatMap(lambda x: x.split())

 csv_data1 = AllUsers1.map(lambda l: l.split(" "))
 row_data1 = csv_data1.map(lambda p: Row(
    month1=(p[0]), 
    date1=p[1],
    time1=p[2],
    user_id=(p[7]),
    username=(p[10])
    )
 ) 

 interactions_df = sqlContext.createDataFrame(row_data1)
 interactions_df.registerTempTable("interactions") 
 print('hey') 
 content_size_stats = (sqlContext
                      .sql("SELECT DISTINCT username FROM interactions" ))
 #print(content_size_stats.count())
 content_size_stats.foreach(g)
 categories = {}

 #for i in idxCategories: ##idxCategories contains indexes of rows that contains categorical data
 distinctValues = df.map(lambda x : x).distinct().collect()
 print(distinctValues)
    
if(question_num=='1'):
 ques_1(input_folder, input_folder2)

if(question_num=='2'):
 ques_2(input_folder)

if(question_num=='3'):
 ques_3(input_folder)
         
