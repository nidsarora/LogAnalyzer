#!/usr/bin/env python

import sys
import glob
from pyspark import SparkContext, SparkConf
import re
from pyspark.sql import SQLContext
from collections import Counter

from pyspark.sql import Row 
conf = SparkConf().setAppName("wordcount1").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

question_num=sys.argv[1]
input_folder=sys.argv[2]
input_folder2=sys.argv[3]
output_file=sys.argv[4]

#4.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846

#Feb 28 03:30:01 iliad systemd: Started Session 4415 of user achille

APACHE_ACCESS_LOG_PATTERN = '^"(\S+) (\d{3}) (\S+)" (\S+) (\S+\:) (\S+) (\S+) (\d{3}) (\S+) (\S+) (\S+)'  

def g(x):
 print x
def h(x):
 print x.count()
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
 return linesSession1
 print(notUniqueUsers1.count())
 print(notUniqueUsers2.count())

def ques_3(input_folder,input_folder2): 
 file=sc.textFile(input_folder)
 file2=sc.textFile(input_folder2)
 linesStarting1 = file.filter(lambda s: 'Starting Session' in s)
 AllUsers1=linesStarting1.filter(lambda s: ' of user ' in s)
 AllUsers11=AllUsers1.filter(lambda s: '.' in s)
 print(AllUsers11.count())
 
 linesStarting2 = file2.filter(lambda s: 'Starting Session' in s)
 AllUsers2=linesStarting2.filter(lambda s: ' of user ' in s)
 AllUsers22=AllUsers2.filter(lambda s: '.' in s)
 print(AllUsers22.count())
 #AllUsers2.foreach(g)
 #words1=notUniqueUsers1.flatMap(lambda x: x.split())

 csv_data1 = AllUsers11.map(lambda l: l.split(" "))
 row_data1 = csv_data1.map(lambda p: Row(
    user=(p[10]),
    user_id=(p[7])
    )
    #Feb 27 03:50:01 iliad systemd: Starting Session 4248 of user achille
 )
 #rowdata1.distinct.collect().foreach(println)
 csv_data2 = AllUsers22.map(lambda l: l.split(" "))
 row_data2 = csv_data2.map(lambda p: Row(
    user=(p[10]),
    user_id2=(p[7])
    
    )
    #Feb 27 03:50:01 iliad systemd: Starting Session 4248 of user achille
 )
 interactions_df = sqlContext.createDataFrame(row_data1)
 interactions_df.registerTempTable("interactions") 
 print('hey') 
 content_size_stats = (sqlContext
                      .sql("select distinct user from interactions where user !='user'" ))
 interactions2_df = sqlContext.createDataFrame(row_data2)
 interactions2_df.registerTempTable("interactions2") 
 print('hey') 
 content_size_stats2 = (sqlContext
                      .sql("select distinct user from interactions2 where user !='user'" ))
 
 print(content_size_stats.show())
 return interactions_df, interactions2_df
 #print(content_size_stats.count())

 #content_size_stats.foreach(g)

 #for i in idxCategories: ##idxCategories contains indexes of rows that contains categorical data
 """
 distinctValues = df.map(lambda x : x).distinct().collect()
 distinctValues.foreach(g)"""
 
def ques_4(input_folder,input_folder2):
 uniqueUsers=ques_3(input_folder,input_folder2)
 #df1.registerTempTable("interactionsdf1") 
 """print('hey') 
 content_size_stats = (sqlContext
                      .sql("select user,\
                                    count(user_id) from interactionsdf1 \
                                    where user !='user'\
                                    group by user " )).show()"""
 data=ques_2(input_folder,input_folder2)
 for i in uniqueUsers:
  notUniqueUsers1=data.filter(lambda s: i in s)
  print(notUniqueUsers1.count())

def ques_5(input_folder,input_folder2):
 file=sc.textFile(input_folder)
 file2=sc.textFile(input_folder2)
 """linesStarting1 = file.filter(lambda s: 'Starting' in s)
 linesSession1= linesStarting1.filter(lambda s: 'Session' in s)"""
 errorLines1 = file.filter(lambda s: 'ERROR' in s.upper())   
 errorLines2 = file2.filter(lambda s: 'ERROR' in s.upper()) 
 print(errorLines1.count())
 print(errorLines2.count())
 return errorLines1, errorLines2                  
 
"""def ques_6(input_folder,input_folder2,output_file):
 errorLines1,errorLines2=  ques_5(input_folder,input_folder2)
 pairs=errorLines1.map(lambda x: (x,1))
 #counts=pairs.reduceByKey(lambda x,y: x+y)
 #output = counts.map(lambda (k,v): (v,k)).sortByKey(True).take(5)
 counter = Counter(pairs)
 print(counter.most_common()[:5]) 
 #print(output)"""

def ques_7(input_folder,input_folder2):
 table1_df, table2_df = ques_3(input_folder,input_folder2)
 table1_df.registerTempTable("table1") 
 table2_df.registerTempTable("table2") 
 
 queryResult=(sqlContext
                      .sql("select distinct table1.user\
        from table1 inner join table2\
        on table1.user = table2.user\
        where table1.user != 'user' " )).show()  
  
if(question_num=='1'):
 ques_1(input_folder, input_folder2)

if(question_num=='2'):
 ques_2(input_folder,input_folder2)

if(question_num=='3'):
 ques_3(input_folder,input_folder2)

if(question_num=='4'):
 ques_4(input_folder,input_folder2)

if(question_num=='5'):
 ques_5(input_folder,input_folder2)

if(question_num=='6'):
 ques_6(input_folder,input_folder2,output_file)

if(question_num=='7'):
 ques_7(input_folder,input_folder2)

 
