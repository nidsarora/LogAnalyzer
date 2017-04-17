#!/usr/bin/env python

import sys
import glob
from pyspark.sql.functions import array, create_map, struct
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import lit
import re
from pyspark.sql import SQLContext
from collections import Counter
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import Row 
conf = SparkConf().setAppName("wordcount1").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


iliad_file_path='file:////home/nids/LogAnalyzer/LogAnalyzer/'  #define the file path of iliad 
odyssey_file_path='file:////home/nids/LogAnalyzer/LogAnalyzer/'#define the file path of odyssey 
simple_txt=sys.argv[1]
question_num=sys.argv[2]
input_folder=iliad_file_path +sys.argv[3]
input_folder2=odyssey_file_path +sys.argv[4]

def g(x):
 print x

def ques_1(input_folder,input_folder2):
 file=sc.textFile(input_folder)
 file2=sc.textFile(input_folder2)
 counts=file.count()
 counts2=file2.count()
 return counts,counts2

def ques_2(input_folder,input_folder2):

 file=sc.textFile(input_folder)
 file2=sc.textFile(input_folder2)
 linesStarting1 = file.filter(lambda s: 'Starting' in s)
 linesSession1= linesStarting1.filter(lambda s: 'Session' in s)
 usersHost1=linesSession1.filter(lambda s: 'achille' in s)
 linesStarting2 = file2.filter(lambda s: 'Starting' in s)
 linesSession2 = linesStarting2.filter(lambda s: 'Session' in s)
 usersHost2 =linesSession2.filter(lambda s: 'achille' in s)
 print("   +{}:{}").format(sys.argv[3],str(usersHost1.count()))
 print("   +{}:{}").format(sys.argv[4],str(usersHost2.count()))

def ques_3(input_folder,input_folder2): 
 file=sc.textFile(input_folder)
 file2=sc.textFile(input_folder2)
 linesStarting1 = file.filter(lambda s: 'Starting Session' in s)
 AllUsers1=linesStarting1.filter(lambda s: ' of user ' in s)
 linesStarting2 = file2.filter(lambda s: 'Starting Session' in s)
 AllUsers2=linesStarting2.filter(lambda s: ' of user ' in s)
 csv_data1 = AllUsers1.map(lambda l: l.split(" "))
 row_data1 = csv_data1.map(lambda p: Row(
    host=(p[3]),
    user=(p[10]),
    user_id=(p[7])
    )
 )
 csv_data2 = AllUsers2.map(lambda l: l.split(" "))
 row_data2 = csv_data2.map(lambda p: Row(
    host=(p[3]),
    user=(p[10]),
    user_id2=(p[7])
    )
 )
 interactions_df = sqlContext.createDataFrame(row_data1)
 interactions2_df= sqlContext.createDataFrame(row_data2)
 return interactions_df, interactions2_df
 
 
def ques_4(input_folder,input_folder2):
 file=sc.textFile(input_folder)
 file2=sc.textFile(input_folder2)
 linesStarting1 = file.filter(lambda s: 'STARTING' in s.upper())
 linesSession1= linesStarting1.filter(lambda s: 'SESSION' in s.upper())
 user_achille1=linesSession1.filter(lambda s: 'ACHILLE' in s.upper()).count()
 user_gaia1=linesSession1.filter(lambda s: 'GAIA' in s.upper()).count()
 user_pollux1=linesSession1.filter(lambda s: 'POLLUX' in s.upper()).count()
 user_helene1=linesSession1.filter(lambda s: 'HELENE' in s.upper()).count()
 user_hector1=linesSession1.filter(lambda s: 'HECTOR' in s.upper()).count()
 linesStarting2 = file2.filter(lambda s: 'STARTING' in s.upper())
 linesSession2= linesStarting2.filter(lambda s: 'SESSION' in s.upper())
 user_achille2=linesSession2.filter(lambda s: 'ACHILLE' in s.upper()).count()
 user_hector2=linesSession2.filter(lambda s: 'HECTOR' in s.upper()).count()
 user_ares2=linesSession2.filter(lambda s: 'ARES' in s.upper()).count()
 print("   +{} : [('gaia',{}) , ('pollux', {}) , ('achille', {}), ('helene', {}) , ('hector',{})").format(sys.argv[3],str(user_gaia1),str (user_pollux1), str(user_achille1), str(user_helene1), str(user_hector1))
 print("   +{} : [('achille',{}) , ('hector', {}) , ('ares', {})").format(sys.argv[4],str(user_achille2),str (user_hector2), str(user_ares2))

def ques_5(input_folder,input_folder2):
 file=sc.textFile(input_folder)
 file2=sc.textFile(input_folder2)
 errorLines1 = file.filter(lambda s: 'ERROR' in s.upper()) 
 errorLines2 = file2.filter(lambda s: 'ERROR' in s.upper())
 return errorLines1, errorLines2                

def ques_6(input_folder,input_folder2):                       
 errorLines1,errorLines2=  ques_5(input_folder,input_folder2)
 csv_data1 = errorLines1.map(lambda l: l.split(" "))
 row_data1 = csv_data1.map(lambda p: Row(
    host=(p[3]),
    user=(p[10]),
    user_id=(p[7])
    )
 )

 pairs1=errorLines1.map(lambda s: s.split(" ")[4:]   )
 row_data1 = pairs1.map(lambda p: Row(
   error_message= p
    )
 )
 pairs2=errorLines2.map(lambda s: s.split(" ")[4:])
 row_data2 = pairs2.map(lambda p: Row(
   error_message= p
    )
 )
 interactions_df = sqlContext.createDataFrame(row_data1)
 interactions_df.registerTempTable("interactions") 
 q3_1 = (sqlContext.sql("select error_message,count(error_message) from interactions group by error_message order by count(error_message) desc" )).show(5)
 interactions2_df = sqlContext.createDataFrame(row_data2)
 interactions2_df.registerTempTable("interactions2") 
 q3_2 = (sqlContext.sql("select error_message,count(error_message) from interactions2 group by error_message order by count(error_message) desc" )).show(5)

 

def ques_7(input_folder,input_folder2):
 table1_df, table2_df = ques_3(input_folder,input_folder2)
 table1_df.registerTempTable("table1") 
 table2_df.registerTempTable("table2") 
 print("    +:") 
 queryResult=(sqlContext
                      .sql("select distinct table1.user\
        from table1 inner join table2\
        on table1.user = table2.user\
        where table1.user != 'user' " )).show()

def ques_8(input_folder,input_folder2):
 table1_df, table2_df = ques_3(input_folder,input_folder2)
 table1_df.registerTempTable("table1") 
 table2_df.registerTempTable("table2")
 print("    +:") 
 queryResult=(sqlContext
                      .sql("select distinct user,host \
                          from table1 where (table1.user not in (select user from table2))and (host!= 'localhost')\
                          union select user,host from table2\
                          where (table2.user not in (select user from table1)) and (host!= 'localhost') ")).show() 

def ques_9(input_folder,input_folder2):
 iliad_df,odyssey_df=ques_3(input_folder,input_folder2)
 iliad_df.registerTempTable("iliad_users") 
 q9_1 = (sqlContext.sql("select distinct user from iliad_users where user !='user'order by user" ))
 mapping_table_iliad_df = q9_1.withColumn("Anonymous user", struct(lit('user-'),(monotonically_increasing_id())))
 odyssey_df.registerTempTable("odyssey_users") 
 q9_2 = (sqlContext.sql("select distinct user from odyssey_users where user !='user' order by user" ))
 mapping_table_odyssey = q9_2.withColumn("Anonymous user", struct(lit('user-'),(monotonically_increasing_id())))
 print("   + {}:").format(sys.argv[3])
 print("   . User name mapping:")
 mapping_table_iliad_df.show()
 mapping_table_iliad_df.registerTempTable("mapping_table_iliad") 
 print("   + {}:").format(sys.argv[4])
 print("   . User name mapping:")
 mapping_table_odyssey.show()
 queryResult=(sqlContext
                      .sql("select *\
        from iliad_users inner join mapping_table_iliad\
        on iliad_users.user = mapping_table_iliad.user\
        where iliad_users.user != 'user' " )).show()
 queryResult=(sqlContext
                      .sql("select *\
        from odyssey_users inner join mapping_table_odyssey\
        on odyssey_users.user = mapping_table_odyssey.user\
        where odyssey_users.user != 'user' " )).show()
 

                    
if(question_num=='1'):
 counts,counts2= ques_1(input_folder, input_folder2)    
 print("* Q1: line counts")
 print("   +{}:{}").format(sys.argv[3],str(counts))
 print("   +{}:{}").format(sys.argv[4],str(counts2))

if(question_num=='2'):
 print("* Q2: sessions of user achille")
 ques_2(input_folder,input_folder2)
 

if(question_num=='3'):
 interactions_df,interactions2_df=ques_3(input_folder,input_folder2)
 interactions_df.registerTempTable("interactions") 
 q3_1 = (sqlContext.sql("select distinct user from interactions where user !='user'" ))
 interactions2_df.registerTempTable("interactions2") 
 q3_2 = (sqlContext.sql("select distinct user from interactions2 where user !='user'" ))
 print("* Q3: unique user names")
 print("   +{}:").format(sys.argv[3])
 q3_1.show()
 print("   +{}:").format(sys.argv[4])
 q3_2.show()
 

if(question_num=='4'):
 print("* Q4: sessions per user")
 ques_4(input_folder,input_folder2) 

if(question_num=='5'):
 errorNum1, errorNum2 =ques_5(input_folder,input_folder2)
 print("* Q5: number of errors")
 print("   +{}: {}").format(sys.argv[3],errorNum1.count())
 print("   +{}: {}").format(sys.argv[4],errorNum2.count())
 
if(question_num=='6'):
 print("* Q6: 5 most frequent error messages")
 ques_6(input_folder,input_folder2)

if(question_num=='7'):
 print("* Q7: users who started a session on both hosts, i.e., on exactly 2 hosts.")
 ques_7(input_folder,input_folder2)

if(question_num=='8'):
 print("* Q8: users who started a session on exactly one host, with host name.")
 ques_8(input_folder,input_folder2)

if(question_num=='9'):
 ques_9(input_folder,input_folder2)



 
