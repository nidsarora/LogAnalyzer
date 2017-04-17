# LogAnalyzer
Python  Program to analyze the logs of the given input files

**Folders and Files**
 1.LogAnalyzer:Contains the input folders, log_analyzer.py, output sample.
 
 2.Input:Consists of two input log folders namely iliad and odyssey
 
 3.log_analyzer.py: THe main python file that consists of the code for analyzing the logs
 
 4.output:consists of a sample of how the output should look like
 
 
**Steps**
1.Mention two input variables in code as defined by the path of the input file folders
 
   1.iliad_file_path='file:////home/nids/LogAnalyzer/LogAnalyzer/' --mention the path of input folder iliad as shown ,
     odyssey_file_path='file:////home/nids/LogAnalyzer/LogAnalyzer/' ; --mention the path of input folder odyssey as shown
     --file path should start from 'file:////'
   
2.Use the following command through command line to get answers to all 9 questions
   './LogAnalyzer/LogAnalyzer/log_analyzer.py -q 2 iliad odyssey 
    where './LogAnalyzer/LogAnalyzer/log_analyzer.py' is the name of the python file with its address wrt to working directory,
           '-q': symbolizes question ,
           '2': question number , 
            'iliad': first input folder ,
             'odyssey':second input folder



**Notes**
1.Java Version needed -Java 8
2.Hadoop 2.7.3
3.Spark
