# Sharath S Bhargav - HW 2 CS 441
## University of Illinois at Chicago

### Introduction
This repo contains 4 different map-reduce programs that perform various operations on log files.




## Installation instructions
This section contains the instructions on how to run the different Map-Reduce implemented as part of this homework.

1. Use following URL to clone the project : git@github.com:sharathbhargav/CS441-HW2.git
2. Navigate into project root directory and run "sbt assembly". This will generate a JAR file in target/scala-2.13/
3. There are 4 classes, one each for 4 tasks: com.Task1, com.Task2, com.Task3, com.Task4.
4. The time period to split the log files is passed as parameter when required. This allows for customization while running the program.
5. Task 1 takes 3 parameters: input files path, output folder path, the number of seconds of each interval the log files have to be split into.
6. Task 2 takes 4 parameters: input files path, temporary output folder path, output folder path, the number of seconds of each interval the log files have to be split into.
7. Task 3 takes 2 parameters: input files path, output folder path, the number of seconds of each interval the log files have to be split into.
8. Task 4 takes 2 parameters: input files path, output folder path, the number of seconds of each interval the log files have to be split into.

To run the tasks use the following command in a terminal : 

For task1 : "hadoop jar LogFileProcessor-assembly-1.0.jar com.Task1 /path/to/input /path/to/output 30"
where path/to/input is a folder containing all log files, path/to/output is folder name where output has to be written and 30 is the duration of each bin in seconds (This number can be changed)

For task2 : "hadoop jar LogFileProcessor-assembly-1.0.jar com.Task2 /path/to/input /path/to/output_temp /path/to/output 30"
where path/to/input is a folder containing all log files, /path/to/output_temp is an intermediate folder used to store temporary results from the first job, path/to/output is folder name where output has to be written and 30 is the duration of each bin in seconds (This number can be changed)

For task3 : "hadoop jar LogFileProcessor-assembly-1.0.jar com.Task3 /path/to/input /path/to/output "
where path/to/input is a folder containing all log files,  path/to/output is folder name where output has to be written

For task4 : "hadoop jar LogFileProcessor-assembly-1.0.jar com.Task4 /path/to/input /path/to/output"
where path/to/input is a folder containing all log files,  path/to/output is folder name where output has to be written


A short demo of running the above tasks on AWS EMR can be found at https://www.youtube.com/watch?v=ANBrpZWj3qc

A map reduce job involves splitting data into multiple parts each of which can be processed independently. This allows faster computation of results. The split parts of input must be processable independently. If there is any dependency between split data then the advantages of map-reduce paradigm cannot be fully utilized.

A map-reduce job involves mainly two steps. The map phase and reduce phase.

In the programs written in  this repo, the map phase will take each line of log file as input and splits the line to obtain the timestamp, the message type, and the actual log message. It then outputs the message type as key and number one as value. The map-reduce framework groups all the same keys and creates a list of values which are sorted and then passed onto reducer. The reduce phase gets a list of number one, it then adds them up to obtain the count of particular message type.

This is the general overview of a map-reduce program. There are various other steps that can be overridden by programmer such as defining a comparator which is run before reduce phase. An example of this can be found in Task2. The comparator is used to sort the list before passing to reduce phase. By overriding this function the programmer can control how elements are compared for sorting.

#Description of input:

The input was 11 log files. 10 of which were 1MB each i.e. while generating the log files a rolling window of 1MB was used to create new log files after each file reaches capacity. One more log file of about 45MB was among the input as well. This would create 11 mappers for all tasks. And by default would create 3 reducers, except in task2 where it is explicitly mentioned that only 1 reducer has to be generated so that all output resides in one file.

#Description of each task:
##Task1
Takes in log files as input. The mapper phase maps the log message type of logs that match the predefined pattern. The timestamp of the message is extracted and binned into an interval set by user in the command line argument. It outputs the message type (INFO, DEBUG, ERROR, WARN) and the binned interval time as key and "1" as value.
The reducer phase aggregates this and outputs number of messages of a particular message type in a given interval. 

An example of output is : 16:42:00,DEBUG,61 

Here 16:42:00 is the time bin of 60 seconds i.e from 16:42:00 to 16:42:59 there were 61 messages of type DEBUG in the log files.


##Task2
Takes in log files as input. The mapper phase maps the time interval bin to number of ERROR messages that match the pattern. The reducer then aggregates these values. 
There is a second map-reduce job in this task which is used explicitly for sorting values. The mapper in this job just inverses the key value such that number of messages is the key and time interval bin is the value. The comparator before reduce phase is overridden to sort the values in descending order. The reduce phase then just outputs this result.  

Example output: 675,15:45:00

Here 675 messages of type ERROR where found in the time interval 15:45:00 to 15:45:59 and this was the highest number of ERROR messages in any of the interval bins.


##Task3 
Takes in log files as input. The mapper phase maps the log message type to number one. The reduce phase aggregates this to obtain the number of messages of each log message type.
Output:
DEBUG,59093 

INFO,413448

WARN,68786

ERROR,49549

##Task4
Takes in log files as input. The mapper phase maps the message string that match the predefined pattern to the message type. The reducer finds the length of the longest string for each type and outputs that as result.

Output:
DEBUG,18

INFO,15

WARN,15

ERROR,24

#Deploying to AWS
Generate the jar file using "sbt assembly" and then use this as input for a step in AWS EMR as shown in the video. The inputs can either be on S3 or can be copied into master node of the cluster. Give respective input paths while running the program.