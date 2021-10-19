# Sharath S Bhargav - HW 2 CS 441
## University of Illinois at Chicago

### Introduction
This repo contains 4 different map-reduce programs that perform various operations on log files.




## Installation instructions
This section contains the instructions on how to run the different Map-Reduce implemented as part of this homework.

1. Use following URL to clone the project : https://github.com/sharathbhargav/CS441-HW2.git
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
