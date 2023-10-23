# Distributed-Query-Processing-System

Implemented a distributed text processing engine. It is able to read a set of .txt files present in a directory
which contains textual data and answer a fixed set of questions which the user selects. The engine runs on multiple servers scale to multiple compute nodes. There is driver program which talks to the engine instances for sending user requests and displaying the results. Following are the details of various components built.
Driver Program
This program takes only one argument: `input.txt`. The format of this file is:
● * Line 1: Fully qualified path to the directory which contains the text files.
● * Line 2: Space separated numbers representing the following commands:
○ total file count (1)
○ total word count (2)
○ total distinct word count (3)
○ top 100 words with frequencies in descending order of frequency of occurence (4)
The driver prints the output to the console where each line contains the output of each command. The
output format for each command's result is:
Command 1:

file_count time_taken_to_execute_command_in_millis

Command 2:

total_word_count time_taken_to_execute_command_in_millis

Command 3:

total_distinct_word_count time_taken_to_execute_command_in_millis

Unset

Unset

Unset
Command 4:

word1 frequency1 word2 frequency2 ... word100 frequency100
time_taken_to_execute_command_in_millis

There will be at least 100 unique words across all files.
For example:
input.txt:

/home/john/text_files
1 2 3 4

output on the console:

20 200
40000 3000
7000 4200
a 600 an 550 is 400 was 350 ... remaining words ... joe 20 sam 10 10000

# Engine Instance
Each engine instance is capable of executing text processing tasks. For example, a task could be reading exactly one file and counting words. The engine instances accepts tasks from the driver, executes them and returns the output back to the driver.
# Shell script (run.sh / run.py)
To run the driver and the engine, Python script which will do the following:
1. Start 3 instances of engines.
2. Start the diver program with the input.txt supplied as an argument to the script.

For example:

$ cd project_dir
$ ./run.sh input.txt
20 200
40000 3000
7000 4200
a 600 an 550 is 400 was 350 ... remaining words ... joe 20 sam 10 10000
