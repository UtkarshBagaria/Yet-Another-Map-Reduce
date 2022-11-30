# Big Data Project 2022 - Yet Another Map Reduce.

### Pre-requisites and Assumptions

1. python is being used to run the code, for running with python3 python needs to be replaced with python3 in the WorkerNode.py file.
2. Commands to create and delete intermeddiate folders and files are written with respect to Windows OS, for any other OS changes need to be made accordingly in all the files.
3. A file needs to be written before it can be read.
4. All input files are present in the same directory as the code.
5. Before running the code, the directory needs to be cleaned of all the files generated by the code.
6. The MasterNode needs to be restarted before each operation by closing the terminal and opening a new one and running the command again.
7. The number of nodes mentioned for reading and writing have to be equal.
<p>NOTE- Client terminal need not be restarted</p>

# For Execution

<pre>
1. Clone the repository
</pre>

## Write Operation

<pre>
2. Open Terminal in that directory
3. Start the MasterNode with the command
    <code>python MasterNode.py</code>
4. Open another terminal in the same directory
4. Start the Client with the name of file to be written and the number of worker nodes 
    <code>python Client.py filename no_of_worker</code>
For example
    <code>python Client.py test.txt 6</code>
Where test.txt is the file to be written and 6 is the number of worker nodes
5. Choose 1 in the menu to write the file
6. Ensure that the terminal of MasterNode is closed before starting a new operation  
</pre>

## Read Operation

<pre>
7. Open Terminal in that directory
8. Start the MasterNode with the command
    <code>python MasterNode.py</code>
9. Open another terminal in the same directory
10. Start the Client with the name of file to be read and the number of worker nodes 
    <code>python Client.py filename no_of_worker</code>
For example
    <code>python Client.py test.txt 6</code>
Where test.txt is the file to be read and 6 is the number of worker nodes
11. Choose 2 in the menu to read the file
12. Output will be displayed on the terminal
12. Ensure that the terminal of MasterNode is closed before starting a new operation 
</pre>

## Map Reduce Operation

<pre>
13. Open Terminal in that directory
14. Start the MasterNode with the command
    <code>python MasterNode.py</code>
15. Open another terminal in the same directory
16. Start the Client with the name of input file, mapper, reducer and the number of worker nodes 
    <code>python Client.py filename mapper.py reducer.py no_of_worker</code>
For example
    <code>python Client.py test.txt Mapper.py Reducer.py 6</code>
Where, 
test.txt is the input file 
Mapper.py is the mapper 
Reducer.py is the reducer
6 is the number of worker nodes
17. Choose 3 in the menu to perform Map Reduce
18. Output will be displayed on the terminal
19. Ensure that the terminal of MasterNode is closed before starting a new operation 
</pre>
