#client func
import sys
import MasterNode as mn

def writeop(filename):
    with open(filename, 'r') as file:
        content = file.readlines()
        # content=content[0]
        l=len(content)
        ldw=l//n
        k=[]
        j=0
        for i in range(0,l,ldw):
            k[j]=content[i:i+ldw]
            j+=1
        nodes_available = mn.nodesdict(n, filename)
        print(content)

#reading a file filename given as command line argument
print("FOR WRITE OPERATION: 1\n FOR READ OPERATION: 2\n FOR MR OPERATION: 3")
choice = int(input("Enter your choice: "))
if choice == 1: 
    filename = sys.argv[1]
    n=int(sys.argv[2])
    writeop(filename)
elif choice==2:
    filename = sys.argv[1]
    n=sys.argv[2]
    # readop(filename)
else:
    filename = sys.argv[1]
    mapfile = sys.argv[2]
    reducefile = sys.argv[3]
    n=sys.argv[4]

# User passes the input file to the client program to be stored in the cluster.
# The client contacts the master node to schedule the WRITE operation.
# The master node returns a list of worker nodes that the client has to write the data to.
# Based on the list, the client program splits the input file into equally sized partitions and contacts the workers to store their respective partitions to the workers storage. After the client has successfully written the data to all the workers, client informs the user that the WRITE operation is successful.