def read_operation():
    pass


def write_operation(f):
    l = []
        
    return l


# User passes the input file to the client program to be stored in the cluster.
# The client contacts the master node to schedule the WRITE operation.
# The master node returns a list of worker nodes that the client has to write the data to.
# Based on the list, the client program splits the input file into equally sized partitions and contacts the workers to store their respective partitions to the workers storage. After the client has successfully written the data to all the workers, client informs the user that the WRITE operation is successful.