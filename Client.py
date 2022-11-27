import sys
# import masternode as mn
import socket
import threading
import math

def client_MN_establish_connection(content):
    # create a socket object
    # print("helloji")
    cm = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # get local machine name
    host = '127.0.0.1'
    port = 33333
    # bind to the port
    # get local machine name
    # connection to hostname on the port.
    cm.connect((host, port))
    # queue up to 5 requests
    print(content)
    cm.send(str(content[0]).encode('ascii'))
    cm.send(str(content[1]).encode('ascii'))
    cm.send(str(content[2]).encode('ascii'))
    a=cm.recv(1024).decode('ascii')
    cm.close()
    # print(a)
    print(type(eval(a)))
    return eval(a)
global partitions
partitions=[]
def send_partition(address, content):
    # create a socket object
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # get local machine name
    host = address[0]
    port = address[1]
    # connection to hostname on the port.
    s.connect((host, port))
    # print(content)
    # Receive no more than 1024 bytes
    # msg = s.recv(1024)
    # s.close()
    # print(msg.decode('ascii'))
    
    s.sendall(content.encode('ascii'))
    s.close()
    return

def recv_partitions(a,b,c):
    # create a socket object
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # get local machine name
    host = a
    port = b
    # print(address)
    # connection to hostname on the port.
    s.connect((host, port))
    # print(content)
    # Receive no more than 1024 bytes
    # msg = s.recv(1024)
    # s.close()
    # print(msg.decode('ascii'))
    partition=s.recv(1024).decode('ascii')
    partitions.append(partition)
    # print(partition)
    return partition

def writeop(filename):
    with open(filename, 'r') as file:
        content = file.readlines()
        # content=content[0]
        l=len(content)
        ldw=math.ceil(l/n)
        k=[]
        j=0
        if l>=n:
                for i in range(0,l,ldw):
                    st=''
                    for j in range(0,ldw):
                        if(i+j<l):
                            st=st+content[i+j]
                        # st=st+content[i+j]
                    k.append(st)
        else:
            for j in range(0,l):
                k.append(content[j])
            for j in range(l,n):
                k.append('')
        print("hi")
        nodes_available = client_MN_establish_connection([n, filename,1])
        print("below the call",type(nodes_available))
        for i in range(len(nodes_available.keys())):
            print("Sending data to node: ", nodes_available[str(i)])
            # sending partition to the address in nodes_available with the partition name
            send_thread=threading.Thread(target=send_partition,args=(nodes_available[str(i)],k[i]))
            send_thread.start()
            # send_partition(nodes_available[i], content)

def readop(filename):
    nodes_available =client_MN_establish_connection([n, filename,2])
    # print(nodes_available['0'])
    recv_thread=[]
    for i in range(0,len(nodes_available.keys())):
        # print("Sending data to node: ", nodes_available[i])
        # print(nodes_available[str(i)])
        # sending partition to the address in nodes_available with the partition name
        recv_thread.append(threading.Thread(target=recv_partitions,args=(nodes_available[str(i)][0],nodes_available[str(i)][1],nodes_available[str(i)][2])))
    for i in range(0,len(recv_thread)):
        recv_thread[i].start()
    for i in range(0,len(recv_thread)):
        recv_thread[i].join() 
    # for i in partitions:
    #     a,b=i.split(' ',1)

        # res=recv_thread.join()
        # print(res)
        # send_partition(nodes_available[i], content)
    
def mapperop(filename):
    nodes_available = mn.nodesdictread(n, filename)
    # print(nodes_available['0'])
    recv_thread=[]
    for i in range(0,len(nodes_available.keys())):
        # print("Sending data to node: ", nodes_available[i])
        # print(nodes_available[str(i)])
        # sending partition to the address in nodes_available with the partition name
        recv_thread.append(threading.Thread(target=recv_partitions,args=(nodes_available[str(i)][0],nodes_available[str(i)][1],nodes_available[str(i)][2])))
    for i in range(0,len(recv_thread)):
        recv_thread[i].start()
    for i in range(0,len(recv_thread)):
        recv_thread[i].join() 
    # for i in partitions:
    #     a,b=i.split(' ',1)

        # res=recv_thread.join()
        # print(res)
        # send_partition(nodes_available[i], content)
#reading a file filename given as command line argument
print("FOR WRITE OPERATION: 1\n FOR READ OPERATION: 2\n FOR MR OPERATION: 3")
choice = int(input("Enter your choice: "))
if choice == 1: 
    filename = sys.argv[1]
    n=int(sys.argv[2])
    writeop(filename)
elif choice==2:
    filename = sys.argv[1]
    n=int(sys.argv[2])
    readop(filename)
    partitions=sorted(partitions)
    for i in partitions:
        b=i.split(' ',1)
        print(b[1])
elif choice==3:
    filename = sys.argv[1]
    mapfile = sys.argv[2]
    reducefile = sys.argv[3]
    n=sys.argv[4]
    mapperop(filename,mapfile,n)

# send_thread=threading.Thread(target=send_partition,args=())
# send_thread.start()


