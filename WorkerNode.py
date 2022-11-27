import socket
import threading
import os
# import time
def conne(a,b,c,op,mrf=""):
    # print("hello")
    host=a
    port=b
    fn=c
    server=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    server.bind((host,port))
    server.listen()
    print("Worker is listening at ",host,port)
    def rec_partition():
        # print("hello")
        while True:
            client,address=server.accept()
            # print("Connected to ",str(address))
            # client.send("Connected".encode('ascii'))
            partition=client.recv(1024).decode('ascii')
            # write to file
            with open(fn,'w') as f:
                f.write(partition)
            client.close()
            return partition
    def sends_partition():
        # print("hello")
        while True:
            client,address=server.accept()
            # print("Connected to ",str(address))
            # client.send("Connected".encode('ascii'))
            # print(fn)
            with open(fn,'r') as f:
                partition=f.read()
                partition=fn+" "+partition
            # time.sleep(1)
            client.send(partition.encode('ascii'))
            client.close()
    def send_mapper(mrf):
            # print("execute mapper")
            partnum=str(port-23333)
            os.system("python "+mrf+"< "+fn+" > "+"intermediate/output"+partnum+".txt")
            # print("mapper executed")
    if(op=='w'):
        rec_partition()
    elif op=='r':
        sends_partition()
    elif op=='m':
        # print("taken mapper")
        send_mapper(mrf)

# connec=threading.Thread(target=conne,args=())
# connec.start()
# {threading.activeCount() - 1}