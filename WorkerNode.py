import socket
import threading
# import time
def conne(a,b,c,op):
    # print("hello")
    host=a
    port=b
    fn=c
    server=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    server.bind((host,port))
    server.listen()
    print("Server is listening at ",host,port)
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
    if(op=='w'):
        rec_partition()
    elif op=='r':
        sends_partition()

# connec=threading.Thread(target=conne,args=())
# connec.start()
# {threading.activeCount() - 1}