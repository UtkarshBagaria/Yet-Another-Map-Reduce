import socket
import threading
def conne(a,b,c):
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
            with open(fn,'a') as f:
                f.write(partition)
            client.close()
            return partition
    rec_partition()

# connec=threading.Thread(target=conne,args=())
# connec.start()
# {threading.activeCount() - 1}