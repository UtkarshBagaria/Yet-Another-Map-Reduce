import socket
import threading
import os
import hashlib
# import time
def conne(a,b,c,op,mrf=""):
    # print("hello")
    host=a
    port=b
    fn=c
    modno=c
    # print("MODULO IN CONNE",modno)
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
    def reducing(mrf):
        # print("execute reducer")
        partnum=str(port-23333)
        os.system("python "+mrf+"< "+"shuffle/output"+partnum+".txt"+" > "+"final/output"+partnum+".txt")
        # print("reducer executed")
    def hasherboi():
        # print("execute hasher")
        partnum=str(port-23333)
        mfn="intermediate/output"+partnum+".txt"
        with open(mfn,'r') as f:
            # read each line one by one
            for line in f:
                line=line.strip()
                # split the line into words
                key=line.split(',')[0]
                keyh=int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16) % 10**8
                keyh=keyh%modno
                # print("THIS IS THE HASH KEY",keyh)
                # print("THIS IS THE MOD NUMBER IN WORKER",modno)
                fs="shuffle/output"+str(keyh)+".txt"
                # print("FILESTRING",fs)
                with open(fs,'a') as fp:
                    fp.write(line+"\n")
                fp.close()
        f.close()
        # print("hasher executed")
    def inkypinkysorter():
        # sort file according to keys
        partnum=str(port-23333)
        mfn="shuffle/output"+partnum+".txt"
        with open(mfn,'r') as f:
            lines=f.readlines()
            lines.sort()
            # print(lines)
        f.close()
        with open(mfn,'w') as f:
            f.writelines(lines)
        f.close()
    if(op=='w'):
        rec_partition()
    elif op=='r':
        sends_partition()
    elif op=='m':
        # print("taken mapper")
        send_mapper(mrf)
    elif op=='red':
        # print("taken reducer")
        inkypinkysorter()
        reducing(mrf)
    elif op=='shh':
        # print("taken hash")
        hasherboi()

# connec=threading.Thread(target=conne,args=())
# connec.start()
# {threading.activeCount() - 1}