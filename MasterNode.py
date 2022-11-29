import threading
import json
import socket

def nodesdictwrite(n=2,filename="xyz"):
    a=dict()
    filename, file_extension = filename.split(".")
    for i in range(n):
        a[i]=["127.0.0.1",23333+i,filename+str(i)+"."+file_extension]
        connec=threading.Thread(target=w.conne,args=(a[i][0],a[i][1],a[i][2],'w'))
        connec.start()
    fw=open('metadata.txt','w')
    fw.write(filename+" "+json.dumps(a))
    return a

def nodesdictread(n=2,filename="xyz"):
    a=dict()
    # print(filename)
    filename, file_extension = filename.split(".")
    fw=open('metadata.txt','r')
    for line in fw:
        if filename in line:
            a=json.loads(line.split(" ",1)[1])
            # print(a)
    for i in a:
        connec=threading.Thread(target=w.conne,args=(a[i][0],a[i][1],a[i][2],'r'))
        connec.start()
        # time.sleep(1)
    return a

def nodesformap(n=2,filename="xyz"):
    a=dict()
    # print(filename)
    filename, file_extension = filename.split(".")
    fw=open('metadata.txt','r')
    for line in fw:
        if filename in line:
            a=json.loads(line.split(" ",1)[1])
            # print(a)
    return a

def maptopartition(a,mrf):
    connec=[]
    for i in a.keys():
        connec.append(threading.Thread(target=w.conne,args=(a[i][0],a[i][1],a[i][2],'m',mrf)))
    for i in connec:
        i.start()
    c=0
    for i in connec:
        i.join()
        print("ACK mapper from port: ",a[str(c)][1])
        c+=1
    # connec.start()
    # print("ACK mapper from port: ",a[i][1])
        # time.sleep(1)
    return a

def reduceforpartition(a,mrf):
    for i in a.keys():
        connec=threading.Thread(target=w.conne,args=(a[i][0],a[i][1],a[i][2],'red',mrf))
        connec.start()
        print("ACK reducer from port: ",a[i][1])
        # time.sleep(1)
    return a

def hashpartition(a):
    no=len(a.keys())
    # print("THIS IS MODULO IN THE MASTER NODE",no)
    connec=[]
    for i in a:
        connec.append(threading.Thread(target=w.conne,args=(a[i][0],a[i][1],no,'shh')))
    for i in connec:
        i.start()
    for i in connec:
        i.join()
        # time.sleep(1)
    return a

def MN_Client_establish_connection():
    server=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    server.bind(('127.0.0.1',55555))
    server.listen()
    print("MasterNode is listening at ",'127.0.0.1',55555)
    while True:
        client,address=server.accept()
        # print("listening")
        # print("Connected to ",str(address))
        # client.send("Connected".encode('ascii'))
        r=client.recv(1024).decode('ascii')
        # print(r)
        op1=r.split(" ")
        filename=op1[1]
        n=int(op1[0])
        op=int(op1[2])
        # n=int(n)
        # op=int(op)
        # print(op)
        # print(n,filename,op)
        if(int(op)==1):
            a=nodesdictwrite(int(n),filename)
            client.send(json.dumps(a).encode('ascii'))
        elif(int(op)==2):
            a=nodesdictread(int(n),filename)
            client.send(json.dumps(a).encode('ascii'))
        else:
            # mrf=op1[3]
            if(int(op)==3):
                a=nodesformap(int(n),filename)
                # client.send(json.dumps(a).encode('ascii'))
                maptopartition(a,op1[3])
                xyz = {'Map':'Done'}
                client.send(json.dumps(xyz).encode('ascii'))
                # print("map",mrf)
            elif(int(op)==4):
                reduceforpartition(a,op1[3])
                xyz = {'Reduce':'Done'}
                client.send(json.dumps(xyz).encode('ascii'))
                # print("reduce",mrf)
            elif(int(op)==5):
                hashpartition(a)
                xyz = {'Hash':'Done'}
                client.send(json.dumps(xyz).encode('ascii'))
                # print("hash")

        # a=nodesdictwrite(int(n),filename)
        # print(a)
        # client.send((str(a)).encode('ascii'))
        client.close()



import WorkerNode as w
MN_Client_establish_connection()