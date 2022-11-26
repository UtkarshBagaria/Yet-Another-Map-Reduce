import threading
import WorkerNode as w
import json
def nodesdictwrite(n=2,filename="xyz"):
    a=dict()
    filename, file_extension = filename.split(".")
    for i in range(n):
        a[i]=["127.0.0.1",23333+i,filename+str(i)+"."+file_extension]
        connec=threading.Thread(target=w.conne,args=(a[i][0],a[i][1],a[i][2]))
        connec.start()
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