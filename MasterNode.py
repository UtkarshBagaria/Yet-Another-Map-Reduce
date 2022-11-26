import threading
def nodesdict(n=2,filename="xyz"):
    a=dict()
    filename, file_extension = filename.split(".")
    for i in range(n):
        a[i]=["127.0.0.1",23333+i,filename+str(i)+"."+file_extension]
        connec=threading.Thread(target=w.conne,args=(a[i][0],a[i][1],a[i][2]))
        connec.start()
    return a
import worker as w