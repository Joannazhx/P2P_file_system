from __future__ import print_function
import sys
from socket import *
import time
import threading 


localHost = "127.0.0.1"
myPeer = int(sys.argv[1])
s1 = int(sys.argv[2])
s2 = int(sys.argv[3])
print(myPeer,s1,s2)
p1 = -1
p2 = -1
starterPort = 5000
myPort = starterPort + myPeer
s1Port = starterPort + s1
s2Port = starterPort + s2
p1Port = -1
p2Port = -1

REQ = 0
RES = 1

lastPing1 = 0
lastPing2 = 0
lastPingSend = 0

lastAccPing1 = -1
lastAccPing2 = -1

lastSendPing1 = -1
lastSendPing2 = -1

timeOut = 3.0
pingSendTime = 3.0 

kill = 0

req = 0
res = 1

Res = 0

myPeerQuit = 0

def main():
    global myPeerQuit

# Start ping monitor (UDP) thread
# the monitor always on till the process end -- set demon 
    PingUDP = threading.Thread(target = setUDPMonitor)
    PingUDP.setDaemon(True)
  #Start TCP monitor thread
  # the monitor always on till the process end -- set demon 
    PingTCP = threading.Thread(target = setTCPMonitor)
    PingTCP.setDaemon(True)
    PingUDP.start()
  
    PingTCP.start()


    checkAlive = threading.Thread(target = checkAlivePing)
    checkAlive.setDaemon(True)
    checkAlive.start()

    while True:
        a = raw_input('').split()
        if a[0] == 'request':
            if not len(a[1]) == 4 or not int(a[1]):
                print('File number should be all number and between 0000-9999')
                continue
            if not len(a) == 2:
                print('File number needed')
                continue
            sendFileReq(a[1])
        elif a[0] == 'quit':
            if len(a) != 1:
                print('only accept one key word')
                continue
            sendLeave(1)
            sendLeave(2)
            while myPeerQuit == 0:
                pass
            sys.exit()



#send req to s1
def sendFileReq(fileNUm):
    soc = socket(AF_INET,SOCK_STREAM)
    soc.connect((localHost,s1Port))
    data = []
    data.append('fileReq\r\n')
    data.append(str(myPeer) + '\r\n')
    data.append(fileNUm + '\r\n')
    mess = ''.join(data)
    soc.sendall(mess.encode())
    soc.close()
    print('\nFile request message for {} has been sent to my successor'.format(fileNUm))

def sendLeave(pNum):
    if pNum == 1:
        tar = p1
        suc = s2
    else:
        tar = p2
        suc = s1
    soc = socket(AF_INET,SOCK_STREAM)
    soc.connect((localHost,tar + starterPort))
    data = []
    data.append('sendLea\r\n')
    data.append(str(myPeer) + '\r\n')
    data.append(str(suc)+ '\r\n')
    mess = ''.join(data)
    soc.sendall(mess.encode())
    soc.close()


  
def setUDPMonitor():
    global lastAccPing1,lastAccPing2,lastSendPing1,lastSendPing2,lastPing1,lastPing2,lastPingSend,p1,p2,Res,s1,s2
    # Create socket that is to be used for listening ping  messages
    soc = socket(AF_INET,SOCK_DGRAM)
    #soc.settimeout(timeOut)
    soc.bind((localHost,myPort))   
    while True:
        data,addr = soc.recvfrom(1024)
        pingRe = data.decode().split('\r\n')
        if pingRe[0] == 'pingReq':
            #print('A ping request message was received from Peer {}.'.format(pingRe[1])) 
            if int(pingRe[3]) == 1:
		        if p1 == -1 or p1 != int(pingRe[1]):
		            p1 = int(pingRe[1])
		        print('A ping request message was received from Peer {}.'.format(pingRe[1]))
		        tar = p1

            elif int(pingRe[3]) == 2:

		        if p2 == -1 or p2 != int(pingRe[1]):
		            p2 = int(pingRe[1])
		        print('A ping request message was received from Peer {}.'.format(pingRe[1]))
		        tar = p2
            mess = decodeData(RES,myPeer,tar,pingRe[3])
            tarPo = tar + starterPort			
            soc.sendto(mess.encode(),(localHost,tarPo))
        elif pingRe[0] == 'pingRes':
            print('A ping request message was responed from Peer {}.'.format(pingRe[1]))
            if int(pingRe[3]) == 1:
		    	lastAccPing1 += 1		    
            elif int(pingRe[3]) == 2:		    	    
		        lastAccPing2 += 1

def decodeData(Type,source,succ,succNum):
    data = []
    if Type == REQ:
        data.append('pingReq\r\n')
    else:
        data.append('pingRes\r\n')
    data.append(str(source)+'\r\n')
    data.append(str(succ)+'\r\n')
    data.append(str(succNum)+'\r\n')
    mess = ''.join(data)
    return mess


def checkAlivePing():
    global lastAccPing1,lastAccPing2,lastSendPing1,lastSendPing2,lastPing1,lastPing2,lastPingSend,p1,p2,s1,s2,kill
    while True:
        time.sleep(5)

        soc1 = socket(AF_INET, SOCK_DGRAM)
        data = decodeData(REQ,myPeer,s1,1)
        targetPort = s1 + starterPort
        soc1.sendto(data.encode(), (localHost, targetPort))
        lastSendPing1 += 1
        soc1.close()
        
        soc2 = socket(AF_INET, SOCK_DGRAM)
        data = decodeData(REQ,myPeer,s2,2)
        targetPort = s2 + starterPort
        soc2.sendto(data.encode(), (localHost, targetPort))
        lastSendPing2 += 1
        soc2.close()
        if kill != 2:
            if (lastSendPing1 - lastAccPing1) > 3 or (lastSendPing2 - lastAccPing2) > 3 :
                if (lastSendPing1 - lastAccPing1) > 3:
                    peerKill(1,s1)
                else:
                    time.sleep(2)
                    peerKill(2,s2)
                kill += 1



def peerKill(Type,sucessor):
    global lastAccPing1,lastAccPing2,lastSendPing1,lastSendPing2,s1,s2
    print('\nPeer {} is no longer alive.'.format(sucessor))
    if Type == 1:
        tar = s2
    else:
        tar = s1
    tarPo = tar + starterPort
    socc = socket(AF_INET,SOCK_STREAM)
    socc.connect((localHost,tarPo))    
    data = []
    data.append('askS\r\n')
    data.append(str(Type) + '\r\n')
    data.append(str(myPeer) + '\r\n')
    messs = ''.join(data)
    socc.sendall(messs.encode())
    socc.close()
    


def setTCPMonitor():
    global Res,myPeerQuit,s1,s2
    soc = socket(AF_INET,SOCK_STREAM)
    soc.bind((localHost,myPort))
    soc.listen(8)

    while True:
        coon,addr = soc.accept()
        while True:
            data = coon.recv(1024)
            mess = data.decode().split('\r\n')
            if mess[0] == 'fileReq':
                handleFileReq(data,mess)
                break
            elif mess[0] == 'fileRes':
                print('\nReceived a response message from peer {}, which has the file {}.'.format(mess[1],mess[2]))
                break
            elif mess[0] == 'sendLea':
                handleLea(mess)
                Res = 0
                break
            elif mess[0] == 'resLea':
                myPeerQuit = 1
                print('Peer {} now leave'.format(myPeer))
                break
            elif mess[0] == 'askS':
                handleAskS(mess)
                break
            elif mess[0] == 'ResS':
                handleResS(mess)
                break
                
        coon.close()

def handleLea(mess):
    global s1,s2
    if int(mess[1]) == s1:
        s1 = s2
        s2 = int(mess[2])
    else:
        s2 = int(mess[2])
    soc = socket(AF_INET,SOCK_STREAM)
    soc.connect((localHost,int(mess[1]) + starterPort))
    data = []
    data.append('resLea\r\n')
    data.append(str(myPeer) + '\r\n')
    messs = ''.join(data)
    soc.sendall(messs.encode())
    soc.close()
    print('\nPeer {} will depart from the network.'.format(int(mess[1])))
    print('My first successor is now peer {}.'.format(s1))
    print('My second successor is now peer {}.\n'.format(s2))

    

def handleFileReq(data,mess):
    fileFind = 0
    fileNum = int(mess[2]) % 256
    if fileNum == myPeer:
        fileFind = 1
    elif p1 < fileNum and fileNum < myPeer:
        fileFind = 1
    elif myPeer < p1:
        if fileNum > p1 or fileNum < myPeer:
            fileFind = 1

    socc = socket(AF_INET,SOCK_STREAM)

    if fileFind:
        socc.connect((localHost,int(mess[1])+ starterPort))
        data = []
        data.append('fileRes\r\n')
        data.append(str(myPeer) + '\r\n')
        data.append(mess[2] + '\r\n')
        messs = ''.join(data)
        socc.sendall(messs.encode())
        print('\nFile {} is here.'.format(mess[2]))
        print('A response message, destined for peer {}, has been sent.'.format(mess[1]))   
    elif not fileFind:
        socc.connect((localHost,s1Port))
        socc.sendall(data)
        print('\nFile request message has been forwarded to my successor.')

    socc.close()

def handleAskS(mess):
    global s1
    socc = socket(AF_INET,SOCK_STREAM)
    socc.connect((localHost,int(mess[2]) + starterPort))    
    data = []
    data.append('ResS\r\n')
    data.append(str(mess[1]) + '\r\n')
    data.append(str(s1) + '\r\n')
    messs = ''.join(data)
    socc.sendall(messs.encode())
    socc.close()

def handleResS(mess):
    global s1,s2,lastAccPing1,lastAccPing2,lastSendPing1,lastSendPing2
    if int(mess[1]) == 1:
        s1 = s2
        s2 = int(mess[2])
        lastAccPing1 = lastSendPing1
        lastAccPing2 = lastSendPing2
        print('\nMy first successor is now peer {}.'.format(s1))
        print('\nMy second successor is now peer {}.'.format(mess[2]))
    else:
        s2 = int(mess[2])
        lastAccPing2 = lastSendPing2
        lastAccPing2 = lastSendPing2
        print('\nMy first successor is now peer {}.'.format(s1))
        print('\nMy second successor is now peer {}.'.format(mess[2]))

if __name__ == '__main__':
	main()

