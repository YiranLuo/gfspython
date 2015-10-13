import zerorpc
import zchunkserver
import sys

MASTER_IP = 'localhost'


def main(argv):

    if argv:
        master_ip = str(argv[0])
    else:
        master_ip = MASTER_IP

    chunkserver = zchunkserver.ZChunkserver(master_ip=master_ip)
    reg_num = chunkserver.chunkloc
    s = zerorpc.Server(chunkserver)
    port = 4400 + reg_num
    address = 'tcp://*:%d' % port
    print 'Registering chunkserver %d on port %s' % (reg_num, address)
    s.bind(address)
    try:
        s.run()
    except:
        print 'Closing server on port %s' % address
        s.close()
        
if __name__ == '__main__':
    main(sys.argv[1:])

