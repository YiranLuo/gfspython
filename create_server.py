import zerorpc
import zchunkserver

__author__ = 'Shane'


def main():

    c = zerorpc.Client()
    # connect to master
    c.connect('tcp://localhost:1400')
    reg_num = c.register_chunk()

    s = zerorpc.Server(zchunkserver.ZChunkserver(reg_num))
    port = 4400 + reg_num
    addr = 'tcp://*:%d' % port
    print 'Registering chunkserver %d on port %s' % (reg_num, addr)
    s.bind(addr)
    try:
        s.run()
    except:
        print 'Closing server on port %s' %addr
        s.close()
        
if __name__ == '__main__':
    main()

