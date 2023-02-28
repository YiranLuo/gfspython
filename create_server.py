#!/usr/bin/python

import sys

from zmq import ZMQError
import zerorpc

import zutils
import zchunkserver


ZOO_IP = 'localhost'


def main(argv):

    if argv:
        zoo_ip = str(argv[0])
    else:
        zoo_ip = ZOO_IP

    chunkserver = zchunkserver.ZChunkserver(zoo_ip=zoo_ip)
    reg_num = int(chunkserver.chunkloc)
    # reg_num = 0
    s = zerorpc.Server(chunkserver)
    port = 4400 + reg_num
    address = 'tcp://%s:%d' % (zutils.get_myip(), port)

    try:
        print('Registering chunkserver %d on at %s' % (reg_num, address))
        s.bind(address)
        s.run()
    except ZMQError as e:
        print("Unable to start server: " + e.strerror)
        s.close()
        sys.exit(2)
    except KeyboardInterrupt:
        pass
    finally:
        print('Closing server on %s' % address)
        s.close()
        
if __name__ == '__main__':
    main(sys.argv[1:])

