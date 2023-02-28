#!/usr/bin/python

import sys

import zerorpc

import zmaster

PORT = 1400
ZOO_IP = 'localhost'

def main(argv):

    # easy way to create master on port other than 1400 if needed
    if argv:
        ip = argv[0]
    else:
        ip = ZOO_IP

    s = zerorpc.Server(zmaster.ZMaster(zoo_ip=ip))
    # connect to master
    s.bind('tcp://*:%d' % PORT)

    print(('Registering master on port {}'.format(PORT)))

    try:
        s.run()
    except:
        print('Unable to start master')
    finally:
        print(('Closing master on port %s'.format(PORT)))
        s.close()
        # scheduler.shutdown()
        
if __name__ == '__main__':
    main(sys.argv[1:])
