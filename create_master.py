import zerorpc
import zmaster
import sys

PORT = 1400


def main(argv):

    # easy way to create master on port other than 1400 if needed
    if argv and argv[0].isdigit():
        port = argv[0]
    else:
        port = PORT

    s = zerorpc.Server(zmaster.ZMaster())
    # connect to master
    s.bind('tcp://*:%d' % port)

    print 'Registering master on port %s' % port

    try:
        s.run()
    except:
        print 'Closing master on port %s' % port
        s.close()
        
if __name__ == '__main__':
    main(sys.argv[1:])
