import zerorpc
import zmaster

PORT = '1400'


def main():

    s = zerorpc.Server(zmaster.ZMaster())
    # connect to master
    s.bind('tcp://*:' + PORT)

    print 'Registering master on port %s' % PORT

    try:
        s.run()
    except:
        print 'Closing master on port %s' % PORT
        s.close()
        
if __name__ == '__main__':
    main()

