import sys

from zmq import ZMQError
import zerorpc

import zutils
import watcher

PORT = '1401'
ZOO_IP = 'localhost'


def main(argv):
    if argv:
        zoo_ip = str(argv[0])
    else:
        zoo_ip = ZOO_IP

    chunkserver = watcher.Watcher(zoo_ip=zoo_ip, port=PORT)

    s = zerorpc.Server(chunkserver)

    address = 'tcp://%s:%s' % (zutils.get_myip(), PORT)

    try:
        print('Registering watcher at %s' % address)
        s.bind(address)
        s.run()
    except ZMQError as e:
        print("Unable to start watcher: " + e.strerror)
        s.close()
        sys.exit(2)
    except KeyboardInterrupt:
        pass
    finally:
        print('Closing watcher on %s' % address)
        s.close()


if __name__ == '__main__':
    main(sys.argv[1:])
