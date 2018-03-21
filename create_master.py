#!/usr/bin/python

import logging
import sys

import zerorpc

import zmaster

PORT = 1400
ZOO_IP = 'localhost'


def main(argv):
    logger = logging.getLogger(__name__)
    if argv:
        ip = argv[0]
    else:
        ip = ZOO_IP

    s = zerorpc.Server(zmaster.ZMaster(zoo_ip=ip))
    # connect to master
    s.bind('tcp://*:%d' % PORT)

    logger.info(f'Registering master on port {PORT}')

    try:
        s.run()
    except Exception:
        logger.error(f'Unable to start master')
        raise
    finally:
        logger.info(f'Closing master on port {PORT}')
        s.close()
        # scheduler.shutdown()


if __name__ == '__main__':
    main(sys.argv[1:])
