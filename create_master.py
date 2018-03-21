#!/usr/bin/python

import logging
import argparse
import zerorpc

import zmaster


def main():
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description='Start master server')
    parser.add_argument('-p', '--port', default=1400, type=int, help='Local port number to start master on, '
                                                                     'default 1400.')
    parser.add_argument('-z', '--zoo-ip', dest='zoo_ip', default='localhost', type=str, help='IP address of zookeeper')

    args = parser.parse_args()
    port, zoo_ip = args.port, args.zoo_ip

    s = zerorpc.Server(zmaster.ZMaster(zoo_ip=zoo_ip))
    # connect to master
    s.bind(f'tcp://*:{port}')

    logger.info(f'Registering master on port {port}')

    try:
        s.run()
    except Exception:
        logger.error(f'Unable to start master')
        raise
    finally:
        logger.info(f'Closing master on port {port}')
        s.close()
        # scheduler.shutdown()


if __name__ == '__main__':
    main()
