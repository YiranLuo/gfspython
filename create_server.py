#!/usr/bin/python
""" Creates a chunkserver for OFS.

This module creates an instance of `zchunkserver.ZChunkserver` and registers with Zookeeper.  After registration,
the chunkserver is able to upload its metadata and query Zookeeper for the location of the master and other
chunkservers.

Example:

    $ python create_server.py -p 4500 -z localhost:2121
"""

import argparse
import logging

from zmq import ZMQError
import zerorpc

import zchunkserver
import zutils


def main():
    """Parse args and start the server"""
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description='Start chunkserver')
    parser.add_argument('-p', '--port', default=4400, type=int, help='Base port to start from, default 4400. '
                                                                     'Chunkserver registration number added to base.')
    parser.add_argument('-z', '--zoo-ip', dest='zoo_ip', default='localhost', type=str, help='IP address of zookeeper')

    args = parser.parse_args()
    chunkserver = zchunkserver.ZChunkserver(zoo_ip=args.zoo_ip)
    reg_num = int(chunkserver.chunkloc)
    port = args.port + reg_num
    address = f'tcp://{zutils.get_myip()}:{port}'

    serv = zerorpc.Server(chunkserver)
    serv.bind(address)
    logger.info(f'Registered chunkserver number {reg_num} at {address}')
    try:
        serv.run()
    except ZMQError as err:
        logging.exception(err.strerror)
        raise SystemExit('Unable to start server due to exception:  {e.strerror}')
    except KeyboardInterrupt:
        pass
    finally:
        logger.info(f'Closing server at {address}')
        serv.close()


if __name__ == '__main__':
    main()
