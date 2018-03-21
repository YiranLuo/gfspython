#!/usr/bin/python
""" Creates the Master server for OFS that handles metadata and directs clients/chunkservers.

This module creates an instance of `zmaster.ZMaster` and registers with Zookeeper.  After registration,
the master queries all chunkservers registered with zookeeper for their file contents and metadata.

Example:

    $ python create_master.py -p 1400 -z localhost:2121

Todo:
    * Check if scheduler.shutdown() needs to be explicitly called from ZMaster
"""

import logging
import argparse
import zerorpc

import zmaster


def main():
    """ Parse args and start the master server """
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description='Start master server')
    parser.add_argument('-p', '--port', default=1400, type=int, help='Local port number to start master on, '
                                                                     'default 1400.')
    parser.add_argument('-z', '--zoo-ip', dest='zoo_ip', default='localhost', type=str, help='IP address of zookeeper')

    args = parser.parse_args()
    port, zoo_ip = args.port, args.zoo_ip

    serv = zerorpc.Server(zmaster.ZMaster(zoo_ip=zoo_ip))

    serv.bind(f'tcp://*:{port}')
    logger.info(f'Registered master on port {port}')

    try:
        serv.run()
    except Exception:
        logger.error(f'Unable to start master')
        raise
    finally:
        logger.info(f'Closing master on port {port}')
        serv.close()


if __name__ == '__main__':
    main()
