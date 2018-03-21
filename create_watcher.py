""" Creates the Master server for OFS that handles metadata and directs clients/chunkservers.

This module creates an instance of `zmaster.ZMaster` and registers with Zookeeper.  After registration,
the master queries all chunkservers registered with zookeeper for their file contents and metadata.

Example:

$ python create_watcher.py -p 1401 -z localhost:2121

Todo:
    * Consolidate into a single create script that accepts master/server/watcher as an operand
"""

import argparse
import logging

from zmq import ZMQError
import zerorpc


import watcher
import zutils


def main():
    """Parse args and start the server"""
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description='Start chunkserver')
    parser.add_argument('-p', '--port', default=1401, type=int, help='Base port to start from, default 4400. '
                                                                     'Chunkserver registration number added to base.')
    parser.add_argument('-z', '--zoo-ip', dest='zoo_ip', default='localhost', type=str, help='IP address of zookeeper')

    args = parser.parse_args()
    shadow = watcher.Watcher(zoo_ip=args.zoo_ip, port=args.port)
    address = f'tcp://{zutils.get_myip()}:{args.port}'

    serv = zerorpc.Server(shadow)
    serv.bind(address)
    logger.info(f'Registered shadow master at {address}')

    try:
        serv.run()
    except ZMQError as err:
        logging.exception(err.strerror)
        raise SystemExit('Unable to start server due to exception:  {e.strerror}')
    except KeyboardInterrupt:
        pass
    finally:
        logger.info(f'Closing shadow master at {address}')
        serv.close()


if __name__ == '__main__':
    main()
