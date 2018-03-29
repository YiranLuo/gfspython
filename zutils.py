"""A list of utility functions for the classes and functions for easy creation
of test objects like zookeeper client, regular client"""

import logging
import subprocess


def get_myip():
    """ Returns IP address.
    ref https://stackoverflow.com/questions/166506/finding-local-ip-addresses-using-pythons-stdlib
    """
    import socket
    return [l for l in (
        [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [
            [(s.connect(('8.8.8.8', 80)), s.getsockname()[0], s.close()) for s in
             [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]


def get_tcp(port=''):
    """ Return a tcp protocol string using local ip with optional port added"""
    port_string = ':{}'
    return f'tcp://{get_myip()}{port_string.format(port) if port else ""}'


def get_zk(addr='localhost:2181'):
    """
    Starts a `KazooClient` to interface with the Zookeeper server.
    :param addr: IP and port of the Zookeeper service.
    :return: `KazooClient` object connected to the Zookeeper service.
    """
    from kazoo.client import KazooClient
    from kazoo.handlers.threading import KazooTimeoutError

    zookeeper = KazooClient(hosts=addr)
    try:
        zookeeper.start(timeout=0)
    except KazooTimeoutError:
        logging.getLogger(__name__).critical('Could not connect to zookeeper')
        raise

    return zookeeper


def get_client():
    """ Start and return a `ZClient` object"""
    from zclient import ZClient
    return ZClient()


def get_stats():
    """ Returns network traffic and storage stats for this chunkserver. """
    # TODO interface enP0s3 is not guaranteed, this should not be hard coded.
    results = []
    pattern = r' \d+[\.]?\d*'
    first = ['ifstat', '-q', '-i', 'enP0s3', '-S', '0.2', '1']  # get network traffic
    second = ['df', '/']  # get free space
    p1 = subprocess.Popen(first, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(second, stdout=subprocess.PIPE)

    # get transfer speed and parse results
    transfer_speed = p1.communicate()[0]
    transfer_speed = re.findall(pattern, transfer_speed)
    results.append(sum([float(num) for num in transfer_speed]))

    # get storage info and parse results
    storage = p2.communicate()[0]
    storage = re.findall(r'\d+%', storage)  # find entry with %
    results.append(int(storage[0][:-1]))  # append entry without %

    return results

# def get_mem(servername):
# res = os.popen('ssh %s "grep MemFree /proc/meminfo | sed \'s/[^0-9]//g\'"' % servername)
# return res.read().strip()
# vnstat -i eth0 -tr | grep rx | awk '{print "RX rate: " $2 " " $3}'
#  RX rate: 62.18 kB/s
# vnstat -i eth0 -tr | grep tx | awk '{print "TX rate: " $2 " " $3}'
#  TX rate: 120.54 kB/s
# vnstat takes too long
# ifstat -q -i wlan0 -S 0.1 1 | perl -n -e '/(\d+\.\d+).*(\d+\.\d+)/ && print "Down: $1 KBps - Up: $2 KBps\n"'
