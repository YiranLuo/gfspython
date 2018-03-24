"""A list of utility functions for the classes and functions for easy creation
of test objects like zookeeper client, regular client"""

import logging


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


def print_exception(context, exception, message=''):
    """

    :param context:
    :param exception:
    :param message:
    """
    print(f'Unexpected error in {context}: {message}')
    if exception:
        print(f'{type(exception).__name__}, : {exception.args}')

# def get_mem(servername):
# res = os.popen('ssh %s "grep MemFree /proc/meminfo | sed \'s/[^0-9]//g\'"' % servername)
# return res.read().strip()
# vnstat -i eth0 -tr | grep rx | awk '{print "RX rate: " $2 " " $3}'
#  RX rate: 62.18 kB/s
# vnstat -i eth0 -tr | grep tx | awk '{print "TX rate: " $2 " " $3}'
#  TX rate: 120.54 kB/s
# vnstat takes too long
# ifstat -q -i wlan0 -S 0.1 1 | perl -n -e '/(\d+\.\d+).*(\d+\.\d+)/ && print "Down: $1 KBps - Up: $2 KBps\n"'
