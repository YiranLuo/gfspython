"""A list of utility functions for the classes and functions for easy creation
of test objects like zookeeper client, regular client"""


def get_myip():
    import socket
    """ Returns IP address"""
    return [l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(('8.8.8.8', 80)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]


def get_tcp(port=None):
    if port:
        return "tcp://%s:%d" % (get_myip(), port)
    else:
        return 'tcp://%s' % get_myip()


def get_zk(ip='localhost:2181'):
    from kazoo.client import KazooClient
    from kazoo.handlers.threading import KazooTimeoutError

    zk = KazooClient(hosts=ip)
    try:
        zk.start(timeout=10)
    except KazooTimeoutError:
        print "Couldn't connect to zookeeper"
        zk.stop()

    return zk


def get_client():
    from zclient import ZClient
    client = ZClient()
    return client


# def get_mem(servername):
    # res = os.popen('ssh %s "grep MemFree /proc/meminfo | sed \'s/[^0-9]//g\'"' % servername)
    # return res.read().strip()
    # vnstat -i eth0 -tr | grep rx | awk '{print "RX rate: " $2 " " $3}'
    #  RX rate: 62.18 kB/s
    # vnstat -i eth0 -tr | grep tx | awk '{print "TX rate: " $2 " " $3}'
    #  TX rate: 120.54 kB/s
    # vnstat takes too long
    # ifstat -q -i wlan0 -S 0.1 1 | perl -n -e '/(\d+\.\d+).*(\d+\.\d+)/ && print "Down: $1 KBps - Up: $2 KBps\n"'



