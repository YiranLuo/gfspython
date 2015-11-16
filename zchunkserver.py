import os
import subprocess
import re

import zerorpc
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NoNodeError

import zutils


class ZChunkserver:

    def __init__(self, zoo_ip='localhost:2181'):
        self.chunktable = {}
        self.chunkloc = None
        self.master = zerorpc.Client()
        self.zookeeper = KazooClient(zoo_ip)

        # register with zookeeper, get IP of master
        # TODO:  need to add handling in case master is down here
        try:
            self.master_ip = self._register_with_zookeeper()
            print 'Chunkserver %d Connecting to master at %s' % (int(self.chunkloc), self.master_ip)
            self.master.connect(self.master_ip)
        except NoNodeError:
            print "No master record in zookeeper"
            raise  # TODO handle shadow master/waiting for master to reconnect later
        except:
            print "\n\tSome other error happened:"
            raise

        # local directory where chunks are stored
        self.local_filesystem_root = "/tmp/gfs/chunks/" + repr(int(self.chunkloc))
        if not os.access(self.local_filesystem_root, os.W_OK):
            os.makedirs(self.local_filesystem_root)

    def _register_with_zookeeper(self):

        def my_listener(state):
            if state == KazooState.LOST or state == KazooState.SUSPENDED:
                print "suspended|lost state"
                # TODO connect to zookeeper again

        self.zookeeper.start()
        self.zookeeper.add_listener(my_listener)
        self.zookeeper.ensure_path('chunkserver')
        master_ip = self.zookeeper.get('master')[0]

        path = self.zookeeper.create('chunkserver/', ephemeral=True, sequence=True)
        self.chunkloc = path.replace('/chunkserver/', '')
        self.zookeeper.set(path, zutils.get_tcp(4400 + int(self.chunkloc)))

        return master_ip

    def print_name(self):
        """
        Prints name to test connectivity
        """
        print 'I am chunkserver #' + str(int(self.chunkloc))
        self.master.answer_server(int(self.chunkloc))

    def write(self, chunkuuid, chunk):
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "w") as f:
            f.write(chunk)
        self.chunktable[chunkuuid] = local_filename

    def read(self, chunkuuid):
        data = None
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "r") as f:
            data = f.read()
        return data

    def chunk_filename(self, chunkuuid):
        local_filename = self.local_filesystem_root + "/" \
            + str(chunkuuid) + '.gfs'
        return local_filename

    def close(self):
        self.master.close()

    @staticmethod
    def get_stats():

        results = []
        pattern = r' \d+[\.]?\d*'
        first = ['ifstat', '-q', '-i', 'wlan0', '-S', '0.1', '1']  # get network traffic
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
