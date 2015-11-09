import os
import zerorpc
from kazoo.client import KazooClient
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
            print 'Chunkserver %d Connecting to master at %s' % (self.chunkloc, self.master_ip)
            self.master.connect(self.master_ip)
        except NoNodeError:
            print "No master record in zookeeper"
        except:
            print "Some other error happened"
            raise

        # get chunkserver number, send ip to master to register
        # myip = zutils.get_myip()
        # print 'Registering with ip %s' % myip
        # self.chunkloc = self.master.register_chunk(myip)

        # local directory where chunks are stored
        self.local_filesystem_root = "/tmp/gfs/chunks/" + repr(self.chunkloc)
        if not os.access(self.local_filesystem_root, os.W_OK):
            os.makedirs(self.local_filesystem_root)

    def _register_with_zookeeper(self):
        self.zookeeper.start()
        self.zookeeper.ensure_path('chunkserver')
        master_ip = self.zookeeper.get('master')[0]

        path = self.zookeeper.create('chunkserver/', ephemeral=True, sequence=True)
        self.chunkloc = int(path.replace('/chunkserver/', ''))
        self.zookeeper.set(path, zutils.get_tcp(4400 + self.chunkloc))

        return master_ip

    def print_name(self):
        """
        Prints name to test connectivity
        """
        print 'I am chunkserver #' + str(self.chunkloc)
        self.master.answer_server(self.chunkloc)

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

