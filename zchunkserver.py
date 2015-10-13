import os
import zerorpc
import socket


class ZChunkserver:

    def __init__(self, master_ip='localhost', port=1400):
        self.chunktable = {}

        self.master = zerorpc.Client()
        self.master_address = 'tcp://%s:%d' % (master_ip, port)
        print 'Connecting to master at %s' % self.master_address
        self.master.connect(self.master_address)

        # get chunkserver number, send ip to master to register
        myip = self.get_myip()
        print 'Registering with ip %s' % myip
        self.chunkloc = self.master.register_chunk(myip)

        # local directory where chunks are stored
        self.local_filesystem_root = "/tmp/gfs/chunks/" + repr(self.chunkloc)
        if not os.access(self.local_filesystem_root, os.W_OK):
            os.makedirs(self.local_filesystem_root)

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

    # temporary solution taken from stackoverflow to get ip
    def get_myip(self):
        return([l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(('8.8.8.8', 80)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0])

