import uuid
import time
import zerorpc


class ZMaster:

    def __init__(self):
        self.num_chunkservers = 0
        self.max_chunkservers = 10
        self.max_chunksperfile = 100
        self.chunksize = 10
        self.chunkrobin = 0
        self.filetable = {}  # file to chunk mapping
        self.chunktable = {}  # chunkuuid to chunkloc mapping
        self.chunkservers = {}  # loc id to chunkserver mapping
        self.chunkclients = {}
        # self.init_chunkservers()

    def get(self, ivar):
        """
        Exposes ZMaster member variables through method access.
        :param ivar: instance variable of ZMaster
        :return:  returns the value of the instance variable
        """
        try:
            return self.__dict__[ivar]
        except KeyError:
            print 'Key error'
            raise

    def register_chunk(self, base_port=4400):
        """
        :param base_port: Beginning tcp port for chunkserver reg.
         default port is base_port + chunkserver_number (4403 for chunkserver #3)
        :return: chunkserver number
        """
        chunkserver_number = self.num_chunkservers
        self.num_chunkservers += 1
        access_port = 'tcp://localhost:' + str(base_port + chunkserver_number)
        self.chunkservers[chunkserver_number] = access_port

        c = zerorpc.Client()
        c.connect(access_port)
        self.chunkclients[chunkserver_number] = c
        print 'Chunksrv #%d registered on port %s' % (
            chunkserver_number, access_port)
        return chunkserver_number

    # temporary functions
    def exists(self, filename):
        return True if filename in self.filetable else False

    def call_servers(self):
        """
        Each chunk server prints its name to console using zerorpc client
        """
        for client in self.chunkclients.values():
            client.print_name()

    def answer_server(self, chunknum):
        print 'Master ack from %d' %chunknum

    def get_chunkloc(self, chunkuuid):
        return self.chunktable[chunkuuid]

    def get_chunkuuids(self, filename):
        return self.filetable[filename]

    def alloc(self, filename, num_chunks):  # return ordered chunk list
        chunks = self.alloc_chunks(num_chunks)
        self.filetable[filename] = chunks
        return chunks

    def alloc_chunks(self, num_chunks):
        chunkuuids = []
        for i in range(0, num_chunks):
            chunkuuid = str(uuid.uuid1())
            chunkloc = self.chunkrobin
            self.chunktable[chunkuuid] = chunkloc
            chunkuuids.append(chunkuuid)
            self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers
        return chunkuuids







