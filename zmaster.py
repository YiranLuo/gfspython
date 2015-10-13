import uuid
import time
import zerorpc
import operator

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

    def list(self):
        """ Returns a list of files that do not start with /hidden/deleted (marked
        for deletion """
        return [key for key in self.filetable.keys()
                if not key.startswith('/hidden/')]

    def register_chunk(self, ip, base_port=4400):
        """
        :param base_port: Beginning tcp port for chunkserver reg.
         default port is base_port + chunkserver_number (4403 for chunkserver #3)
        :param ip:  The ip of the chunkserver registering
        :return: chunkserver number
        """
        chunkserver_number = self.num_chunkservers
        self.num_chunkservers += 1
        port_num = base_port + chunkserver_number
        address = 'tcp://%s:%d' % (ip, port_num)
        self.chunkservers[chunkserver_number] = address

        c = zerorpc.Client()
        c.connect(address)
        self.chunkclients[chunkserver_number] = c
        print 'Chunksrv #%d registered at %s' % (chunkserver_number, address)
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

    def alloc_append(self, filename, num_append_chunks):  # append chunks
        chunkuuids = self.filetable[filename]
        append_chunkuuids = self.alloc_chunks(num_append_chunks)
        chunkuuids.extend(append_chunkuuids)
        return append_chunkuuids

    def dump_metadata(self):
        print "Filetable:",
        for filename, chunkuuids in self.filetable.items():
            print filename, "with", len(chunkuuids), "chunks"
        print "Chunkservers: ", len(self.chunkservers)
        print "Chunkserver Data:"
        for chunkuuid, chunkloc in sorted(self.chunktable.iteritems(),
                                          key=operator.itemgetter(1)):
            chunk = self.chunkclients[chunkloc].read(chunkuuid)
            print chunkloc, chunkuuid, chunk

    def delete(self, filename):  # rename for later garbage collection
        chunkuuids = self.filetable[filename]
        del self.filetable[filename]
        timestamp = repr(time.time())
        deleted_filename = "/hidden/deleted/" + timestamp + filename
        self.filetable[deleted_filename] = chunkuuids
        print "deleted file: " + filename + " renamed to " + \
             deleted_filename + " ready for gc"





