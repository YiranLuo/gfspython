import os
import zerorpc


class ZChunkserver:

    def __init__(self, chunkloc):
        self.chunkloc = chunkloc
        self.chunktable = {}
        self.local_filesystem_root = "/tmp/gfs/chunks/" + repr(chunkloc)
        self.master = zerorpc.Client()
        self.masterloc = 'tcp://localhost:1400'
        self.master.connect(self.masterloc)
        if not os.access(self.local_filesystem_root, os.W_OK):
            os.makedirs(self.local_filesystem_root)

    def print_name(self):
        """
        Prints name to test connectivity
        """
        print 'I am chunkserver' + str(self.chunkloc)
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

    def delete(self, chunkuuids):
        for chunkid in chunkuuids:
            filename = self.chunk_filename(chunkid)
            try:
              if os.path.exists(filename):
		 print "Removing "+filename
                 os.remove(filename)
		 return True
            except:
              None
    def disp(self,a):
	print str(a)+ str(self.chunkloc)

    def chunk_filename(self, chunkuuid):
        local_filename = self.local_filesystem_root + "/" \
            + str(chunkuuid) + '.gfs'
        return local_filename
