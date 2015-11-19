import os
import zerorpc
import ast


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
	self.populate()

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

    def rwrite(self, chunkuuid, chunk):
        local_filename = self.chunk_filename(chunkuuid)
	try:
           with open(local_filename, "w") as f:
             f.write(chunk)
           self.chunktable[chunkuuid] = local_filename
	   return True
	except:
	   return False

    def read(self, chunkuuid):
        data = None
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "r") as f:
            data = f.read()
        return data

    def _establish_connection(self,chunkloc):
	chunkservers = self.master.get('chunkservers')
        zclient = zerorpc.Client()
        print 'Server connecting to chunkserver at %s' % chunkloc
        zclient.connect(chunkservers[chunkloc])
        zclient.print_name()
        return zclient

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

    def copy_chunk(self,chunkid,chunklocs):
	chunklocs=ast.literal_eval(chunklocs)
	flag=False
	for chunkloc in chunklocs:
	  try:
	    chunkserver=self._establish_connection(chunkloc)
	    data=chunkserver.read(chunkid)
	    flag=self.rwrite(chunkid,data)
	    if flag:
	       break
	  except:
	    flag=False
	    print "soe"
	
	return flag

    def rename(self, chunkids, filename, newfilename):
	for chunkid in chunkids:
	    local_filename = self.chunk_filename(chunkid)
	    new_local_filename=local_filename.replace(filename, newfilename)
	    print new_local_filename

            try:
                os.rename(local_filename, new_local_filename)
            except WindowsError:
                os.remove(new_local_filename)
                os.rename(local_filename, new_local_filename)
	return True

    def populate(self):
	print "in populate"
	local_dir=self.chunk_filename("").replace(".gfs","")
	file_list=os.listdir(local_dir)
	if file_list!=[]:
	  files={}
	  for items in file_list:
	    items=items.replace(".gfs","")
	    filename=items.split("$%#")[0]
	    self.chunktable[items] = self.chunk_filename(items)
	    try:
		files[filename].append(items)
	    except:
		files[filename]=[]
		files[filename].append(items)
	
	  self.master.populate(files, str(self.chunkloc))
	else:
	  print "nothing to populate"
	

