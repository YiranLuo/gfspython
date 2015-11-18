import uuid
import time
import zerorpc
import sys
import ast
from apscheduler.schedulers.background import BackgroundScheduler


class ZMaster:

    def __init__(self):
        self.num_chunkservers = 0
        self.max_chunkservers = 10
        self.max_chunksperfile = 100
        self.chunksize = 10
        self.chunkrobin = 0
        self.versntable = {}  # file version counter
        self.filetable = {}  # file to chunk mapping
        self.chunktable = {}  # chunkuuid to chunkloc mapping
        self.chunkservers = {}  # loc id to chunkserver mapping
        self.chunkclients = {}
        #self.init_chunkservers()

        #this schedules background tasks in separate thread
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.collect_garbage, 'interval', minutes=0.2)
        scheduler.add_job(self.replicate, 'interval', minutes=0.1)
        scheduler.start()

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

    def see_chunkloc(self):
        return self.chunktable

    def get_chunkuuids(self, filename):
        return self.filetable[filename]

    def alloc(self, filename, num_chunks, seq):  # return ordered chunk list
        chunks = self.alloc_chunks(num_chunks, filename, seq)
        self.filetable[filename] = chunks
        return chunks

    def updatevrsn(self, filename,flag):
	if flag==1:
           self.versntable[filename]+=1
	else:
	   self.versntable[filename]=0
	print filename, self.versntable[filename]

    def _establish_connection(self,chunkloc):
        """
        Creates zerorpc client for each chunkserver
        :return:  Dictionary of zerorpc clients bound to chunkservers
        """
        #chunkserver_client = {}
        chunkservers = self.get('chunkservers')

	if True:
            zclient = zerorpc.Client()
            print 'Server connecting to chunkserver at %s' % chunkservers[chunkloc]
            zclient.connect(chunkservers[chunkloc])
            zclient.print_name()
            #chunkserver_client[chunkloc] = zclient
            return zclient
	    #chunkserver_client

    def collect_garbage(self):
	try:
	   chunklocs=self.filetable["#garbage_collection#"]
	except:
	   self.filetable["#garbage_collection#"]={}
	   chunklocs={}

	if chunklocs:
	   print "in garbage"
	   for chunkloc in chunklocs.keys():
	       # connect with each chunkserver. Change to check/establish later
       	       chunkserver_clients = self._establish_connection(chunkloc)

	       #print "call delchunkfile fn() in chunkserver-"+str(chunkloc)
	       flag=chunkserver_clients.delete(chunklocs[chunkloc])
	       if flag==True:
		  #print "remove value from garbage collection for "+str(chunkloc)
		  del self.filetable["#garbage_collection#"][chunkloc]
	else:
	   print "nothing to clear in garbage"

    def replicate(self):
	replicas={}
	no_servers=self.num_chunkservers
	reps=min(3, no_servers)
	#self.chunktable={'test.txt$%#0$%#17229618-8c09-11e5-8017-000c29c12a87': [0], 'test.txt$%#1$%#17229619-8c09-11e5-8017-000c29c12a87': [1]}
	
	chunktable=self.chunktable
	chunkserver={}
	values=[]
	for chunkid,values in chunktable.items():
	    temp=str(values)
	    values=ast.literal_eval(temp)
	    while len(values) < reps:
	       self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers
	       chunkloc = self.chunkrobin
	       while chunkloc in values:
		  self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers
	          chunkloc = self.chunkrobin

	       #print "call connection to "+str(chunkloc)+" pass ",chunkid,temp
	       if not chunkloc in chunkserver:
	          try:
	             chunkserver[chunkloc]=self._establish_connection(chunkloc)
		  except:
		     None

	       if chunkserver[chunkloc].copy_chunk(chunkid,temp):
		  print "Update chunktable"
		  self.chunktable[chunkid].append(chunkloc)

	       result={}
	       result[chunkid]=temp
	       try:
		replicas[chunkloc].append(result)
	       except:
		replicas[chunkloc]=[]
		replicas[chunkloc].append(result)

	       values.append(chunkloc)

	if len(replicas) == 0:
	     print "Nothing to do in replicate"
	else:
	     print "replica: ",replicas

    def delete(self, filename, chunkuuids):  # rename for later garbage collection
	if chunkuuids=="":
          chunkuuids = self.filetable[filename]
          del self.filetable[filename]
        timestamp = repr(time.time())
        deleted_filename = "#garbage_collection#"

	try:
	    if self.filetable[deleted_filename]:
	       None	
	except:
	    self.filetable[deleted_filename]={}
	    #self.filetable[deleted_filename]=[]

	for chunkid in chunkuuids:
	    chunklocs = self.get_chunkloc(chunkid)
	    for chunkloc in chunklocs:
	      try:
                self.filetable[deleted_filename][chunkloc].append(chunkid)
	      except:
		self.filetable[deleted_filename][chunkloc]=[]
		self.filetable[deleted_filename][chunkloc].append(chunkid)
	    del self.chunktable[chunkid]

	print self.filetable[deleted_filename]
	#self.collect_garbage()

    def delete_chunks(self, filename, chunk_rm_ids):
	chunkuuids=self.filetable[filename] 
	self.filetable[filename]=[x for x in chunkuuids if x not in chunk_rm_ids]
	self.delete(filename,chunk_rm_ids)
	
    def alloc_chunks(self, num_chunks, filename, seq):
        chunkuuids = []
	tseq=seq
        for i in range(0, num_chunks):
            chunkuuid = filename+"$%#"+str(tseq)+"$%#"+str(uuid.uuid1())
            chunkloc = self.chunkrobin
            self.chunktable[chunkuuid] = [chunkloc]
            chunkuuids.append(chunkuuid)
            self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers
	    tseq+=1
        return chunkuuids

    def alloc_append(self, num_append_chunks, filename, seq):  # append chunks
        chunkuuids = self.filetable[filename]
	tseq=seq
        append_chunkuuids = self.alloc_chunks(num_append_chunks, filename, tseq)
        chunkuuids.extend(append_chunkuuids)
        return append_chunkuuids

    def rename(self, result, filename, newfilename):
	chunkserver={}
	chunkids=[]
	flag=True
	for chunkloc, chunkids in result.items():
	    try:
               chunkserver[chunkloc]=self._establish_connection(chunkloc)
            except:
               print "Coudnt connect to chunkserver ",chunkloc

	    flag=True
	    no_keys=len(chunkids)
	    flag=flag and chunkserver[chunkloc].rename(chunkids, filename, newfilename)

	if flag:
	   for chunkid in self.filetable[filename]:
	       temp=str(chunkid).replace(filename, newfilename)
	       self.chunktable[temp]=self.chunktable.pop(chunkid)
	   self.filetable[newfilename]=ast.literal_eval(str(self.filetable.pop(filename)).replace(filename, newfilename))
	else:
	   print "Some error occured while renaming"

