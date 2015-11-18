import zerorpc


class ZClient:

    def __init__(self):
        self.master = zerorpc.Client()
        self.master.connect('tcp://localhost:1400')

    def write(self, filename, data):
        """
        Creates a new file, writes the data
        :param filename:
        :param data:
        """

        if self._exists(filename):
           self.master.updatevrsn(filename,1)
	   self.edit(filename,data)
	else:
	   seq=0
           self.master.updatevrsn(filename,0)

           num_chunks = self._num_chunks(len(data))
           chunkuuids = self.master.alloc(filename, num_chunks, seq)
           self._write_chunks(chunkuuids, data)

    def _exists(self, filename):
        return self.master.exists(filename)

    def _num_chunks(self, size):
        chunksize = self.master.get('chunksize')
        return (size // chunksize) + (1 if size % chunksize > 0 else 0)

    def _write_chunks(self, chunkuuids, data):
        chunksize = self.master.get('chunksize')
        chunks = [data[x:x+chunksize] for x in range(0, len(data), chunksize)]

        # connect with each chunkserver. Change to check/establish later
        chunkserver_clients = self._establish_connection()

        # write to each chunkserver
        for idx, chunkuuid in enumerate(chunkuuids):
            chunkloc = self.master.get_chunkloc(chunkuuid)[0]
            chunkserver_clients[chunkloc].write(chunkuuid, chunks[idx])

    def _establish_connection(self):
        """
        Creates zerorpc client for each chunkserver
        :return:  Dictionary of zerorpc clients bound to chunkservers
        """
        chunkserver_client = {}
        chunkservers = self.master.get('chunkservers')

        for idx in range(len(chunkservers)):
            zclient = zerorpc.Client()
            print 'Client connecting to chunkserver at %s' % chunkservers[idx]
            zclient.connect(chunkservers[idx])
            zclient.print_name()
            chunkserver_client[idx] = zclient

        return chunkserver_client

    def read(self, filename):  # get metadata, then read chunks direct
        """
        Connects to each chunkserver and reads the chunks in order, then
        assembles the file by reducing
        :param filename:
        :return:  file contents
        """
        if not self._exists(filename):
            raise Exception("read error, file does not exist: " + filename)

        if filename=="#garbage_collection#":
	   print self.master.get_chunkuuids(filename)
	else:
         chunks = []
         chunkuuids = self.master.get_chunkuuids(filename)
	 print chunkuuids
         chunkserver_clients = self._establish_connection()
	 print self.master.see_chunkloc()
         for chunkuuid in chunkuuids:
            chunkloc = self.master.get_chunkloc(chunkuuid)[0]
            chunk = chunkserver_clients[chunkloc].read(chunkuuid)
            chunks.append(chunk)
         data = reduce(lambda x, y: x + y, chunks)  # reassemble in order
         return data

    def deletechunk(self, chunkdetails, len_newdata, len_olddata, chunksize):  
	x=y=0
	filename="test.txt"
	chunkids=[]
        chunkserver_clients = self._establish_connection()
        for chunkuuid in chunkdetails:
	    if x>len_newdata:
	       chunkids.append(chunkuuid['chunkuid'])
	    x+=chunksize
	self.master.delete_chunks(filename,chunkids)
	return 'True'

    def replacechunk(self, chunkdetails, data1, data2, chunksize):  
	x=y=0
	chunkserver_clients = self._establish_connection()#can be avoided, pass from the edit function
	for x in range(0, len(data1), chunksize):
		#and len(data2[x:x+chunksize])<=chunksize
		if data1[x:x+chunksize]!=data2[x:x+chunksize] or len(data2[x:x+chunksize])<chunksize:
		   print "replace '"+data1[x:x+chunksize]+"' with '"+data2[x:x+chunksize]+"'"
		   for i in chunkdetails[y]['chunkloc']:
		       #chunkserver_clients[chunkdetails[y]['chunkloc']].write(chunkdetails[y]['chunkuid'], data2[x:x+chunksize])
		       chunkserver_clients[i].write(chunkdetails[y]['chunkuid'], data2[x:x+chunksize])
	        y+=1
	return 'True'

    def append(self, filename, data):
        if not self._exists(filename):
            raise Exception("append error, file does not exist: " + filename)
	else:
            num_chunks = self._num_chunks(len(data))
            chunkuuids = self.master.get_chunkuuids(filename)[-1]
	    seq = int(chunkuuids.split('$%#')[1]) + 1

            append_chunkuuids = self.master.alloc_append(num_chunks, filename, seq)
            self._write_chunks(append_chunkuuids, data)
 
    def delete(self, filename):
	if not self._exists(filename):
            raise Exception("append error, file does not exist: " + filename)
	else:
	    self.master.delete(filename,"")

    def edit(self, filename,newdata):  
	"""
	Read the file with the read() from above and update only the 
	chunkservers where the data in the chunk has changed
	"""

        if not self._exists(filename):
            raise Exception("read error, file does not exist: " + filename)

        chunks = []
	chunkdetails=[]
	i=0
        chunkuuids = self.master.get_chunkuuids(filename)
        chunkserver_clients = self._establish_connection()

        for chunkuuid in chunkuuids:
	    temp={}
	    #maybe use subprocess to execute the download process in parallel
	    #may throw error if chunkserver dies off in between
            chunkloc = self.master.get_chunkloc(chunkuuid)
            chunk = chunkserver_clients[chunkloc[0]].read(chunkuuid)
	    temp['chunkloc']=chunkloc
	    temp['chunkuid']=chunkuuid
	    temp['chunk']=chunk
	    chunkdetails.append(temp)
            chunks.append(chunk)

        olddata = reduce(lambda x, y: x + y, chunks)  # reassemble in order

	print "\nCurrent data in "+filename+"\n"+olddata+"\nEdited data:\n"+newdata
	
	newchunks = []
        chunksize = self.master.get('chunksize')
	len_newdata=len(newdata)
	len_olddata=len(olddata)
        newchunks = [newdata[x:x+chunksize] for x in range(0, len_newdata, chunksize)]

	if len_newdata==len_olddata:
	   if newdata == olddata:
	      print "no change in contents"
	   else:
	      print "same size but content changed"
	      x=self.replacechunk(chunkdetails, olddata, newdata, chunksize)
	elif len_newdata<len_olddata:
	   #print "deleted some contents"
	   x=self.replacechunk(chunkdetails, olddata[0:len_newdata],newdata, chunksize)
	   print "call fn() to delete chunks "+olddata[len_newdata+1:]+" from chunk server"
	   x=self.deletechunk(chunkdetails, len_newdata, len_olddata, chunksize)
	elif len_newdata>len_olddata:
	   #print "added some contents"
           x=self.replacechunk(chunkdetails, olddata, newdata[0:len_olddata], chunksize)
           print "call fn() to add chunks '"+newdata[len_olddata+1:]+"' to chunk server"
	   self.append(filename, newdata[len_olddata:])
	  
        self.master.updatevrsn(filename,1)
	
	
    def rename(self, filename, newfilename):  
	if self._exists(filename):
	   if not self._exists(newfilename):
	      chunkuids = self.master.get_chunkuuids(filename)

              result={}
	      for chunkuid in chunkuids:
            	  #maybe use subprocess to execute the download process in parallel
            	  #may throw error if chunkserver dies off in between
            	  chunklocs = self.master.get_chunkloc(chunkuid)
		  for chunkloc in chunklocs:
		      try:
			 result[chunkloc].append(chunkuid)
		      except:
			 result[chunkloc]=[]
			 result[chunkloc].append(chunkuid)

	      self.master.rename(result, filename, newfilename)

           else:
	      print "read error, file already exist: " + newfilename
        else:
	   print "read error, file does not exist: " + filename

