import uuid
import time
import operator
import threading

from kazoo.client import KazooClient
import zerorpc

import zutils

CHUNKSERVER_PATH = 'chunkserver/'
UPDATE_FREQUENCY = 5  # update frequency in seconds

class ZMaster:

    def __init__(self, zoo_ip='localhost:2181', master_port=1400):
        self.num_chunkservers = 0
        self.last_updated = 0  # time since last stats poll
        self.ip = zutils.get_myip() + ':' + str(master_port)
        self.chunksize = 10
        self.chunkrobin = 0
        self.filetable = {}  # file to chunk mapping
        self.chunktable = {}  # chunkuuid to chunkloc mapping
        self.chunkservers = {}  # loc id to chunkserver mapping
        self.chunkclients = {}  # zerorpc clients connected to chunkservers

        self.chunkstats = {}  # stats for capacity and network load

        self.zookeeper = KazooClient(hosts=zoo_ip)
        self._register_with_zookeeper(master_port)

    def _register_with_zookeeper(self, master_port):
        try:
            self.zookeeper.start()
            address = "tcp://%s:%s" % (zutils.get_myip(), master_port)

            # use ephemeral node for shadow master to subscribe to later
            # self.zookeeper.create('master', ephemeral=True, value=address)
            self.zookeeper.ensure_path('master')
            self.zookeeper.ensure_path('chunkserver')
            self.zookeeper.set('master', address)

            # registers chunkserver with master when ip set on zookeeper
            def watch_ip(event):
                path = event.path
                chunkserver_ip = self.zookeeper.get(path)[0]
                chunkserver_num = path[path.rfind('/')+1:]
                print "New IP %s detected in chunknum %s" % (chunkserver_ip, chunkserver_num)
                self._register_chunkserver(chunkserver_num, chunkserver_ip)

            @self.zookeeper.ChildrenWatch(CHUNKSERVER_PATH)
            def watch_children(children):
                if len(children) > len(self.chunkservers):
                    print "New chunkserver(s) detected"
                    # This creates a watch function for each new chunk server, where the
                    # master waits to register until the data(ip address) is updated
                    new_chunkservers = (chunkserver_num for chunkserver_num in children
                                        if chunkserver_num not in self.chunkservers)
                    for chunkserver_num in new_chunkservers:
                        chunkserver_ip = self.zookeeper.get(CHUNKSERVER_PATH + chunkserver_num)[0]
                        # if IP is not set yet, assign watcher to wait
                        if len(chunkserver_ip) == 0:
                            self.zookeeper.exists(CHUNKSERVER_PATH+chunkserver_num, watch=watch_ip)
                        else:
                            self._register_chunkserver(chunkserver_num, chunkserver_ip)

                # TODO:  Handle deletion, replication checks, metadata update
                elif len(children) < len(self.chunkservers):
                    # call replication checks, etc here
                    print "Chunkserver was removed"
        except:
            print "Unable to connect to zookeeper"
            raise

    def _register_chunkserver(self, chunkserver_num, chunkserver_ip):
        """
        Adds chunkserver IP to chunkserver table and a connected zerorpc client to chunkclients
        :param chunkserver_num:
        :param chunkserver_ip:
        :return:  None
        """
        try:
            c = zerorpc.Client()
            c.connect(chunkserver_ip)
            self.chunkclients[chunkserver_num] = c
            self.chunkservers[chunkserver_num] = chunkserver_ip
            # self.chunkstats[chunkserver_num] = c.get_stats()
            # TODO c.get_metadata() and add to filetable, handle errors without raising
        except:
            print "Error connecting master to chunksrv %s at %s" % (chunkserver_num, chunkserver_ip)
            raise

        print 'Chunksrv #%d registered at %s' % (int(chunkserver_num), chunkserver_ip)
        self.num_chunkservers += 1
        print 'Number of chunkservers = %d' % self.num_chunkservers

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

    def update_stats(self):
        """ Updates storage/network use for each chunkserver if enough time has elapsed """
        new_time = time.time()
        if new_time - self.last_updated > UPDATE_FREQUENCY:
            jobs = []
            for chunkserver_num, chunkclient in self.chunkclients.iteritems():
                thread = threading.Thread(target=self._get_stats(chunkserver_num, chunkclient))
                jobs.append(thread)
                thread.start()

            # block until all threads are done before returning
            for j in jobs:
                j.join()

            self.last_updated = new_time

    def _get_stats(self, chunkserver_num, chunkclient):
        stats = chunkclient.get_stats()

        # average network speed for smoother view
        stats[0] = (stats[0] + self.chunkstats[chunkserver_num]) / 2
        self.chunkstats[chunkserver_num] = stats

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
        print 'Master ack from %d' % chunknum

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
        keys_list = self.chunkservers.keys()
        for i in range(0, num_chunks):
            chunkuuid = str(uuid.uuid1())
            chunkloc = self.chunkrobin
            self.chunktable[chunkuuid] = keys_list[chunkloc]
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
        print "deleted file: " + filename + " renamed to " + deleted_filename + " ready for gc"





