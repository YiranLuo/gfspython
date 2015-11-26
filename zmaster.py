import uuid
import time
import threading
import ast

from kazoo.client import KazooClient
import zerorpc
from apscheduler.schedulers.background import BackgroundScheduler

import zutils

CHUNKSERVER_PATH = 'chunkserver/'
UPDATE_FREQUENCY = 5  # update frequency in seconds


class ZMaster:
    def __init__(self, zoo_ip='localhost:2181', master_port=1400):
        self.num_chunkservers = 0
        self.last_updated = 0  # time since last stats poll
        self.ip = zutils.get_myip() + ':' + str(master_port)
        # self.chunksize = 10
        self.chunkrobin = 0
        self.versntable = {}  # file version counter
        self.filetable = {}  # file to chunk mapping
        self.chunktable = {}  # chunkuuid to chunkloc mapping
        self.filetable = {}  # file to chunk mapping
        self.chunktable = {}  # chunkuuid to chunkloc mapping
        self.chunkservers = {}  # loc id to chunkserver mapping
        self.chunkclients = {}
        # self.init_chunkservers()
        self.chunkclients = {}  # zerorpc clients connected to chunkservers
        self.chunkstats = {}  # stats for capacity and network load
        self.chunksize = {}  # filename -> chunksize mapping
        self.zookeeper = KazooClient(hosts=zoo_ip)
        self._register_with_zookeeper(master_port)

        # this schedules background tasks in separate thread
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.collect_garbage, 'interval', minutes=1)
        scheduler.add_job(self.replicate, 'interval', minutes=1)
        scheduler.start()

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
                chunkserver_num = path[path.rfind('/') + 1:]
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
                            self.zookeeper.exists(CHUNKSERVER_PATH + chunkserver_num,
                                                  watch=watch_ip)
                        else:
                            self._register_chunkserver(chunkserver_num, chunkserver_ip)

                # TODO:  Handle deletion, replication checks, metadata update
                elif len(children) < len(self.chunkservers):
                    # call replication checks, etc here
                    print "Chunkserver was removed"
                    self.num_chunkservers -= 1
                    print "now ", self.num_chunkservers, " chunkservers"
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

    def get_chunksize(self, filename):
        return self.chunksize[filename]

    def list(self):
        """ Returns a list of files that do not start with /hidden/deleted (marked
        for deletion """
        return [key for key in self.filetable.keys()
                if not key.startswith('#garbage_collection#')]

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

    # TODO temporary
    def see_chunkloc(self):
        return self.chunktable

    def get_chunkuuids(self, filename):
        return self.filetable[filename]

    def alloc(self, filename, num_chunks, chunksize, seq):  # return ordered chunk list
        chunks = self.alloc_chunks(num_chunks, filename, seq)
        self.filetable[filename] = chunks
        self.chunksize[filename] = chunksize
        return chunks

    ###############################################################################

    def updatevrsn(self, filename, flag):
        if flag == 1:
            self.versntable[filename] += 1
        else:
            self.versntable[filename] = 0
        print filename, self.versntable[filename]

    def _establish_connection(self, chunkloc):
        """
        Creates zerorpc client for each chunkserver
        :return:  Dictionary of zerorpc clients bound to chunkservers
        """
        # chunkserver_client = {}
        chunkservers = self.get('chunkservers')

        if True:
            zclient = zerorpc.Client()
            print 'Server connecting to chunkserver at %s' % chunkservers[chunkloc]
            zclient.connect(chunkservers[chunkloc])
            zclient.print_name()
            # chunkserver_client[chunkloc] = zclient
            return zclient
            # chunkserver_client

    def collect_garbage(self):

        try:
            chunklocs = self.filetable["#garbage_collection#"]
        except:
            self.filetable["#garbage_collection#"] = {}
            chunklocs = {}

        if chunklocs:
            print "in garbage"
            for chunkloc in chunklocs.keys():
                # connect with each chunkserver. Change to check/establish later
                chunkserver_clients = self._establish_connection(chunkloc)

                # print "call delchunkfile fn() in chunkserver-"+str(chunkloc)
                flag = chunkserver_clients.delete(chunklocs[chunkloc])
                if flag == True:
                    # print "remove value from garbage collection for "+str(chunkloc)
                    del self.filetable["#garbage_collection#"][chunkloc]
        else:
            print "nothing to clear in garbage"

    def replicate(self):
        replicas = {}
        no_servers = self.num_chunkservers
        reps = min(3, no_servers)
        # self.chunktable={'test.txt$%#0$%#17229618-8c09-11e5-8017-000c29c12a87': [0], 'test.txt$%#1$%#17229619-8c09-11e5-8017-000c29c12a87': [1]}

        chunktable = self.chunktable
        chunkserver = {}
        values = []
        for chunkid, values in chunktable.items():
            temp = str(values)
            values = ast.literal_eval(temp)
            keys_list = self.chunkservers.keys()
            while len(values) < reps:
                self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers
                chunkloc = keys_list[self.chunkrobin]
                while chunkloc in values:
                    self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers
                    chunkloc = keys_list[self.chunkrobin]

                # print "call connection to "+str(chunkloc)+" pass ",chunkid,temp
                if not chunkloc in chunkserver:
                    try:
                        chunkserver[chunkloc] = self._establish_connection(chunkloc)
                    except:
                        None

                if chunkserver[chunkloc].copy_chunk(chunkid, temp):
                    print "Update chunktable"
                    self.chunktable[chunkid].append(chunkloc)

                result = {}
                result[chunkid] = temp
                try:
                    replicas[chunkloc].append(result)
                except:
                    replicas[chunkloc] = []
                    replicas[chunkloc].append(result)

                values.append(chunkloc)

        if len(replicas) == 0:
            print "Nothing to do in replicate"
        else:
            print "replica: ", replicas

    def delete(self, filename, chunkuuids):  # rename for later garbage collection
        if chunkuuids == "":
            chunkuuids = self.filetable[filename]
            del self.filetable[filename]

        deleted_filename = "#garbage_collection#"

        try:
            if self.filetable[deleted_filename]:
                None
        except:
            self.filetable[deleted_filename] = {}
            # self.filetable[deleted_filename]=[]

        for chunkid in chunkuuids:
            chunklocs = self.get_chunkloc(chunkid)
            for chunkloc in chunklocs:
                try:
                    self.filetable[deleted_filename][chunkloc].append(chunkid)
                except:
                    self.filetable[deleted_filename][chunkloc] = []
                    self.filetable[deleted_filename][chunkloc].append(chunkid)
            del self.chunktable[chunkid]

        print self.filetable[deleted_filename]
        # self.collect_garbage()

    def delete_chunks(self, filename, chunk_rm_ids):
        print filename, chunk_rm_ids
        chunkuuids = self.filetable[filename]
        self.filetable[filename] = [x for x in chunkuuids if x not in chunk_rm_ids]
        self.delete(filename, chunk_rm_ids)

    def alloc_chunks(self, num_chunks, filename, seq):
        chunkuuids = []
        tseq = seq
        keys_list = self.chunkservers.keys()
        for i in range(0, num_chunks):
            chunkuuid = filename + "$%#" + str(tseq) + "$%#" + str(uuid.uuid1())
            chunkloc = self.chunkrobin
            self.chunktable[chunkuuid] = [keys_list[chunkloc]]
            chunkuuids.append(chunkuuid)
            self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers
            tseq += 1
        return chunkuuids

    def alloc_append(self, num_append_chunks, filename, seq):  # append chunks
        chunkuuids = self.filetable[filename]
        tseq = seq
        append_chunkuuids = self.alloc_chunks(num_append_chunks, filename, tseq)
        chunkuuids.extend(append_chunkuuids)
        return append_chunkuuids

    def rename(self, result, filename, newfilename):
        chunkserver = {}
        chunkids = []
        flag = True
        for chunkloc, chunkids in result.items():
            try:
                chunkserver[chunkloc] = self._establish_connection(chunkloc)
            except:
                print "Couldnt connect to chunkserver ", chunkloc

            flag = True
            no_keys = len(chunkids)
            flag = flag and chunkserver[chunkloc].rename(chunkids, filename, newfilename)

        if flag:
            for chunkid in self.filetable[filename]:
                temp = str(chunkid).replace(filename, newfilename)
                self.chunktable[temp] = self.chunktable.pop(chunkid)
            self.filetable[newfilename] = ast.literal_eval(
                str(self.filetable.pop(filename)).replace(filename, newfilename))
            self.versntable[newfilename] = self.versntable.pop(filename)
        else:
            print "Some error occurred while renaming"

    def dump_metadata(self):
        print "Filetable:",
        for filename, chunkuuids in self.filetable.items():
            print filename, "with", len(chunkuuids), "chunks"
        print "Chunkservers: ", len(self.chunkservers)
        # TODO fix or remove
        # print "Chunkserver Data:"
        # for chunkuuid, chunkloc in sorted(self.chunktable.iteritems(),
        #                                  key=operator.itemgetter(1)):
        #    chunk = self.chunkclients[chunkloc].read(chunkuuid)
        #    print chunkloc, chunkuuid, chunk

    def rm_from_ctable(self, chunkloc):
        for values in self.chunktable.itervalues():
            values.remove(chunkloc)

    def sort_filetable(self, filename):
        temp = []
        temptable = {}

        for fileid in self.filetable[filename]:
            temptable[fileid.split("$%#")[1]] = fileid

        keys = sorted(temptable.keys())
        for key in keys:
            temp.append(temptable[key])
        self.filetable[filename] = temp

    def populate(self, files, chunkloc):
        chunkloc = int(chunkloc)
        for filename, chunkids in files.items():
            if self.exists(filename):
                print "operations for merging"
                for chunkid in chunkids:
                    if chunkid not in self.filetable[filename]:
                        self.filetable[filename].append(chunkid)
                        self.chunktable[chunkid] = [chunkloc]
                    else:
                        self.chunktable[chunkid].append(chunkloc)
            else:
                print "operation for adding", filename
                self.filetable[filename] = chunkids
                temp = {}
                for chunkid in chunkids:
                    temp[chunkid] = [chunkloc]
                self.chunktable.update(temp)
                self.versntable[filename] = 1
                # update chunksize table
            self.sort_filetable(filename)

            # print self.filetable
            # print self.chunktable
