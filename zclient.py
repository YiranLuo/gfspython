import time
import threading

import zerorpc
from kazoo.client import KazooClient
from kazoo.recipe.lock import LockTimeout
from kazoo.exceptions import NoNodeError

TARGET_CHUNKS = 15
MIN_CHUNK_SIZE = 1024


class ZClient:
    def __init__(self, zoo_ip='localhost:2181', port=1400):
        self.master = zerorpc.Client()
        self.zookeeper = KazooClient(hosts=zoo_ip)

        # connect to zookeeper for master ip, then connect to master
        master_ip = self._connect_to_zookeeper()
        self._connect_to_master(master_ip)

    def _connect_to_master(self, master_ip):
        try:
            print 'Connecting to master at %s' % master_ip
            self.master.connect(master_ip)
        except:
            print "Error connecting client to master"
            raise

    def _connect_to_zookeeper(self):
        try:
            self.zookeeper.start()
            master_ip = self.zookeeper.get('master')[0]
        except NoNodeError:
            print "No master record in zookeeper"
            raise  # TODO handle shadow master/waiting for master to reconnect later
        except:
            print "\n\tSome other error happened:"
            raise

        return master_ip

    def close(self):
        """Closes connection with master"""
        self.master.close()

    def write(self, filename, data):
        """
        Creates a new file, writes the data
        :param filename:
        :param data:
        """
        # to be implemented
        '''if self._exists(filename):
            self.delete(filename)'''

        start = time.time()

        # TODO retry here
        try:
            lock = self.zookeeper.Lock('files/' + filename)
            lock.acquire(timeout=5)
            num_chunks, chunksize = self._num_chunks(len(data))
            chunkuuids = self.master.alloc(filename, num_chunks, chunksize)
            self._write_chunks(chunkuuids, data, chunksize)

            end = time.time()
            print "Total time writing was %0.2f" % ((end - start) * 1000)
            lock.release()
        except LockTimeout:
            return "File in use - try again later"


    def _exists(self, filename):
        return self.master.exists(filename)

    def _num_chunks(self, size):
        chunksize = max(MIN_CHUNK_SIZE, size / TARGET_CHUNKS)
        # chunksize = self.master.get('chunksize')
        return (size // chunksize) + (1 if size % chunksize > 0 else 0), chunksize

    def _write_chunks(self, chunkuuids, data, chunksize):
        # chunksize = self.master.get('chunksize')
        chunks = [data[x:x+chunksize] for x in range(0, len(data), chunksize)]

        # connect with each chunkserver. TODO Change to check/establish later
        chunkserver_clients = self._establish_connection()

        # TODO get partial table
        chunktable = self.master.get('chunktable')
        # write to each chunkserver
        for idx, chunkuuid in enumerate(chunkuuids):
            chunkloc = chunktable[chunkuuid]
            chunkserver_clients[chunkloc].write(chunkuuid, chunks[idx])

    # TODO add argument here so that we only establish necessary connections
    def _establish_connection(self):
        """
        Creates zerorpc client for each chunkserver
        :return:  Dictionary of zerorpc clients bound to chunkservers
        """
        chunkserver_clients = {}
        chunkservers = self.master.get('chunkservers')

        for chunkserver_num, chunkserver_ip in chunkservers.iteritems():
            zclient = zerorpc.Client()
            print 'Client connecting to chunkserver %s at %s' % (chunkserver_num, chunkserver_ip)
            zclient.connect(chunkserver_ip)
            zclient.print_name()
            chunkserver_clients[chunkserver_num] = zclient

        return chunkserver_clients

    def list(self):
        filelist = self.master.list()
        if filelist:
            for filename in filelist:
                print filename
        else:
            print 'No files in the system.'

    # TODO Only establish connection to necessary servers
    def read(self, filename):  # get metadata, then read chunks direct
        """
        Connects to each chunkserver and reads the chunks in order, then
        assembles the file by reducing
        :param filename:
        :return:  file contents
        """

        if not self._exists(filename):
            raise Exception("read error, file does not exist: " + filename)

        # TODO retry later
        try:
            lock = self.zookeeper.Lock('files/' + filename)
            lock.acquire(timeout=5)

            # chunks = []
            jobs = []
            chunkuuids = self.master.get_chunkuuids(filename)

            # TODO get partial table
            chunktable = self.master.get('chunktable')
            chunks = [None] * len(chunkuuids)
            chunkserver_clients = self._establish_connection()
            for i, chunkuuid in enumerate(chunkuuids):
                print "reading " + str(i) + " " + str(chunkuuid)
                chunkloc = chunktable[chunkuuid]
                thread = threading.Thread(
                    target=self._read(chunkuuid, chunkserver_clients[chunkloc], chunks, i))
                jobs.append(thread)
                thread.start()

            # block until all threads are done before reducing chunks
            for j in jobs:
                j.join()

            data = reduce(lambda x, y: x + y, chunks)  # reassemble in order
            lock.release()

        except LockTimeout:
            return "File in use - try again later"
        except:
            print "Error reading file %s" % filename
            raise

        return data

    @staticmethod
    def _read(chunkuuid, chunkserver_client, chunks, i):
        """
        Gets appropriate chunkserver to contact from master, and retrieves the chunk with
        chunkuuid. This function is passed to a threading service. Thread safe since each thread
        accesses only one index.
        :param chunkuuid:
        :param chunkserver_client: chunkserver to retrieve chunk from
        :param chunks: list of chunks we will append this chunk to
        :param i: current index we are working on
        :return: Calling threads in this fashion cannot return values, so we pass in chunks
        """
        chunk = chunkserver_client.read(chunkuuid)
        chunks[i] = chunk
        print "Finished with chunk %d with len %d" % (i, len(chunk))

    def dump_metadata(self):
        self.master.dump_metadata()

    # TODO change for variable chunksize
    def append(self, filename, data):
        if not self._exists(filename):
            raise Exception("append error, file does not exist: " + filename)
        num_chunks = self._num_chunks(len(data))
        append_chunkuuids = self.master.alloc_append(filename, num_chunks)
        self._write_chunks(append_chunkuuids, data, 1024)  # change 1024

    def delete(self, filename):
        self.master.delete(filename)
