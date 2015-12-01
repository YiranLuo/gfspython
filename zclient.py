import hashlib
import threading
import time

import zerorpc
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.recipe.lock import LockTimeout
from zerorpc.exceptions import LostRemote

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

        if self._exists(filename):
            self.master.updatevrsn(filename, 1)
            self.edit(filename, data)
        else:
            seq = 0
            self.master.updatevrsn(filename, 0)

            start = time.time()

            try:
                lock = self.zookeeper.Lock('files/' + filename)
                lock.acquire(timeout=5)
                num_chunks, chunksize = self._num_chunks(len(data))
                # chunkuuids = self.master.alloc(filename, num_chunks, chunksize, seq)
                # self._write_chunks(chunkuuids, data, chunksize)
                chunkuuids = self.master.alloc2(filename, num_chunks, chunksize, seq)
                if not chunkuuids:
                    print "No chunkservers online"
                    return None
                chunklist = self._write_chunks(chunkuuids, data, chunksize)
                if chunklist:
                    self._update_master(filename, chunklist)
                else:
                    "Failed to write file"
                    return None
                end = time.time()
                print "Total time writing was %0.2f ms" % ((end - start) * 1000)
                print "Transfer rate: %0.f MB/s" % (len(data) / 1024 ** 2 / (end - start))

            except LockTimeout:
                return "File in use - try again later"
            finally:
                lock.release()

    def _update_master(self, filename, chunklist):

        print "File transfer successful. Updating master"
        try:
            self.master.update_file(filename, chunklist)
        except Exception as e:
            print "Error updating master: "

    def _exists(self, filename):
        return self.master.exists(filename)

    def _num_chunks(self, size, chunksize=None):
        if not chunksize:
            chunksize = max(MIN_CHUNK_SIZE, size / TARGET_CHUNKS)
        return (size // chunksize) + (1 if size % chunksize > 0 else 0), chunksize

    def _write_chunks(self, chunkuuids, data, chunksize):
        chunks = [data[x:x + chunksize] for x in range(0, len(data), chunksize)]

        # connect with each chunkserver. TODO Change to check/establish later
        chunkserver_clients = self._establish_connection()
        # print "connection established"
        # raw_input('wait')
        # chunkuuids is already a table
        # write to each chunkserver

        finished = False
        failed_chunkservers = []
        while not finished:
            chunklist = []  # list of successful writes for updating master
            for idx, (chunkuuid, c_locs) in enumerate(chunkuuids):
                chunklocs = [c_loc for c_loc in c_locs
                             if c_loc not in failed_chunkservers]
                if not chunklocs:
                    'No chunkservers to write to, write failed'
                    return False
                else:
                    try:
                        chunkloc = chunklocs[idx % len(chunklocs)]
                        digest = hashlib.md5(chunks[idx]).digest()
                        retdigest = chunkserver_clients[chunkloc].write(chunkuuid, chunks[idx])
                        i = 3  # maximum amount of retries before we exit
                        while digest != retdigest:
                            if i == 0:
                                print "Failed transferring chunk without errors"
                                return False
                            retdigest = chunkserver_clients[chunkloc].write(chunkuuid, chunks[idx])
                            i -= 1
                        chunklist.append((chunkuuid, chunkloc))
                        if idx == len(chunkuuids) - 1:
                            finished = True
                    except LostRemote:
                        failed_chunkservers.append(chunkloc)
                        finished = False
                        break
                    except Exception as e:
                        print 'Failed writing chunk %d to srv %s' % (idx, chunkloc)
                        print e.__doc__
                        print e.message

        return chunklist

    # TODO add argument here so that we only establish necessary connections
    def _establish_connection(self, targets=None):
        """
        Creates zerorpc client for each chunkserver
        :return:  Dictionary of zerorpc clients bound to chunkservers
        """
        chunkserver_clients = {}
        chunkservers = self.master.get('chunkservers')

        for chunkserver_num, chunkserver_ip in chunkservers.iteritems():
            zclient = zerorpc.Client()
            print 'Client connecting to chunkserver %s at %s' % (chunkserver_num, chunkserver_ip)
            try:
                zclient.connect(chunkserver_ip)
                zclient.print_name()
                chunkserver_clients[chunkserver_num] = zclient
            except LostRemote as e:
                self.master._print_exception('Lost remote in client', e)

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
            print "Read error - file does not exist"

        if filename == "#garbage_collection#":
            print self.master.get_chunkuuids(filename)
        else:
            try:
                start = time.time()

                lock = self.zookeeper.Lock('files/' + filename)
                lock.acquire(timeout=5)

                # chunks = []
                jobs = []
                chunkuuids = self.master.get_chunkuuids(filename)
                # print "How many chunks? = %d" % len(chunkuuids)
                chunktable = self.master.get_file_chunks(filename)
                chunkserver_nums = set(num for numlist in chunktable.values() for num in numlist)
                # result = set(x for l in v for x in l)
                chunks = [None] * len(chunkuuids)
                chunkserver_clients = self._establish_connection2(chunkserver_nums)
                for i, chunkuuid in enumerate(chunkuuids):
                    chunkloc = chunktable[chunkuuid][0]  # FIX ME LATER
                    thread = threading.Thread(
                        target=self._read(chunkuuid, chunkserver_clients[chunkloc], chunks, i))
                    jobs.append(thread)
                    thread.start()

                # block until all threads are done before reducing chunks
                for j in jobs:
                    j.join()

                data = ''.join(chunks)
                end = time.time()
                print "Total time reading was %0.2f ms" % ((end - start) * 1000)
                print "Transfer rate: %0.f MB/s" % (len(data) / 1024 ** 2. / (end - start))

            except LockTimeout:
                print "File in use - try again later"
                return None
            except Exception as e:
                print "Error reading file %s" % filename
                print e.__doc__
                print e.message
                return None
            finally:
                lock.release()

            # TODO temporary
            import os
            import webbrowser
            fdir = "/tmp/gfs/files/"
            if not os.access(fdir, os.W_OK):
                os.makedirs(fdir)
            fn = os.path.abspath('/tmp/gfs/files/' + filename)
            f = open(fn, 'wb')
            f.write(data)
            os.fsync(f)  # ensure data is on disk
            f.close()
            webbrowser.open(fn)
            raw_input("Press any key to continue")
            try:
                os.remove(fn)
            except Exception as e:
                print "Error removing tmp file: ", e.__doc__
                print e.message

            # return data
            return "opened in webbrowser - call read_with_details(filename)[0] for the data =P"

    def read_with_details(self, filename):  # get metadata, then read chunks direct
        """
        Connects to each chunkserver and reads the chunks in order, then
        assembles the file by reducing.  Returns details for editing
        :param filename:
        :return:  details, file contents
        """

        if not self._exists(filename):
            raise Exception("read error, file does not exist: " + filename)

        if filename == "#garbage_collection#":
            print self.master.get_chunkuuids(filename)
        else:
            try:
                lock = self.zookeeper.Lock('files/' + filename)
                lock.acquire(timeout=5)

                # chunks = []
                jobs = []
                chunkuuids = self.master.get_chunkuuids(filename)
                chunkdetails = []
                chunktable = self.master.get_file_chunks(filename)
                chunks = [None] * len(chunkuuids)
                chunkserver_clients = self._establish_connection()
                for i, chunkuuid in enumerate(chunkuuids):
                    chunkloc = chunktable[chunkuuid]  # TODO FIX ME LATER, reads from [0] below
                    temp = {'chunkloc': chunkloc,
                            'chunkuid': chunkuuid}
                    chunkdetails.append(temp)

                    thread = threading.Thread(
                        target=self._read(chunkuuid, chunkserver_clients[chunkloc[0]],
                                          chunks, i, temp))
                    jobs.append(thread)
                    thread.start()

                # block until all threads are done before reducing chunks
                for j in jobs:
                    j.join()

                data = ''.join(chunks)

                # print chunkdetails

            except LockTimeout:
                print "File in use - try again later"
                return None
            except:
                print "Error reading file %s" % filename
                raise
            finally:
                lock.release()

            return data, chunkdetails

    @staticmethod
    def _read(chunkuuid, chunkserver_client, chunks, i, temp=None):
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
        # update temp with chunk for edit details function if exists
        if temp:
            temp['chunk'] = chunk
            # print "Finished reading chunk %s " % chunkuuid

    def dump_metadata(self):
        self.master.dump_metadata()

    # TODO change for variable chunksize
    # def append(self, filename, data):
    #     if not self._exists(filename):
    #         raise Exception("append error, file does not exist: " + filename)
    #     num_chunks = self._num_chunks(len(data))
    #     append_chunkuuids = self.master.alloc_append(filename, num_chunks)
    #     self._write_chunks(append_chunkuuids, data, 1024)  # change 1024

    ####################################################################################

    def append(self, filename, data):
        try:
            lock = self.zookeeper.Lock('files/' + filename)
            lock.acquire(timeout=5)
            self._edit_append(filename, data)
        except LockTimeout:
            print "File in use - try again later"
            return None
        except:
            print "Error reading file %s" % filename
            raise
        finally:
            lock.release()

    def _edit_append(self, filename, data):
        """ Separate function, called if you already have a lock acquired for appending"""
        if not self._exists(filename):
            print "Can't append, file '%s' does not exist" % filename
            return None
        else:
            chunksize = self.master.get_chunksize(filename)
            last_chunk_id = self.master.get_last_chunkuuid(filename)
            num_chunks, _ = self._num_chunks(len(data), chunksize)
            seq = int(last_chunk_id.split('$%#')[1]) + 1
            append_chunkuuids = self.master.alloc2_chunks(num_chunks, filename, seq)
            # print "append_chuids", append_chunkuuids
            if not append_chunkuuids:
                print "No chunkservers online"
                return None
            chunklist = self._write_chunks(append_chunkuuids, data, chunksize)
            # print "chunklist = %s" % chunklist
            if chunklist:
                self._update_master(filename, chunklist)
            else:
                "Failed to write file"
                return None

    def deletechunk(self, filename, chunkdetails, len_newdata, len_olddata, chunksize):
        x = y = 0
        chunkids = []
        chunkserver_clients = self._establish_connection()  # can reuse the connection thats already established
        for chunkuuid in chunkdetails:
            if x > len_newdata:
                chunkids.append(chunkuuid['chunkuid'])
            x += chunksize
        # print "@ client",filename,chunkids
        self.master.delete_chunks(filename, chunkids)
        return 'True'

    def replacechunk(self, chunkdetails, data1, data2, chunksize):
        x = y = 0
        chunkserver_clients = self._establish_connection()  # can be avoided, pass from the edit function
        for x in range(0, len(data1), chunksize):
            if data1[x:x + chunksize] != data2[x:x + chunksize] or len(
                    data2[x:x + chunksize]) < chunksize:
                print "replace '" + data1[x:x + chunksize] + "' with '" + data2[
                                                                          x:x + chunksize] + "'"
                for i in chunkdetails[y]['chunkloc']:
                    chunkserver_clients[i].write(chunkdetails[y]['chunkuid'],
                                                 data2[x:x + chunksize])

            y += 1
        return 'True'

    # def append(self, filename, data):
    #     if not self._exists(filename):
    #         raise Exception("append error, file does not exist: " + filename)
    #     else:
    #         num_chunks = self._num_chunks(len(data))
    #         chunkuuids = self.master.get_chunkuuids(filename)[-1]
    #         seq = int(chunkuuids.split('$%#')[1]) + 1
    #         append_chunkuuids = self.master.alloc_append(num_chunks, filename, seq)
    #         self._write_chunks(append_chunkuuids, data)

    def delete(self, filename):
        if not self._exists(filename):
            raise Exception("append error, file does not exist: " + filename)
        else:
            self.master.delete(filename, "")

    def edit(self, filename, newdata):
        """
        Read the file with the read() from above and update only the
        chunkservers where the data in the chunk has changed
        """

        if not self._exists(filename):
            raise Exception("read error, file does not exist: " + filename)

        # TODO possibly change, read acquires full lock so this can't happen after asking lock below
        # kazoo lock is not reentrant, so it will block forever if the same thread acquires twice
        olddata, chunkdetails = self.read_with_details(filename)

        if not olddata:
            return False  # exit if unable to read details

        try:
            lock = self.zookeeper.Lock('files/' + filename)
            lock.acquire(timeout=5)
            chunks = []
            i = 0
            chunkuuids = self.master.get_chunkuuids(filename)
            # chunkserver_clients = self._establish_connection()
            #
            # for chunkuuid in chunkuuids:
            #   temp={}
            # maybe use subprocess to execute the download process in parallel
            # may throw error if chunkserver dies off in between
            #   chunkloc = self.master.get_chunkloc(chunkuuid)
            #   chunk = chunkserver_clients[chunkloc[0]].read(chunkuuid)
            #   temp['chunkloc']=chunkloc
            #   temp['chunkuid']=chunkuuid
            #   temp['chunk']=chunk
            #   chunkdetails.append(temp)
            #   chunks.append(chunk)

            # olddata = reduce(lambda x, y: x + y, chunks)  # reassemble in order

            # print "\nCurrent data in " + filename + "\n" + olddata + "\nEdited data:\n" + newdata

            # newchunks = []
            # chunksize = self.master.get('chunksize')
            chunksize = self.master.get_chunksize(filename)
            len_newdata = len(newdata)
            len_olddata = len(olddata)
            # newchunks = [newdata[x:x + chunksize] for x in range(0, len_newdata, chunksize)]

            if len_newdata == len_olddata:
                if newdata == olddata:
                    print "no change in contents"
                else:
                    # print "same size but content changed"
                    x = self.replacechunk(chunkdetails, olddata, newdata, chunksize)
            elif len_newdata < len_olddata:
                # print "deleted some contents"
                x = self.replacechunk(chunkdetails, olddata[0:len_newdata], newdata, chunksize)
                # print "call fn() to delete chunks " + olddata[len_newdata + 1:] + " from chunk server"
                x = self.deletechunk(filename, chunkdetails, len_newdata, len_olddata, chunksize)
            elif len_newdata > len_olddata:
                # print "added some contents"
                x = self.replacechunk(chunkdetails, olddata, newdata[0:len_olddata], chunksize)
                # print "call fn() to add chunks '" + newdata[len_olddata + 1:] + "' to chunk server"
                self._edit_append(filename, newdata[len_olddata:])

            self.master.updatevrsn(filename, 1)

        except LockTimeout:
            print "File in use - try again later"
            return None
        except Exception as e:
            print "Error editing file %s - try again later" % filename
            print e.__doc__
            print e.message
        finally:
            lock.release()

    def rename(self, filename, newfilename):
        if self._exists(filename):
            if not self._exists(newfilename):
                chunkuids = self.master.get_chunkuuids(filename)
                result = {}
                for chunkuid in chunkuids:
                    # maybe use subprocess to execute the download process in parallel
                    # may throw error if chunkserver dies off in between
                    chunklocs = self.master.get_chunkloc(chunkuid)
                    for chunkloc in chunklocs:
                        try:
                            result[chunkloc].append(chunkuid)
                        except:
                            result[chunkloc] = []
                            result[chunkloc].append(chunkuid)

                    self.master.rename(result, filename, newfilename)

            else:
                print "read error, file already exist: " + newfilename
        else:
            print "read error, file does not exist: " + filename
