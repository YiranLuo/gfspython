""" ZClient interfaces with Zookeeper and the Master server to coordinate with the chunkservers.  Implements the
create, open, close, read, and write operations on files contained by chunkservers.
operations

"""
import logging
import multiprocessing as mp
import random
import threading
import time
from collections import defaultdict

import xxhash
import zerorpc
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.recipe.lock import LockTimeout
from zerorpc.exceptions import LostRemote

TARGET_CHUNKS = 10
MIN_CHUNK_SIZE = 1024000


# TODO implement custom exceptions for typical crud filesystem behavior

class ZClient:
    """ Zclient class """

    def __init__(self, zoo_ip='localhost:2181'):
        logging.basicConfig(filename='log.txt')
        self.logger = logging.getLogger(__name__)
        self.master = zerorpc.Client()
        self.zookeeper = KazooClient(hosts=zoo_ip)

        # connect to zookeeper for master ip, then connect to master
        master_ip = self._connect_to_zookeeper()
        self._connect_to_master(master_ip)

    def _connect_to_master(self, master_ip):
        """
        Creates a `zerorpc.Client` connection at the given `master_ip`
        :param master_ip: ip given for master server
        """
        try:
            self.master.connect(master_ip)
        except Exception:
            self.logger.exception('Error connecting client to master')
            raise
        else:
            self.logger.info(f'Connected to master at {master_ip}')

    def _connect_to_zookeeper(self):
        try:
            self.zookeeper.start()
            master_ip = self.zookeeper.get('master')[0].split('@')[-1]
        except NoNodeError:
            self.logger.exception('No master record in zookeeper')
            raise  # TODO handle shadow master/waiting for master to reconnect later
        except Exception:
            self.logger.exception('Unable to connect to Zookeeper')
            raise

        return master_ip

    def load(self, filename):
        """ Loads a file and writes it to the file system """
        with open(filename, 'rb') as f:
            data = f.read()

        self.write(filename, data)

    def close(self):
        """ Closes connection with master"""
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
            lock = self.zookeeper.Lock('files/' + filename)

            try:
                lock.acquire(timeout=5)
            except LockTimeout:
                return 'File in use - try again later'
            else:
                num_chunks, chunksize = self._num_chunks(len(data))
                # chunkuuids = self.master.alloc(filename, num_chunks, chunksize, seq)
                # self._write_chunks(chunkuuids, data, chunksize)
                chunkuuids = self.master.alloc2(filename, num_chunks, chunksize, seq)
                if not chunkuuids:
                    self.logger.warning('No chunkservers online')
                chunklist = self._write_chunks(chunkuuids, data, chunksize)
                if chunklist:
                    self._update_master(filename, chunklist)
                else:
                    self.logger.info('Failed to write file')
                end = time.time()
                self.logger.info(f'Total time writing was {((end - start) * 1000):%0.2f} ms')
                self.logger.info(f'Transfer rate: {(len(data) / 1024 ** 2. / (end - start))} MB/s')
            finally:
                lock.release()

    def _update_master(self, filename, chunklist):

        self.logger.info('File transfer successful. Updating master')
        try:
            self.master.update_file(filename, chunklist)
        except Exception:
            self.logger.exception(f'Error updating master')

    def _exists(self, filename):
        """ Waits for a response from the master server """
        response = None
        while response is None:
            response = self.master.exists(filename)

        return response

    @staticmethod
    def _num_chunks(size, chunksize=None):
        if not chunksize:
            chunksize = max(MIN_CHUNK_SIZE, size / TARGET_CHUNKS)
        return (size // chunksize) + (1 if size % chunksize > 0 else 0), chunksize

    def _write_chunks(self, chunkuuids, data, chunksize):
        chunks = [data[x:x + chunksize] for x in range(0, len(data), chunksize)]

        # connect with each chunkserver. TODO Change to check/establish later
        # chunkserver_nums = set(num for numlist in chunkuuids.values() for num in numlist)
        chunkserver_clients = self._establish_connection()
        # print 'connection established'
        # raw_input('wait')
        # chunkuuids is already a table
        # write to each chunkserver

        finished = False
        call_replicate = False
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
                        if len(chunklocs) > 1:
                            chunkloc, chunkloc2 = random.sample(chunklocs, 2)
                        else:
                            chunkloc = random.sample(chunklocs, 1)[0]
                            chunkloc2 = None

                        # print 'chunklocs = %s, chunkloc1 = %s, chunkloc2=%s' % (
                        #    chunklocs, chunkloc, chunkloc2)
                        digest = xxhash.xxh64(chunks[idx]).digest()
                        retdigest = chunkserver_clients[chunkloc].write(chunkuuid, chunks[idx],
                                                                        chunkloc2)
                        i = 3  # maximum amount of retries before we exit
                        while digest != retdigest:
                            if i == 0:
                                print()
                                'Failed transferring chunk without errors'
                                return False
                            retdigest = chunkserver_clients[chunkloc].write(chunkuuid, chunks[idx],
                                                                            chunkloc2)
                            i -= 1

                        if chunkloc2:
                            chunklist.append((chunkuuid, [chunkloc, chunkloc2]))
                        else:
                            chunklist.append((chunkuuid, [chunkloc]))

                        if idx == len(chunkuuids) - 1:
                            finished = True
                    except LostRemote:
                        failed_chunkservers.append(chunkloc)
                        finished = False
                        call_replicate = True
                        break
                    except Exception:
                        self.logger.exception(f'Failed writing chunk {idx} to srv #{chunkloc}')
                        raise

        # print call_replicate, finished
        if call_replicate and finished:
            self.master.replicate()

        for client in list(chunkserver_clients.values()):
            client.close()

        return chunklist

    # TODO only establish necessary target connections here
    def _establish_connection(self, targets=None):
        """
        Creates zerorpc client for each chunkserver
        :return:  Dictionary of zerorpc clients bound to chunkservers
        """
        chunkserver_clients = {}
        chunkservers = self.master.get('chunkservers')

        for chunkserver_num, chunkserver_ip in list(chunkservers.items()):
            zclient = zerorpc.Client()
            # print 'Client connecting to chunkserver %s at %s' % (chunkserver_num, chunkserver_ip)
            try:
                zclient.connect(chunkserver_ip)
                # zclient.print_name()
                chunkserver_clients[chunkserver_num] = zclient
            except LostRemote:
                self.logger.critical('LostRemote exception')

        return chunkserver_clients

    def list(self):
        """ List all files in the file system """
        filelist = self.master.list() or ('No files in the system',)
        for filename in filelist:
            print(filename)

    def read(self, filename):  # get metadata, then read chunks directly
        """
        Connects to each chunkserver and reads the chunks in order, then
        assembles the file by reducing
        :param filename:
        :return: file contents or None if read was not successful
        """

        if not self._exists(filename):
            self.logger.warning(f'{filename} does not exist')

        if filename == '#garbage_collection#':
            self.logger.debug(self.master.get_chunkuuids(filename))
        else:
            try:
                start = time.time()

                # lock = self.zookeeper.Lock('files/' + filename)
                # lock.acquire(timeout=5)

                chunkuuids = self.master.get_chunkuuids(filename)
                # print 'How many chunks? = %d' % len(chunkuuids)
                chunktable = self.master.get_file_chunks(filename)
                chunkserver_nums = set(num for numlist in list(chunktable.values()) for num in numlist)
                # result = set(x for l in v for x in l)
                chunks = [''] * len(chunkuuids)
                chunkserver_clients = self._establish_connection(chunkserver_nums)
                jobs = []
                failed_chunkservers = []
                for i, chunkuuid in enumerate(chunkuuids):
                    finished = False
                    id_ = 0
                    while not finished:
                        try:
                            chunklocs = [c_loc for c_loc in chunktable[chunkuuid] if c_loc not in failed_chunkservers]
                            lenchunkloc = len(chunklocs)
                            self.logger.info('chunklocs = {chunklocs}')
                            if chunklocs:
                                next_chunkloc = random.sample(chunklocs, 1)[0]
                            else:
                                self.logger.error('Failed reading file - no chunkservers')
                                return
                            self.logger.info(f'next chunkloc is {next_chunkloc}')

                            thread = threading.Thread(
                                target=self._read(chunkuuid, chunkserver_clients[next_chunkloc],
                                                  chunks, i))
                            jobs.append(thread)
                            thread.start()
                            finished = True
                        except Exception:
                            self.logger.exception(f'Failed to connect to loc {id_}')
                            failed_chunkservers.append(next_chunkloc)
                            finished = False
                            id_ += 1
                            if id_ >= lenchunkloc:
                                self.logger.error('Failed reading file {filename}')
                                return None
                # TODO resolve type checker issues when possible by converting to properties
                data = ''.join(chunks)
                end = time.time()
                self.logger.info('Total time reading was %0.2f ms' % ((end - start) * 1000))
                self.logger.info('Transfer rate: %0.f MB/s' % (len(data) / 1024 ** 2. / (end - start)))

            except LockTimeout:
                self.logger.warning('File in use - try again later')
                return None
            except Exception:
                self.logger.exception(f'Error reading file {filename}')
                return None
            finally:
                # lock.release()
                for client in list(chunkserver_clients.values()):
                    client.close()
                pass

        return data

    def read_mp(self, filename):
        """
        Connects to each chunkserver and reads the chunks in order, then
        assembles the file by reducing.  Returns details for editing
        :param filename:
        :return:  details, file contents
        """

        if not self._exists(filename):
            raise Exception('read error, file does not exist: ' + filename)

        if filename == '#garbage_collection#':
            print()
            self.master.get_chunkuuids(filename)
        else:
            lock = self.zookeeper.Lock('files/' + filename)
            lock.acquire(timeout=5)
            try:
                # chunks = []
                jobs = []
                chunkuuids = self.master.get_chunkuuids(filename)
                chunkdetails = []
                chunktable = self.master.get_file_chunks(filename)
                chunks = [''] * len(chunkuuids)
                chunkserver_clients = self._establish_connection()
                failed_chunkservers = []
                eval(input('Enter'))
                for i, chunkuuid in enumerate(chunkuuids):
                    chunkloc = chunktable[chunkuuid]  # TODO FIX ME LATER, reads from [0] below

                    flag = False
                    id_ = 0
                    lenchunkloc = len(chunkloc)
                    pool = mp.Pool(processes=4)
                    while flag is not True and id_ <= lenchunkloc:
                        # print 'id_=', id_
                        try:

                            # thread = threading.Thread(
                            #     target=self._read(chunkuuid, chunkserver_clients[chunkloc[id_]],
                            #                       chunks, i))
                            result = pool.apply_async(self._read, args=(chunkuuid,
                                                                        chunkserver_clients[
                                                                            chunkloc[id_]], chunks,
                                                                        i))
                            jobs.append(result)
                            flag = True
                        except Exception:
                            self.logger.exception(f'Failed to connect to loc {id_:d}')
                            failed_chunkservers.append(chunkloc[id_])
                            flag = False
                            id_ += 1
                            if id_ >= lenchunkloc:
                                print(f'Error reading file {filename}')
                                return None

                # block until all threads are done before reducing chunks
                for j in jobs:
                    j.wait()

                data = ''.join(chunks)

                # print chunkdetails

            except LockTimeout:
                self.logger.info('File in use - try again later')
                return None
            except Exception:
                self.logger.exception(f'Error reading file {filename}')
                raise
            finally:
                lock.release()

            return data, chunkdetails, chunkserver_clients, failed_chunkservers

    def read_with_details(self, filename, failed_chunkservers):  # get metadata, then read chunks direct
        """
        Connects to each chunkserver and reads the chunks in order, then
        assembles the file by reducing.  Returns details for editing
        :param filename:
        :param failed_chunkservers
        :return:  details, file contents
        """

        if not self._exists(filename):
            raise Exception('read error, file does not exist: ' + filename)

        if filename == '#garbage_collection#':
            print()
            self.master.get_chunkuuids(filename)
        else:
            try:
                lock = self.zookeeper.Lock('files/' + filename)
                lock.acquire(timeout=5)

                # chunks = []
                jobs = []
                chunkuuids = self.master.get_chunkuuids(filename)
                chunkdetails = []
                chunktable = self.master.get_file_chunks(filename)
                chunks = [''] * len(chunkuuids)
                chunkserver_clients = self._establish_connection()
                # raw_input('Enter')
                for i, chunkuuid in enumerate(chunkuuids):
                    chunkloc = chunktable[chunkuuid]  # TODO FIX ME LATER, reads from [0] below
                    temp = {'chunkloc': chunkloc,
                            'chunkuid': chunkuuid}
                    chunkdetails.append(temp)

                    flag = False
                    id_ = 0
                    lenchunkloc = len(chunkloc)
                    while flag is not True and id_ <= lenchunkloc:
                        # print 'id_=', id_
                        try:
                            thread = threading.Thread(
                                target=self._read(chunkuuid, chunkserver_clients[chunkloc[id_]],
                                                  chunks, i, temp))
                            jobs.append(thread)
                            thread.start()
                            flag = True
                        except Exception:
                            self.logger.exception(f'Failed to connect to loc {id_:d}')
                            failed_chunkservers.append(chunkloc[id_])
                            flag = False
                            id_ += 1
                            if id_ >= lenchunkloc:
                                self.logger.exception(f'Error reading file {filename}')
                                return None

                # block until all threads are done before reducing chunks
                for j in jobs:
                    j.join()

                data = ''.join(chunks)

                # print chunkdetails

            except LockTimeout:
                self.logger.info('File in use - try again later')
                return None
            except Exception:
                self.logger.exception(f'Error reading file {filename}')
                raise
            finally:
                lock.release()
                for client in list(chunkserver_clients.values()):
                    client.close()

            return data, chunkdetails, chunkserver_clients, failed_chunkservers

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
        # add md5 check

        chunk = chunkserver_client.read(chunkuuid)
        chunks[i] = chunk
        # update temp with chunk for edit details function if exists
        if temp:
            temp['chunk'] = chunk
            # print 'Finished reading chunk %s ' % chunkuuid

    def dump_metadata(self):
        """ Dump all metadata contained in master for debugging purposes """
        self.master.dump_metadata()

    # TODO change for variable chunksize
    # def append(self, filename, data):
    #     if not self._exists(filename):
    #         raise Exception('append error, file does not exist: ' + filename)
    #     num_chunks = self._num_chunks(len(data))
    #     append_chunkuuids = self.master.alloc_append(filename, num_chunks)
    #     self._write_chunks(append_chunkuuids, data, 1024)  # change 1024

    ####################################################################################

    def append(self, filename, data):
        """
        Attempt to lock `filename` for appending
        :param filename:
        :param data:
        :return:
        """
        lock = self.zookeeper.Lock('files/' + filename)
        try:
            lock.acquire(timeout=5)
        except LockTimeout:
            print()
            'File in use - try again later'
            return None
        except Exception:
            self.logger.exception(f'Error reading file {filename}')
            raise
        else:
            self._edit_append(filename, data)
        finally:
            lock.release()

    def _edit_append(self, filename, data):
        """ Separate function, called if you already have a lock acquired for appending"""
        if not self._exists(filename):
            self.logger.error('Append failed - {filename} does not exist')
            return False
        else:
            chunksize = self.master.get_chunksize(filename)
            last_chunk_id = self.master.get_last_chunkuuid(filename)
            num_chunks, _ = self._num_chunks(len(data), chunksize)
            seq = int(last_chunk_id.split('$%#')[1]) + 1
            append_chunkuuids = self.master.alloc2_chunks(num_chunks, filename, seq)
            # print 'append_chuids', append_chunkuuids
            if not append_chunkuuids:
                print()
                'No chunkservers online'
                return False
            chunklist = self._write_chunks(append_chunkuuids, data, chunksize)
            # print 'chunklist = %s' % chunklist
            if chunklist:
                self._update_master(filename, chunklist)
                return True
            else:
                'Failed to write file'
                return False

    def deletechunk(self, filename, chunkdetails, len_newdata, len_olddata, chunksize):
        """
        Delete chunk from the filetable
        :param filename:
        :param chunkdetails:
        :param len_newdata:
        :param len_olddata:
        :param chunksize:
        :return:
        """
        x = 0
        chunkids = []
        for chunkuuid in chunkdetails:
            if x > len_newdata:
                chunkids.append(chunkuuid['chunkuid'])
            x += chunksize
        self.master.delete_chunks(filename, chunkids)
        return True

    @staticmethod
    def replacechunk(chunkserver_clients, failed_chunkservers, chunkdetails, data1, data2, chunksize):
        """
        Replace data in the given chunk and propagate edits to all chunkservers holding that chunk
        :param chunkserver_clients:
        :param failed_chunkservers:
        :param chunkdetails:
        :param data1:
        :param data2:
        :param chunksize:
        :return:
        """
        y = 0
        # chunkserver_clients = self._establish_connection()  # can be avoided, pass from the edit function
        for x in range(0, len(data1), chunksize):
            if data1[x:x + chunksize] != data2[x:x + chunksize] or len(
                    data2[x:x + chunksize]) < chunksize:
                # print 'replace '' + data1[x:x + chunksize] + '' with '' + data2[
                #                                                         x:x + chunksize] + '''
                validservers = list(set(chunkdetails[y]['chunkloc']) - set(failed_chunkservers))
                for i in validservers:
                    try:
                        chunkserver_clients[i].write(chunkdetails[y]['chunkuid'],
                                                     data2[x:x + chunksize])
                    except Exception:
                        pass  # TODO why

            y += 1
        return True

    # def append(self, filename, data):
    #     if not self._exists(filename):
    #         raise Exception('append error, file does not exist: ' + filename)
    #     else:
    #         num_chunks = self._num_chunks(len(data))
    #         chunkuuids = self.master.get_chunkuuids(filename)[-1]
    #         seq = int(chunkuuids.split('$%#')[1]) + 1
    #         append_chunkuuids = self.master.alloc_append(num_chunks, filename, seq)
    #         self._write_chunks(append_chunkuuids, data)

    def delete(self, filename):
        """

        :param filename:
        """
        if not self._exists(filename):
            raise OSError(2, 'No such file or directory', filename)
        else:
            self.master.delete(filename, '')

    def edit(self, filename, newdata):
        """
        Read the file with the read() from above and update only the
        chunkservers where the data in the chunk has changed
        """

        if not self._exists(filename):
            raise Exception('read error, file does not exist: ' + filename)

        # TODO possibly change, read acquires full lock so this can't happen after asking lock below
        # kazoo lock is not reentrant, so it will block forever if the same thread acquires twice
        olddata, chunkdetails, chunkservers, failed_chunkservers = self.read_with_details(filename, [])

        if not olddata:
            return False  # exit if unable to read details

        # TODO split into multiple try blocks with appropriate-sized scope
        try:
            lock = self.zookeeper.Lock('files/' + filename)
            lock.acquire(timeout=5)
            # chunks = []
            # i = 0
            # chunkuuids = self.master.get_chunkuuids(filename)
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

            # print '\nCurrent data in ' + filename + '\n' + olddata + '\nEdited data:\n' + newdata

            # newchunks = []
            # chunksize = self.master.get('chunksize')
            chunksize = self.master.get_chunksize(filename)
            len_newdata = len(newdata)
            len_olddata = len(olddata)
            # newchunks = [newdata[x:x + chunksize] for x in range(0, len_newdata, chunksize)]

            if len_newdata == len_olddata and newdata != olddata:
                self.replacechunk(chunkservers, failed_chunkservers, chunkdetails, olddata, newdata, chunksize)
            elif len_newdata < len_olddata:
                self.replacechunk(chunkservers, failed_chunkservers, chunkdetails,
                                  olddata[0:len_newdata], newdata, chunksize)
                # print 'call fn() to delete chunks ' + olddata[
                # len_newdata + 1:] + ' from chunk server'
                self.deletechunk(filename, chunkdetails, len_newdata, len_olddata, chunksize)
            elif len_newdata > len_olddata:
                self.replacechunk(chunkservers, failed_chunkservers, chunkdetails, olddata,
                                  newdata[0:len_olddata], chunksize)
                # print 'call fn() to add chunks '' + newdata[len_olddata + 1:] + '' to chunk server'
                self._edit_append(filename, newdata[len_olddata:])

            self.master.updatevrsn(filename, 1)

            try:
                for chunkserver in chunkservers:
                    chunkserver.close()
            except Exception:
                pass

        except LockTimeout:
            self.logger.info('File in use - try again later')
            return None
        except Exception:
            self.logger.exception(f'Unexpected error editing file {filename} - try again later')
        finally:
            lock.release()

    def rename(self, filename, newfilename):
        """

        :param filename:
        :param newfilename:
        """
        if self._exists(filename):
            if not self._exists(newfilename):
                chunkuids = self.master.get_chunkuuids(filename)
                result = defaultdict(list)
                for chunkuid in chunkuids:
                    # maybe use subprocess to execute the download process in parallel
                    # may throw error if chunkserver dies off in between
                    chunklocs = self.master.get_chunkloc(chunkuid)
                    for chunkloc in chunklocs:
                        result[chunkloc].append(chunkuid)

                    self.master.rename(result, filename, newfilename)

            else:
                self.logger.info(f'File already exists: {newfilename}')
        else:
            self.logger.error(f'read error, file does not exist: {filename}')
