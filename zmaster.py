import ast
import random
import sys
import threading
import time
import uuid
import getpass

import zerorpc
from apscheduler.schedulers.background import BackgroundScheduler
from kazoo.client import KazooClient

import zutils

CHUNKSERVER_PATH = 'chunkserver/'
UPDATE_FREQUENCY = 5  # update frequency in seconds


class ZMaster:
    def __init__(self, zoo_ip='localhost:2181', master_port=1400):
        self.lock = threading.RLock()  # lock for modifying metadata
        self.num_chunkservers = 0
        self.last_updated = 0  # time since last stats poll
        self.ip = zutils.get_myip() + ':' + str(master_port)
        # self.chunksize = 10
        self.chunkrobin = 0
        self.versntable = {}  # file version counter
        # self.filetable = {'#garbage_collection#': {'0000000024': [
        #    'abc$%#0$%#27a04c46-9c4f-11e5-92b7-000c29c12a87']}}  # file to chunk mapping
        self.filetable = {'#garbage_collection#': {}}
        self.chunktable = {}  # chunkuuid to chunkloc mapping
        self.chunkservers = {}  # loc id to chunkserver mapping
        self.no_replica = 3
        # self.init_chunkservers()
        self.chunkclients = {}  # zerorpc clients connected to chunkservers
        self.chunkstats = {}  # stats for capacity and network load
        self.chunksize = {}  # filename -> chunksize mapping
        self.zookeeper = KazooClient(hosts=zoo_ip)
        self._register_with_zookeeper(master_port)

        # this schedules background tasks in separate thread
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.collect_garbage, 'interval', minutes=10)
        scheduler.add_job(self.replicate, 'interval', minutes=5)
        scheduler.start()

    def _register_with_zookeeper(self, master_port):
        try:
            self.zookeeper.start()
            address = "tcp://%s:%s" % (zutils.get_myip(), master_port)

            # use ephemeral node for shadow master to subscribe to later
            # self.zookeeper.create('master', ephemeral=True, value=address)
            self.zookeeper.ensure_path('master')
            self.zookeeper.ensure_path('chunkserver')
            self.zookeeper.create('master/0', ephemeral=True)
            data = '{username}@{tcpip}'.format(username=getpass.getuser(),
                                               tcpip=address)

            self.zookeeper.set('master', data)

            # registers chunkserver with master when ip set on zookeeper
            def watch_ip(event):
                path = event.path
                # chunkserver_ip = self.zookeeper.get(path)[0] ~ changed to username@[tcp:ip]
                chunkserver_ip = self.zookeeper.get(path)[0].split('@')[-1]
                chunkserver_num = path[path.rfind('/') + 1:]
                print "New IP %s detected in chunknum %s" % (chunkserver_ip, chunkserver_num)
                self._register_chunkserver(chunkserver_num, chunkserver_ip)

            @self.zookeeper.ChildrenWatch(CHUNKSERVER_PATH)
            def watch_children(children):
                if len(children) > len(self.chunkservers):
                    print "New chunkserver(s) detected"
                    # This creates a watch function for each new chunk server, where the
                    # master waits to register until the data(ip address) is updated
                    new_chunkservers = [chunkserver_num for chunkserver_num in children
                                        if chunkserver_num not in self.chunkservers]
                    for chunkserver_num in new_chunkservers:
                        try:
                            # ~ changed to username@[tcp:ip]
                            # chunkserver_ip = self.zookeeper.get(CHUNKSERVER_PATH +
                            #                                     chunkserver_num)[0]
                            chunkserver_ip = self.zookeeper.get(CHUNKSERVER_PATH +
                                                                chunkserver_num)[0].split('@')[-1]
                            # if IP is not set yet, assign watcher to wait
                            if len(chunkserver_ip) == 0:
                                self.zookeeper.exists(CHUNKSERVER_PATH + chunkserver_num,
                                                      watch=watch_ip)
                            else:
                                self._register_chunkserver(chunkserver_num, chunkserver_ip)
                        except Exception as ex:
                            self.print_exception('watch children, adding chunkserver', ex)

                elif len(children) < len(self.chunkservers):
                    self.lock.acquire()
                    try:
                        removed_servers = [chunkserver_num for chunkserver_num in self.chunkservers
                                           if chunkserver_num not in children]
                        for chunkserver_num in removed_servers:
                            self._unregister_chunkserver(chunkserver_num)
                            print "Chunkserver %s was removed" % chunkserver_num

                        self.num_chunkservers = len(self.chunkservers)
                        print "Now %d chunksrv" % self.num_chunkservers
                        print "Calling replicate directly"
                        self.replicate()
                    except Exception as ex:
                        self.print_exception('Removing chunkserver', ex)
                    finally:
                        self.lock.release()
        except Exception as e:
            self.print_exception('connecting to zookeeper', e)
            print "Unable to connect to zookeeper - master shutting down"
            sys.exit(2)

    def set_chunk(self):
        self.num_chunkservers = len(self.chunkservers)

    def _register_chunkserver(self, chunkserver_num, chunkserver_ip):
        """
        Adds chunkserver IP to chunkserver table and a connected zerorpc client to chunkclients
        :param chunkserver_num:
        :param chunkserver_ip:
        :return:  None
        """

        self.lock.acquire()
        try:
            c = zerorpc.Client()
            c.connect(chunkserver_ip)
            files, chunkloc = c.populate()
            if files:
                print 'Populating files from server %s' % chunkserver_num
                self.populate(files, chunkloc)

            self.chunkclients[chunkserver_num] = c
            self.chunkservers[chunkserver_num] = chunkserver_ip
            self.num_chunkservers += 1
            # self.chunkstats[chunkserver_num] = c.get_stats()
            print 'Chunksrv #%d registered at %s' % (int(chunkserver_num), chunkserver_ip)
            print 'Number of chunkservers = %d' % self.num_chunkservers
        except Exception as e:
            self.print_exception('registering chunkserver %s to %s' %
                                 (chunkserver_num, chunkserver_ip), e)
        finally:
            self.lock.release()

    def _unregister_chunkserver(self, chunkserver_num):

        # called inside watch children, metadata lock already acquired
        del self.chunkservers[chunkserver_num]
        del self.chunkclients[chunkserver_num]
        for filename, chunkid_list in self.filetable.items():
            for chunkid in chunkid_list:
                if chunkserver_num in self.chunktable[chunkid]:
                    print "Removing %s from %s " % (chunkserver_num, chunkid)
                    self.chunktable[chunkid].remove(chunkserver_num)
                    if not self.chunktable[chunkid]:
                        self.print_exception("List is empty now, deleting file %s " % filename,
                                             None)
                        self.delete(filename, '')
                        break  # breaks inner for loop

                        # print "\t Chunktable = ", self.chunktable
                        # print "\t Filetable = ", self.filetable
                        # print "\t Chunkservers = ", self.chunkservers

    @staticmethod
    def print_exception(context, exception, message=''):
        print "Unexpected error in ", context, message
        if exception:
            print type(exception).__name__, ': ', exception.args

    def get(self, ivar):
        """
        Exposes ZMaster member variables through method access.
        :param ivar: instance variable of ZMaster
        :return:  returns the value of the instance variable
        """
        try:
            return self.__dict__[ivar]
        except KeyError:
            self.print_exception('key error in get', KeyError)

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
        exists = filename in self.filetable
        return exists
        # return True if filename in self.filetable else False

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

    def get_last_chunkuuid(self, filename):
        chunkuuid = self.filetable[filename][-1]
        return chunkuuid

    # TODO what about same file exists?
    def update_file(self, filename, chunklist):
        self.lock.acquire()
        try:
            if filename not in self.filetable:
                self.filetable[filename] = []

            for chunkuuid, chunkloc in chunklist:
                self.filetable[filename].append(chunkuuid)
                self.chunktable[chunkuuid] = chunkloc
        except Exception as e:
            self.print_exception('updating file', e)
        finally:
            self.lock.release()

    def get_file_chunks(self, filename):
        """ Returns only relevant chunkuuids instead of entire chunktable """
        chunkuuids = self.filetable[filename]
        file_chunks = {chunkid: self.chunktable[chunkid] for chunkid in chunkuuids}

        return file_chunks

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
            try:
                zclient = zerorpc.Client()
                print 'Server connecting to chunkserver at %s' % chunkservers[chunkloc]
                zclient.connect(chunkservers[chunkloc])
                zclient.print_name()
                return zclient
            except:
                return False

    # TODO delete /tmp/gfs/files/*
    def collect_garbage(self):

        try:
            chunklocs = self.filetable["#garbage_collection#"]
            print chunklocs, chunklocs.keys(), self.chunkservers.keys()
            failedservers = list(set(chunklocs.keys()) - set(self.chunkservers.keys()))
            legitservers = list(set(chunklocs.keys()) - set(failedservers))
            print "legit,failed ", legitservers, failedservers
        except:
            chunklocs = {}

        if chunklocs:
            print "in garbage"
            for chunkloc in chunklocs.keys():
                # connect with each chunkserver if its not a failed server
                if chunkloc not in failedservers:
                    chunkserver_clients = self._establish_connection(chunkloc)

                    if chunkserver_clients != False:
                        # print "call delchunkfile fn() in chunkserver-"+str(chunkloc)
                        flag = chunkserver_clients.delete(list(set(chunklocs[chunkloc])))
                        if flag:
                            # remove chunkid if present in failedservers
                            for chunkid in list(set(chunklocs[chunkloc])):
                                i = 0
                                while i < len(failedservers):
                                    if chunkid in chunklocs[failedservers[i]]:
                                        chunklocs[failedservers[i]].remove(chunkid)
                                        if chunklocs[failedservers[i]] == []:
                                            del self.filetable["#garbage_collection#"][
                                                failedservers[i]]
                                            failedservers.remove(failedservers[i])
                                        break
                                    i += 1

                            # print "remove value from garbage collection for "+str(chunkloc)
                            del self.filetable["#garbage_collection#"][chunkloc]

                    else:
                        print "Failed to connect to ", chunkloc
        else:
            print "nothing to clear in garbage"

    def replicate(self):
        # print "In replicate"
        self.lock.acquire()
        try:
            replicas = {}
            no_servers = len(self.chunkservers)

            # skip checking for copies if we have 0 chunkservers
            if no_servers == 0:
                print "No chunkservers online"
                return None

            reps = min(self.no_replica, no_servers)

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
                    if chunkloc not in chunkserver:
                        try:
                            chunkserver[chunkloc] = self._establish_connection(chunkloc)
                        except Exception as e:
                            self.print_exception('in replicate', e)

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
                # print "replica: ", replicas
                print "replicated %d items " % len(replicas.keys())
        except Exception as e:
            self.print_exception('acquiring lock in replicate', e)
        finally:
            self.lock.release()

    def delete(self, filename, chunkuuids):  # rename for later garbage collection

        self.lock.acquire()
        try:
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
        except Exception as e:
            print "Unexpected error in delete:"
            print e.__doc__, e.message
        finally:
            self.lock.release()

    def delete_chunks(self, filename, chunk_rm_ids):

        self.lock.acquire()
        try:
            print filename, chunk_rm_ids
            chunkuuids = self.filetable[filename]
            self.filetable[filename] = [x for x in chunkuuids if x not in chunk_rm_ids]
            self.delete(filename, chunk_rm_ids)
        except Exception as e:
            self.print_exception('delete_chunks', e)
        finally:
            self.lock.release()

    def next_chunkloc(self, keys_list, num_items):
        next_chunklocs = random.sample(keys_list, num_items)
        return next_chunklocs

    def alloc2(self, filename, num_chunks, chunksize, seq):  # return ordered chunk map to server
        chunks = self.alloc2_chunks(num_chunks, filename, seq)
        # self.filetable[filename] = chunks  don't update filetable until writing successful
        self.chunksize[filename] = chunksize
        return chunks

    def alloc2_chunks(self, num_chunks, filename, seq):

        if self.num_chunkservers == 0:
            return None

        # chunkuuids = {}
        chunkuuids = []
        tseq = seq
        keys_list = self.chunkservers.keys()
        for i in range(0, num_chunks):
            chunkuuid = filename + "$%#" + str(tseq) + "$%#" + str(uuid.uuid1())

            if len(keys_list) < 3:
                num_chunklocs = len(keys_list)
            else:
                num_chunklocs = 3

            next_chunklocs = self.next_chunkloc(keys_list, num_chunklocs)
            chunkuuids.append((chunkuuid, next_chunklocs))
            tseq += 1

        return chunkuuids

    def alloc(self, filename, num_chunks, chunksize, seq):  # return ordered chunk list
        chunks = self.alloc_chunks(num_chunks, filename, seq)
        self.filetable[filename] = chunks
        self.chunksize[filename] = chunksize
        return chunks

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
        self.lock.acquire()
        try:
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
        except Exception as e:
            self.print_exception('rename', e)
        finally:
            self.lock.release()

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
        self.lock.acquire()
        try:
            for values in self.chunktable.itervalues():
                values.remove(chunkloc)
        except Exception as e:
            self.print_exception('rm from ctable', e)
        finally:
            self.lock.release()

    def sort_filetable(self, filename):

        self.lock.acquire()
        try:
            temp = []
            temptable = {}

            for fileid in self.filetable[filename]:
                temptable[fileid.split("$%#")[1]] = fileid

            keys = sorted(temptable.keys())
            for key in keys:
                temp.append(temptable[key])
            self.filetable[filename] = temp
        except Exception as e:
            self.print_exception('sort filetable', e)
        finally:
            self.lock.release()

    def populate(self, files, chunkloc):

        self.lock.acquire()
        try:
            # print files, chunkloc, "in master"
            for filename, chunkids in files.items():
                if self.exists(filename):
                    print "operations for merging"
                    for chunkid in chunkids:
                        if chunkid not in self.filetable[filename] and chunkid not in [list(x)[0]
                                                                                       for x in set(
                                tuple(x) for x in
                                self.filetable['#garbage_collection#'].values())]:
                            if True:  # condition to check if hash for chunkids are the same
                                self.filetable[filename].append(chunkid)
                                self.chunktable[chunkid] = [chunkloc]
                        else:
                            if len(self.chunktable[
                                       chunkid]) < self.no_replica:  # also check if data is the same
                                self.chunktable[chunkid].append(chunkloc)
                            else:
                                try:
                                    self.filetable['#garbage_collection#'][chunkloc].append(chunkid)
                                except:

                                    self.filetable['#garbage_collection#'][chunkloc] = [chunkid]
                else:
                    orig_chunkids = chunkids[:]
                    chunkids = list(set(chunkids) - set([list(x)[0] for x in set(
                        tuple(x) for x in self.filetable['#garbage_collection#'].values())]))
                    print "chunkids", chunkids
                    if chunkids != []:
                        print "operation for adding", filename
                        self.filetable[filename] = chunkids
                        temp = {}
                        for chunkid in chunkids:
                            temp[chunkid] = [chunkloc]
                        self.chunktable.update(temp)
                        self.versntable[filename] = 1
                        # update chunksize table
                    else:
                        print "operation for deleting", list(set(orig_chunkids) - set(chunkids))

                    for chunkid in list(set(orig_chunkids) - set(chunkids)):
                        try:
                            self.filetable['#garbage_collection#'][chunkloc].append(chunkid)
                        except:
                            self.filetable['#garbage_collection#'][chunkloc] = [chunkid]

                if self.exists(filename):
                    self.sort_filetable(filename)

                    # print self.filetable
                    # print self.chunktable
        except Exception as e:
            self.print_exception('populate', e)
        finally:
            self.lock.release()
