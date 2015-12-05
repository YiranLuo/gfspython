import ast
import os
import re
import subprocess
import xxhash

import zerorpc
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NoNodeError

import zutils


class ZChunkserver:
    def __init__(self, zoo_ip='localhost:2181'):
        self.chunktable = {}
        self.chunkloc = None
        self.master = zerorpc.Client()
        self.zookeeper = KazooClient(zoo_ip)

        # register with zookeeper, get IP of master
        # TODO:  need to add handling in case master is down here
        try:
            self.master_ip = self._register_with_zookeeper()
            print 'Chunkserver %d Connecting to master at %s' % (int(self.chunkloc), self.master_ip)
            self.master.connect(self.master_ip)
        except NoNodeError:
            print "No master record in zookeeper"
            raise  # TODO handle shadow master/waiting for master to reconnect later
        except Exception as e:
            print "Unexpected error connecting to master:"
            print e.__doc__, e.message

        # local directory where chunks are stored
        self.local_filesystem_root = "/tmp/gfs/chunks/" + repr(int(self.chunkloc))
        if not os.access(self.local_filesystem_root, os.W_OK):
            os.makedirs(self.local_filesystem_root)

    def _register_with_zookeeper(self):

        def my_listener(state):
            if state == KazooState.LOST or state == KazooState.SUSPENDED:
                print "suspended|lost state"
                # TODO connect to zookeeper again

        self.zookeeper.start()
        self.zookeeper.add_listener(my_listener)
        self.zookeeper.ensure_path('chunkserver')
        master_ip = self.zookeeper.get('master')[0]

        path = self.zookeeper.create('chunkserver/', ephemeral=True, sequence=True)
        # path = self.zookeeper.create('chunkserver/2', ephemeral=True)

        self.chunkloc = path.replace('/chunkserver/', '')
        self.zookeeper.set(path, zutils.get_tcp(4400 + int(self.chunkloc)))

        return master_ip

    def print_name(self):
        """
        Prints name to test connectivity
        """
        print 'I am chunkserver #' + str(int(self.chunkloc))
        self.master.answer_server(int(self.chunkloc))

    def write(self, chunkuuid, chunk, forward=None):
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "wb") as f:
            f.write(chunk)
            self.chunktable[chunkuuid] = local_filename

        print "forward is ", forward
        if forward:
            print "Forwarding chunk to loc", forward
            self.send_chunk(chunkuuid, str([forward]), chunk)
        return xxhash.xxh64(chunk).digest()

    def close(self):
        self.master.close()

    @staticmethod
    def get_stats():

        results = []
        pattern = r' \d+[\.]?\d*'
        first = ['ifstat', '-q', '-i', 'enP0s3', '-S', '0.2', '1']  # get network traffic
        second = ['df', '/']  # get free space
        p1 = subprocess.Popen(first, stdout=subprocess.PIPE)
        p2 = subprocess.Popen(second, stdout=subprocess.PIPE)

        # get transfer speed and parse results
        transfer_speed = p1.communicate()[0]
        transfer_speed = re.findall(pattern, transfer_speed)
        results.append(sum([float(num) for num in transfer_speed]))

        # get storage info and parse results
        storage = p2.communicate()[0]
        storage = re.findall(r'\d+%', storage)  # find entry with %
        results.append(int(storage[0][:-1]))  # append entry without %

        return results

    ##############################################################################

    def rwrite(self, chunkuuid, chunk):
        local_filename = self.chunk_filename(chunkuuid)
        try:
            with open(local_filename, "wb") as f:
                f.write(chunk)
            self.chunktable[chunkuuid] = local_filename
            return True
        except:
            return False

    def read(self, chunkuuid):
        data = None
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "rb") as f:
            data = f.read()
        return data

    def _establish_connection(self, chunkloc):
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
                    print "Removing " + filename
                    os.remove(filename)
                    return True
            except:
                None

    def disp(self, a):
        print str(a) + str(self.chunkloc)

    def chunk_filename(self, chunkuuid):
        local_filename = self.local_filesystem_root + "/" + str(chunkuuid) + '.gfs'
        return local_filename

    def copy_chunk(self, chunkid, chunklocs):
        chunklocs = ast.literal_eval(chunklocs)
        flag = False
        for chunkloc in chunklocs:
            try:
                chunkserver = self._establish_connection(chunkloc)
                # TODO md5 check
                data = chunkserver.read(chunkid)
                flag = self.rwrite(chunkid, data)
                if flag:
                    break
            except:
                flag = False
                print "soe"

        return flag

    def send_chunk(self, chunkid, chunklocs, data):
        chunklocs = ast.literal_eval(chunklocs)
        flag = False
        for chunkloc in chunklocs:
            try:
                chunkserver = self._establish_connection(chunkloc)
                flag = chunkserver.rwrite(chunkid, data)
                if flag:
                    break
            except Exception as e:
                flag = False
                self.master.print_exception('sending chunk', e)

        return flag

    def rename(self, chunkids, filename, newfilename):
        for chunkid in chunkids:
            local_filename = self.chunk_filename(chunkid)
            new_local_filename = local_filename.split('/')
            new_local_filename[-1] = new_local_filename[-1].replace(filename, newfilename)
            new_local_filename = '/'.join(new_local_filename)
            print "Changing %s to %s" % (local_filename, new_local_filename)
            try:
                os.rename(local_filename, new_local_filename)
            except:
                os.remove(new_local_filename)
                os.rename(local_filename, new_local_filename)
        return True

    def populate(self):
        print "in populate, chunkloc=", self.chunkloc
        local_dir = self.chunk_filename("").replace(".gfs", "")
        print "local dir is ", local_dir
        file_list = os.listdir(local_dir)
        if file_list != []:
            files = {}
            for items in file_list:
                items = items.replace(".gfs", "")
                filename = items.split("$%#")[0]
                self.chunktable[items] = self.chunk_filename(items)
                try:
                    files[filename].append(items)
                except:
                    files[filename] = []
                    files[filename].append(items)

            print "files=%s, chunkloc=%s" % (files, self.chunkloc)
            # self.master.populate(files, str(self.chunkloc))
            return files, self.chunkloc
        else:
            print "nothing to populate"
            return None, None
