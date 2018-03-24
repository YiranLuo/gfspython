import ast
import getpass
import logging
import os
import re
import subprocess
import sys

import xxhash
import zerorpc
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NoNodeError

import zutils


class ZChunkserver:
    """
    This is the Chunkserver class
    """

    def __init__(self, zoo_ip='localhost:2181'):
        self.logger = logging.getLogger(__name__)
        self.chunktable = {}
        self.chunkloc = None
        self.master = zerorpc.Client()
        self.zookeeper = KazooClient(zoo_ip)

        # register with zookeeper, get IP of master
        # TODO:  need to add handling in case master is down here
        # TODO: take master_ip out and convert to an attrib.
        try:
            self.master_ip = self._register_with_zookeeper()
            self.logger.info(f'Chunkserver {self.chunkloc} Connecting to master at {self.master_ip}')
            self.master.connect(self.master_ip)
        except NoNodeError:
            self.logger.critical('No master record in zookeeper')
            raise  # TODO handle shadow master/waiting for master to reconnect later
        except Exception:
            self.logger.debug(f'Unknown exception, traceback: {sys.exc_info()[2]}')
            self.logger.exception('Unexpected error connecting to master')

        # local directory where chunks are stored
        self.local_filesystem_root = "/tmp/gfs/chunks/"  # + repr(int(self.chunkloc))
        if not os.access(self.local_filesystem_root, os.W_OK):
            os.makedirs(self.local_filesystem_root)

    def _register_with_zookeeper(self):
        """
        Register Chunkserver with Zookeeper by getting chunkloc and creating ephemeral node.
        :return:
        """

        def my_listener(state):
            """
            :param state:
            """
            if state == KazooState.LOST or state == KazooState.SUSPENDED:
                self.logger.critical(f'Zookeeper connection interrupted, requires reconnect.')
                # TODO connect to zookeeper again

        try:
            self.zookeeper.start()
            self.zookeeper.add_listener(my_listener)
            self.zookeeper.ensure_path('chunkserver')
            # TODO this will be an attribute
            master_ip = self.zookeeper.get('master')[0].split('@')[-1]

            path = self.zookeeper.create('chunkserver/', ephemeral=True, sequence=True)
            self.chunkloc = path.replace('/chunkserver/', '')

            # self.zookeeper.set(path, zutils.get_tcp(4400 + int(self.chunkloc)))
            self.zookeeper.set(path, f'{getpass.getuser()}@{zutils.get_tcp(4400 + int(self.chunkloc))}')

        except Exception:
            self.logger.debug(f'Exception: {sys.exc_info()[2]}')
            self.logger.critical('Exception while registering with zookeeper')
            raise
        else:
            return master_ip

    # TODO make this an attribute
    def get_master_ip(self):
        """

        """
        pass

    def print_name(self):
        """
        Prints name to test connectivity
        """
        print()
        'I am chunkserver #' + str(int(self.chunkloc))
        self.master.answer_server(int(self.chunkloc))

    def write(self, chunkuuid, chunk, forward=None):
        """

        :param chunkuuid:
        :param chunk:
        :param forward:
        :return:
        """
        local_filename = self.chunk_filename(chunkuuid)
        try:
            with open(local_filename, "wb") as f:
                f.write(chunk)
                self.chunktable[chunkuuid] = local_filename
        except:
            return False

        # print "forward is ", forward
        if forward:
            self.logger.info('Forwarding chunk to chunkserver {forward}')
            self.send_chunk(chunkuuid, str([forward]), chunk)
        return xxhash.xxh64(chunk).digest()

    def close(self):
        """

        """
        self.master.close()

    @staticmethod
    def get_stats():
        """

        :return:
        """
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
        """

        :param chunkuuid:
        :param chunk:
        :return:
        """
        local_filename = self.chunk_filename(chunkuuid)
        try:
            with open(local_filename, "wb") as f:
                f.write(chunk)
            self.chunktable[chunkuuid] = local_filename
            return True
        except:
            return False

    def read(self, chunkuuid):
        """

        :param chunkuuid:
        :return:
        """
        # data = None
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "rb") as f:
            data = f.read()
        return data

    def _establish_connection(self, chunkloc):
        chunkservers = self.master.get('chunkservers')
        zclient = zerorpc.Client()
        self.logger.info(f'Server connecting to chunkserver at {chunkloc}')
        zclient.connect(chunkservers[chunkloc])
        # zclient.print_name()
        return zclient

    def delete(self, chunkuuids):
        """

        :param chunkuuids:
        :return:
        """
        for chunkid in chunkuuids:
            filename = self.chunk_filename(chunkid)
            try:
                if os.path.exists(filename):
                    self.logger.info(f'Removing {filename}')
                    os.remove(filename)
                    return True
            except Exception:
                self.logger.exception('Error deleting file')

    def disp(self, a):
        """
        :param a:
        """
        # TODO what does this do
        self.logger.info(f'{str(a)}{str(self.chunkloc)}')

    def chunk_filename(self, chunkuuid):
        """

        :param chunkuuid:
        :return:
        """
        local_filename = self.local_filesystem_root + "/" + str(chunkuuid) + '.gfs'
        return local_filename

    def copy_chunk(self, chunk_id, chunklocs):
        """

        :param chunk_id:
        :param chunklocs:
        :return:
        """
        chunklocs = ast.literal_eval(chunklocs)
        flag = False
        for chunkloc in chunklocs:
            try:
                chunkserver = self._establish_connection(chunkloc)
                # TODO md5 check
                data = chunkserver.read(chunk_id)
                flag = self.rwrite(chunk_id, data)
                if flag:
                    break
            except Exception:
                flag = False
                self.logger.exception('some error happened in copy_chunk')

        return flag

    def send_chunk(self, chunk_id, chunklocs, data):
        """

        :param chunk_id:
        :param chunklocs:
        :param data:
        :return:
        """
        chunklocs = ast.literal_eval(chunklocs)
        flag = False
        for chunkloc in chunklocs:
            try:
                chunkserver = self._establish_connection(chunkloc)
                flag = chunkserver.rwrite(chunk_id, data)
                if flag:
                    break
            except Exception as e:
                flag = False
                self.master.print_exception('sending chunk', None, type(e).__name__)

        return flag

    def rename(self, chunkids, filename, newfilename):
        """

        :param chunkids:
        :param filename:
        :param newfilename:
        :return:
        """
        for chunkid in chunkids:
            local_filename = self.chunk_filename(chunkid)
            new_local_filename = local_filename.split('/')
            new_local_filename[-1] = new_local_filename[-1].replace(filename, newfilename)
            new_local_filename = '/'.join(new_local_filename)
            self.logger.info(f'Changing {local_filename} to {new_local_filename}')
            try:
                os.rename(local_filename, new_local_filename)
            # TODO add specific OS exception here
            except Exception:
                os.remove(new_local_filename)
                os.rename(local_filename, new_local_filename)
        return True

    def populate(self):
        """

        :return:
        """
        # print "in populate, chunkloc=", self.chunkloc
        local_dir = self.chunk_filename("").replace(".gfs", "")
        # print "local dir is ", local_dir
        file_list = os.listdir(local_dir)
        if len(file_list) != 0:
            files = {}
            for items in file_list:
                # TODO
                # if master.exists
                # read all chunks (in parallel?)
                # if any xxhash is not the same, os.delete()
                # else add as regular
                items = items.replace(".gfs", "")
                filename = items.split("$%#")[0]
                self.chunktable[items] = self.chunk_filename(items)
                try:
                    files[filename].append(items)
                except:
                    files[filename] = []
                    files[filename].append(items)

            # print "files=%s, chunkloc=%s" % (files, self.chunkloc)
            # self.master.populate(files, str(self.chunkloc))
            return files, self.chunkloc
        else:
            print()
            "nothing to populate"
            return None, None
