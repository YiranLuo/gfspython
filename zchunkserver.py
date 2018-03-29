"""
Module zchunkserver contains the `ZChunkserver` class, which models a GFS chunkserver.  Chunkservers primarily
communicate metadata with the master server and facilitate transfers directly to clients.
"""

import ast
import getpass
import logging
import os
import sys
from collections import defaultdict

import xxhash
import zerorpc
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NoNodeError

import zutils


class ZChunkserver:
    """
    ZChunkserver class contains methods to manipulate file chunks and connect with clients and master server.
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
        Query Zookeeper and return the master's IP address.
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
        Writes the given chunk to file and forwards the chunk to the given chunkserver(s) to replicate if necessary.
        :param chunkuuid: Unique ID for chunk.
        :param chunk:  Chunk data.
        :param forward:  A list of chunkservers to forward the chunk to for replication.
        :return:  Checksum of chunk written for verification.
        """

        self.write_local(chunkuuid, chunk)

        if forward:
            self.logger.debug('Forwarding chunk to chunkserver {forward}')
            self.send_chunk(chunkuuid, str([forward]), chunk)

        return xxhash.xxh64(chunk).digest()

    def write_local(self, chunkuuid, chunk):
        """ Writes the given chunk to local disk and updates metadata if successful """

        local_filename = self.chunk_filename(chunkuuid)
        try:
            with open(local_filename, "wb") as f:
                f.write(chunk)
        except EnvironmentError:
            self.logger.exception(f'Could not write chunk with id {chunkuuid}')
            return False
        else:
            self.chunktable[chunkuuid] = local_filename
            return True

    def close_master(self):
        """ Close the connection to master server.  """
        self.master.close()

    @staticmethod
    def get_stats():
        """ Returns network traffic and storage stats for this chunkserver. """

        return zutils.get_stats()

    def read(self, chunkuuid):
        """ Reads chunk from file and returns the contents """

        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "rb") as f:
            data = f.read()
        return data

    def _establish_connection(self, chunkloc):
        chunkservers = self.master.get('chunkservers')
        zclient = zerorpc.Client()
        self.logger.info(f'Server connecting to chunkserver at {chunkloc}')
        zclient.connect(chunkservers[chunkloc])
        return zclient

    def delete(self, chunkuuids):
        """ Remove each chunk from disk given a list of chunkuuids.  Called by garbage collection
        :param chunkuuids: A list of chunkuuid to delete from disk
        """

        for filename in map(self.chunk_filename, chunkuuids):
            try:
                self.logger.info(f'Removing {filename}')
                os.remove(filename)
            except EnvironmentError:
                self.logger.exception(f'Error deleting file with name {filename}')

    def chunk_filename(self, chunkuuid):
        """ Maps a universally unique ID to a local filename for a specific chunk.
        :param chunkuuid: Universally unique id for the given chunk
        :return: Local unique filename.
        """
        local_filename = f'{self.local_filesystem_root}/{chunkuuid}.gfs'
        return local_filename

    def copy_chunk(self, chunk_id, chunklocs):
        """
        Copy a chunk with the given chunk_id to the local filesystem.  Copy_chunk queries each chunkserver in
        chunklocs until the transfer is successful or there are no remaining servers to contact.
        :param chunk_id: UUID of the chunk to copy
        :param chunklocs: A list of chunkservers that service this chunk
        :return: True if transfer was successful or false otherwise.
        """

        # TODO why is this literal_eval?
        chunklocs = ast.literal_eval(chunklocs)
        flag = False
        for chunkloc in chunklocs:
            try:
                chunkserver = self._establish_connection(chunkloc)
                data = chunkserver.read(chunk_id)
                flag = self.write_local(chunk_id, data)
                if flag:
                    break
            except EnvironmentError:
                flag = False
                self.logger.exception('some error happened in copy_chunk')
        else:
            self.logger.error(f'No chunkservers available to copy chunk {chunk_id}')

        return flag

    def send_chunk(self, chunk_id, chunklocs, data):
        """ Sends chunk data to a list of chunkservers
        :param chunk_id: UUID of the given chunk
        :param chunklocs:  List of chunkservers to replicate the chunk to.
        :param data:  Chunk data
        :return:  True if successful or false otherwise.
        """
        chunklocs = ast.literal_eval(chunklocs)
        flag = False
        for chunkloc in chunklocs:
            try:
                chunkserver = self._establish_connection(chunkloc)
                flag = chunkserver.write_local(chunk_id, data)
                if flag:
                    break
            except EnvironmentError:
                flag = False
                self.logger.exception(f'Error sending {chunk_id} to {chunkserver}')

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
        Populate metadata from local contents and send to master
        :return: A tuple containing filetable and chunkloc of chunkserver.
        """
        # print "in populate, chunkloc=", self.chunkloc
        local_dir = self.chunk_filename("").replace(".gfs", "")
        # print "local dir is ", local_dir
        file_list = os.listdir(local_dir)
        if len(file_list) != 0:
            files = defaultdict(list)
            for items in file_list:
                # TODO
                # if master.exists
                # read all chunks (in parallel?)
                # if any xxhash is not the same, os.delete()
                # else add as regular
                items = items.replace(".gfs", "")
                filename = items.split("$%#")[0]
                self.chunktable[items] = self.chunk_filename(items)
                files[filename].append(items)

            # print "files=%s, chunkloc=%s" % (files, self.chunkloc)
            # self.master.populate(files, str(self.chunkloc))
            return files, self.chunkloc
        else:
            self.logger.debug('nothing to populate')
            return None, None
