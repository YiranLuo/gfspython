""" Watcher module implements shadow master functionality, which includes the ability to regenerate both the master
server and chunkservers.  Watcher monitors Zookeeper for ephemeral files that disappear whenever the process stops.
"""
import logging
import subprocess
import sys
import threading

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException

import zutils

PORT = 1401
CHUNKSERVER_PATH = 'chunkserver/'
MASTER_PATH = 'master'
GFS_PATH = '~/gfspython/'
SERVER = 'create_server.py {}'
MASTER = 'create_master.py {}'


class Watcher:
    """ Watcher class monitors zookeeper for component failures and regenerates failed components """

    def __init__(self, zoo_ip='localhost:2181', port=PORT):
        self.logger = logging.getLogger(__name__)
        self.lock = threading.RLock()
        self.chunkservers = {}
        self.master_address = None
        self.garbage_table = {'#garbage_collection#': {}}
        self.zookeeper = KazooClient(hosts=zoo_ip)
        self._register_with_zookeeper(port)

        global SERVER
        if zoo_ip == 'localhost':
            zoo_ip = zutils.get_myip()
        SERVER = SERVER.format(zoo_ip)
        global MASTER
        MASTER = MASTER.format(zoo_ip)

    def _register_with_zookeeper(self, port):
        try:
            self.zookeeper.start()
        except KazooException:
            self.logger.exception('Unable to connect to zookeeper, shutting down')
            sys.exit(2)
        else:
            address = zutils.get_tcp(port)
            self.zookeeper.ensure_path('watcher')
            self.zookeeper.set('watcher', address)

            # self.master_address = self.zookeeper.get('master')[0].split('@')[-1]
            master_ip = self.zookeeper.get('master')[0]
            self.master_address = self.convert_zookeeper_ip(master_ip)
            self.logger.info(f'Registered with master at {self.master_address}')

        def watch_it(event):
            """

            :param event:
            :return:
            """
            path = event.path
            # get chunkserver_ip = uname@tcp://ip:port, convert to uname@ip for ssh
            try:
                chunkserver_ip = self.convert_zookeeper_ip(self.zookeeper.get(path)[0])
                chunkserver_num = path[path.rfind('/') + 1:]
            except Exception as e:
                print(f'Error registering chunkserver:  {e.message}')
                return False

            self.logger.info('Registering chunkserver num %s as %s' % (chunkserver_num, chunkserver_ip))
            self._register_chunkserver(chunkserver_num, chunkserver_ip)

        @self.zookeeper.ChildrenWatch(MASTER_PATH)
        def watch_master():
            """
            """
            children = self.zookeeper.get_children('master')
            if children:
                self.master_address = self.convert_zookeeper_ip(self.zookeeper.get('master')[0])
                self.logger.info(f'Master down - bringing up new master at {self.master_address}')
            else:
                if self.ssh(self.master_address, MASTER):
                    self.logger.info(f'New master initializing at {self.master_address}')
                else:
                    # TODO cycle through chunkservers until exhausted or a master is successfully created
                    self.logger.critical('Could not recover master')

        @self.zookeeper.ChildrenWatch(CHUNKSERVER_PATH)
        def watch_chunkservers(children):
            """

            :param children:
            """
            if len(children) > len(self.chunkservers):
                self.logger.info('New chunkserver(s) detected')
                # This creates a watch function for each new chunk server, where the
                # master waits to register until the data(ip address) is updated
                new_chunkservers = [chunkserver_num for chunkserver_num in children
                                    if chunkserver_num not in self.chunkservers]
                for chunkserver_num in new_chunkservers:
                    try:
                        # zoo_ip = self.zookeeper.get(CHUNKSERVER_PATH + chunkserver_num,
                        #                             watch=watch_it)[0]
                        zoo_ip = self.zookeeper.get(CHUNKSERVER_PATH + chunkserver_num)[0]
                        self.logger.info(f'zoo ip = {zoo_ip}')

                        # chunkserver_ip = self.convert_zookeeper_ip(
                        #     self.zookeeper.get(CHUNKSERVER_PATH + chunkserver_num)[0])
                        if not zoo_ip:
                            print()
                            self.logger.info('zoo_ip not ready, watching for it.')
                            self.zookeeper.exists(CHUNKSERVER_PATH + chunkserver_num,
                                                  watch=watch_it)
                        else:
                            chunkserver_ip = self.convert_zookeeper_ip(zoo_ip)
                            self._register_chunkserver(chunkserver_num, chunkserver_ip)
                    except Exception as ex:
                        zutils.print_exception('watch children, adding chunkserver', ex)

            elif len(children) < len(self.chunkservers):

                try:
                    removed_servers = [chunkserver_num for chunkserver_num in self.chunkservers
                                       if chunkserver_num not in children]
                    for chunkserver_num in removed_servers:

                        if self.ssh(self.chunkservers[chunkserver_num], SERVER):
                            self.logger.info(f'Successfully regenerated chunkserver #{chunkserver_num}')
                        else:
                            self.logger.critical(f'Failed to recover from chunkserver #{chunkserver_num} failure')
                            self._unregister_chunkserver(chunkserver_num)

                except Exception as ex:
                    zutils.print_exception('Removing chunkserver', ex)
                finally:
                    # TODO why is this here
                    pass

    def _register_chunkserver(self, chunkserver_num, chunkserver_ip):
        """
        Adds chunkserver IP to chunkserver table
        :param chunkserver_num:
        :param chunkserver_ip:
        """

        self.lock.acquire()
        try:
            self.chunkservers[chunkserver_num] = chunkserver_ip
        except Exception as e:
            zutils.print_exception('register chunkserver', e)
        finally:
            self.lock.release()

    def _unregister_chunkserver(self, chunkserver_num):
        self.lock.acquire()
        try:
            del self.chunkservers[chunkserver_num]
        except Exception as e:
            zutils.print_exception('unregister chunkserver', e)
        finally:
            self.lock.release()

    def ssh(self, target, command):
        """ TODO fix this hacky stuff
        :param target:
        :param command:
        :return:
        """
        # y = x.split('@')[0]  name
        # y.split('//')[-1]command
        command = '{}{}'.format(GFS_PATH, command)
        print(f'in ssh, target={target}, command={command}')

        ssh_opts = {'shell': False, 'stdout': subprocess.PIPE, 'stdin': subprocess.PIPE}
        try:
            subprocess.Popen(['ssh', '%s' % target, command], **ssh_opts)
        except Exception:
            self.logger.exception(f'ssh command failed: {command}')
            return False
        else:
            return True

    def print_metadata(self):
        """ metadata associated with the watcher instance.
        """
        self.logger.info(f'Chunkservers: {self.chunkservers}\nmaster address: {self.master_address}')

    @staticmethod
    def convert_zookeeper_ip(data):
        """

        :param data:
        :return:
        """
        data = data.replace('tcp://', '')
        data = data[:data.rfind(':')]

        return data
