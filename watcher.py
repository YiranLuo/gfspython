import sys
import subprocess
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


def ssh(target, command):
    # y = x.split('@')[0]  name
    # y.split('//')[-1]command
    command = '{}{}'.format(GFS_PATH, command)
    print(f'in ssh, target={target}, command={command}')

    try:
        subprocess.Popen(['ssh', '%s' % target, command],
                         shell=False,
                         stdout=subprocess.PIPE,
                         stdin=subprocess.PIPE)

        return True

    except Exception as e:
        print()
        e.message, type(e)
        return False


class Watcher:
    def __init__(self, zoo_ip='localhost:2181', port=PORT):
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
            print()
            'Unable to connect to zookeeper, shutting down'
            sys.exit(2)
        else:
            address = "tcp://%s:%s" % (zutils.get_myip(), port)
            self.zookeeper.ensure_path('watcher')
            self.zookeeper.set('watcher', address)

            # self.master_address = self.zookeeper.get('master')[0].split('@')[-1]
            master_ip = self.zookeeper.get('master')[0]
            self.master_address = self.convert_zookeeper_ip(master_ip)
            print()
            'master address', self.master_address, master_ip

        def watch_it(event):
            path = event.path
            # get chunkserver_ip = uname@tcp://ip:port, convert to uname@ip for ssh
            try:
                chunkserver_ip = self.convert_zookeeper_ip(self.zookeeper.get(path)[0])
                chunkserver_num = path[path.rfind('/') + 1:]
            except Exception as e:
                print(f'Error registering chunkserver:  {e.message}')
                return False

            print()
            'Registering chunkserver num %s as %s' % (chunkserver_num, chunkserver_ip)
            self._register_chunkserver(chunkserver_num, chunkserver_ip)

        @self.zookeeper.ChildrenWatch(MASTER_PATH)
        def watch_master(children):
            children = self.zookeeper.get_children('master')
            if children:
                self.master_address = self.convert_zookeeper_ip(self.zookeeper.get('master')[0])
                print()
                '\nMaster down - attempting to recover', self.master_address
            else:
                if ssh(self.master_address, MASTER):
                    print()
                    'Another master successfully started'
                else:
                    print()
                    'Could not recover master'

        @self.zookeeper.ChildrenWatch(CHUNKSERVER_PATH)
        def watch_chunkservers(children):
            if len(children) > len(self.chunkservers):
                print()
                "New chunkserver(s) detected"
                # This creates a watch function for each new chunk server, where the
                # master waits to register until the data(ip address) is updated
                new_chunkservers = [chunkserver_num for chunkserver_num in children
                                    if chunkserver_num not in self.chunkservers]
                for chunkserver_num in new_chunkservers:
                    try:
                        # zoo_ip = self.zookeeper.get(CHUNKSERVER_PATH + chunkserver_num,
                        #                             watch=watch_it)[0]
                        zoo_ip = self.zookeeper.get(CHUNKSERVER_PATH + chunkserver_num)[0]
                        print()
                        'zoo ip ', zoo_ip

                        # chunkserver_ip = self.convert_zookeeper_ip(
                        #     self.zookeeper.get(CHUNKSERVER_PATH + chunkserver_num)[0])
                        if not zoo_ip:
                            print()
                            'no ip yet, watching for it'
                            self.zookeeper.exists(CHUNKSERVER_PATH + chunkserver_num,
                                                  watch=watch_it)
                        else:
                            chunkserver_ip = self.convert_zookeeper_ip(zoo_ip)
                            self._register_chunkserver(chunkserver_num, chunkserver_ip)
                    except Exception as ex:
                        self.print_exception('watch children, adding chunkserver', ex)

            elif len(children) < len(self.chunkservers):

                try:
                    removed_servers = [chunkserver_num for chunkserver_num in self.chunkservers
                                       if chunkserver_num not in children]
                    for chunkserver_num in removed_servers:

                        if ssh(self.chunkservers[chunkserver_num], SERVER):
                            print()
                            "Another chunkserver to replace %s " % chunkserver_num
                        else:
                            print()
                            'Failed to recover from cs num %s failure' % chunkserver_num

                        self._unregister_chunkserver(chunkserver_num)

                except Exception as ex:
                    self.print_exception('Removing chunkserver', ex)
                finally:
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
            self.print_exception('register chunkserver', e)
        finally:
            self.lock.release()

    def _unregister_chunkserver(self, chunkserver_num):
        self.lock.acquire()
        try:
            del self.chunkservers[chunkserver_num]
        except Exception as e:
            self.print_exception('unregister chunkserver', e)
        finally:
            self.lock.release()

    @staticmethod
    def print_exception(context, exception, message=''):
        print()
        "Unexpected error in ", context, message
        if exception:
            print()
            type(exception).__name__, ': ', exception.args

    def get(self):
        print()
        self.chunkservers
        print()
        self.master_address

    @staticmethod
    def convert_zookeeper_ip(data):
        data = data.replace('tcp://', '')
        data = data[:data.rfind(':')]

        return data
