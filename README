# Osley File System

Osley File System is a basic implementation of [Google File System](http://static.googleusercontent.com/media/research.google.com/es//archive/gfs-sosp2003.pdf) written for a Distributed Systems class project.  GFS is a high availability scalable distributed file system with fault tolerance, replication, maintains consistency, and strong performance for large-scale data processing on commodity hardware.  

Osley implements the main features of GFS, including:

  - Supports create, delete, open, close, read, write operations on files
  - Monitors self to detect, tolerate, and recover from component failures
  - Single master/chunkserver model 
  - Shadow master monitors network status, which brings up new chunkservers when failure is detected
  - Shadow master becomes new master upon detecting master failure.  New shadow master is created.
  - Requires master and shadow master to simultaneously fail to create unrecoverable state.
  - Files are chunked and these chunks stored in random chunkservers based on load balancing and use (files are not stored contiguously) 
  - Triple replication of files handles 2 simultaneous chunkserver failures without loss of data.
  - Chunkservers register with a central location (zookeeper), query for other chunkservers, and report any local chunks, updating if necessary based on version number
  - Versioning ensures stale chunks get updated 
  - Reasonable write (~20-30 MB/s) and read (~50-60 MB/s) speeds tested on local network with 10 chunkservers and 3-5 clients. 
  - Garbage collection is simplified, files are flagged and garbage collection is run every 5-30 minutes as set by user. 
  - Chunk status is verified by hashing when reading/writing to ensure data integrity
  - Master and chunkserver take only seconds to come online, regsiter with the network, and begin receiving/transmitting necessary files and metadata. 
  
**The integrity of the ZooKeeper system is assumed.  The system has no way to recover from ZooKeeper failure.  
 
# Architecture
Single master oversees multiple chunkservers.  A shadow master is responsible for regenerating failed chunkservers that have not been properly unregistered.  The shadow master will become the new master in the event of a master failure, and will spawn a new shadow master. 

The master contains metadata such as file to chunk mappings, chunk locations on chunkservers, read/write locks through Zookeeper.  Master uses heartbeat messages to ensure chunkservers are constantly connected and status is up to date. 

To perform some operation, the client sends requests to the master.  The master is responsible for returning metadata, such as a list of files currently in the file system.  However, when a client wishes to read/write to a file, the master sends instructions to the client about which chunkservers the client must communicate with.  These chunkservers are chosen randomly with some weight towards capacity (i.e., chunkservers with fewer files will be more likely to have new chunks added).  

The client then communicates directly with each chunkserver, requesting a particular chunk or writing a particular chunk.  The master does not engage in read/write operations, which ensures that a single master does not create an I/O bottleneck.  

Zookeeper maintains location status of master, shadow master, and chunkservers through ephemeral files.  Shadow master monitors these files to instantly bring up another chunkserver or master if failure is detected.  Master monitors shadow master status in a similar way.  

Master contains a repository of metadata, but is not the single source.  When a new master comes online, it queries all chunkservers for the list of chunks they contain, the file-to-chunk mappings for those chunks, and chunk versions.  

The main difference between GFS and OFS is the amount of logging, snapshotting, and checkpointing the system does.  The metadata contained for each file is minimal.  File operations are not journaled; instead, we opted for a locking solution with transactions through zookeeper.  Transaction failure reverts any changes and fails loudly.  

---

1. supply zookeeper with zoo.cfg 
2. modify zookeeper IP for master/clients 
3. start zookeeper with script zkServer.sh start
4. Start single master and several chunkservers using provided scripts
5. Start shadow master using create_watcher


create_master.py [optional port] to create master on port 1400 or specified port number
create_server.py [optional ip] to connect to a master on localhost by default or to specified ip address

ZClient is used to access the file system from within a python interactive session.  

```python
import zclient
ZOO_IP = ???
PORT = ????
client = zclient.ZClient(zoo_ip=ZOO_IP, port=PORT)

# some supported actions
client.list()
client.read(filename=???)
client.read_gui(filename=???)
client.append(filename=???, data=???)
client.delete(filename=???)
client.rename(filename=???, newfilename=???)
client.write(filename=???, data=???) # writing to a file that currently exists will simply edit the necessary chunks and may overwrite
client.write(filename=???, data='') # creates a new blank file.  Data may also be supplied upon creation 
client.append(filename=???, data=???) 
client.close()  # closes connection with master
```

TODO: 
incorporate click to move all functionality from python session to the command line.  
Fix issues related to piping in data for write/append and piping out read data

