Getting ZooKeeper work

1) Download stable release http://zookeeper.apache.org/releases.html#download 
1.b) pip install kazoo for the python kazoo client package 
2) Extract archive
3) rename conf/zoo_sample.cfg to zoo.cfg 
	- config has default heartbeat time of 2000 miliseconds (min session timeout is 2x heartbeat)
	- default port 2181 
	- data directory /tmp/zookeeper
	
4) start server with bin/zkServer.sh start
5) in python shell, can follow https://kazoo.readthedocs.org/en/latest/basic_usage.html for kazoo client
	- from kazoo.client import KazooClient
	  zk = KazooClient() # defaults to localhost:2181
	  
	  some possible examples
	  zk.create("/chunkservers/0/ip", "optional data value(ip address)") 
	  zk.create("/master/ip", "ip address") 
	  data, stats = zk.get("/master/ip") # data contains ip address, or whatever data is stored at that node
	  
	  zk.create("/files/filename/read") # create a read lease?
	  zk.create("/files/filename/write") # create a write lease?
	  
	  To check leases, zk.get_children("/files/filename") to see if any read/write lease exists
	  
	  You can lock a node, not sure how useful that will be, because it doesn't seem to stop
	  another client from changing the locked node's data, or creating children from that node. 
	  
Supports transactions with ZooKeeper 3.4 and above.  
	transaction = zk.transaction()
	transaction.check('/node/a', version=3)
	transaction.create('/node/b', b"a value")
	results = transaction.commit()
