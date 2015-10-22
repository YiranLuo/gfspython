Getting ZooKeeper working 

1) Download stable release http://zookeeper.apache.org/releases.html#download 
2) Extract archive
3) rename conf/zoo_sample.cfg to zoo.cfg 
	- config has default heartbeat time of 2000 miliseconds (min session timeout is 2x heartbeat)
	- default port 2181 
	- data directory /var/zookeeper
	
4) start server with bin/zkServer.sh start
5) in python shell, can follow https://kazoo.readthedocs.org/en/latest/basic_usage.html for kazoo client
	- from kazoo.client import KazooClient
	  zk = KazooClient() # defaults to localhost:2181
	  
Can create nodes, set data, get children, watch for data/children changes.  Supports transactions with ZooKeeper 3.4 and above.  

