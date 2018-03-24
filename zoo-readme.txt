****************************************************
 **
 download zookeeper, tested with version 3.4.6.
 add zoo.cfg to zookeeper-3.4.6/conf/
 first time: may need to set execution permission by chmod u+x zookeeper-3.4.6/bin/*
start server with zookeeper-3.4.6/bin/zkServer.sh start
stop with zookeeper-3.4.6/bin/zkServer.sh start

To check data in python shell
	- import zutils
	  zk = zutils.get_zk()
	  
	  some possible examples
	  zk.get(path)
	  zk.get_children(path)
	  
	  zk.create("/files/filename/read") # create a read lease?
	  zk.create("/files/filename/write") # create a write lease?
**************************************************************
	  
	  To check leases, zk.get_children("/files/filename") to see if any read/write lease exists, write lease with locking file.  
	  
Supports transactions with ZooKeeper 3.4 and above.  
	transaction = zk.transaction()
	transaction.check('/node/a', version=3)
	transaction.create('/node/b', b"a value")
	results = transaction.commit()
