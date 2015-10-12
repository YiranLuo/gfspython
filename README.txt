Basic replication of gfs.py - read/write functionality with separation.  


Basic TODO:
Extend servers to multiple machines (zookeeper with registration)
add appending?
separate create file from write file 
Basic metadata updates before heartbeat implementation?

Advanced TODO:
Replication of files
Chunkserver register saved content with master (needs replication first, otherwise its garbage if the master doesn't know about it)
heartbeat metadata transfer 
Consistency model
Acquire lease
accurate data deletion and/or garbage collection




