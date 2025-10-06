@echo off
mvn -q -DskipTests package
echo Launching Stage 0 placeholders...
start java -cp metadata\target\* com.ds.metadata.MetadataServer --port 7000
start java -cp storage\target\* com.ds.storage.StorageNode --port 8001 --data .\data\node1
start java -cp storage\target\* com.ds.storage.StorageNode --port 8002 --data .\data\node2
start java -cp storage\target\* com.ds.storage.StorageNode --port 8003 --data .\data\node3
pause
