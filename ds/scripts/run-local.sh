#!/usr/bin/env bash
set -e
mvn -q -DskipTests package
echo "Launching Stage 1 gRPC placeholders..."
COMMON_CP="common/target/classes:common/target/*"
java -cp "metadata/target/classes:metadata/target/*:${COMMON_CP}" com.ds.metadata.MetadataServer --port 7000 &
java -cp "storage/target/classes:storage/target/*:${COMMON_CP}" com.ds.storage.StorageNode --port 8001 --data ./data/node1 &
java -cp "storage/target/classes:storage/target/*:${COMMON_CP}" com.ds.storage.StorageNode --port 8002 --data ./data/node2 &
java -cp "storage/target/classes:storage/target/*:${COMMON_CP}" com.ds.storage.StorageNode --port 8003 --data ./data/node3 &
wait
