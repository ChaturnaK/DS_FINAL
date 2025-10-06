#!/usr/bin/env bash
set -euo pipefail
mvn -q -DskipTests package
echo "Launching Stage 2 services..."
COMMON_CP="common/target/classes:common/target/*"
java -cp "metadata/target/classes:metadata/target/*:${COMMON_CP}" \
  com.ds.metadata.MetadataServer --port 7000 --zk localhost:2181 --replication 3 &
java -cp "storage/target/classes:storage/target/*:${COMMON_CP}" \
  com.ds.storage.StorageNode --port 8001 --data ./data/node1 --zone z1 --zk localhost:2181 &
java -cp "storage/target/classes:storage/target/*:${COMMON_CP}" \
  com.ds.storage.StorageNode --port 8002 --data ./data/node2 --zone z2 --zk localhost:2181 &
java -cp "storage/target/classes:storage/target/*:${COMMON_CP}" \
  com.ds.storage.StorageNode --port 8003 --data ./data/node3 --zone z3 --zk localhost:2181 &
wait
