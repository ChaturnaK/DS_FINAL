#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Start ZooKeeper if available and wait until it's up
echo "Starting ZooKeeper (if available)..."
if [[ -x "$ROOT_DIR/scripts/start-zk.sh" ]]; then
  "$ROOT_DIR/scripts/start-zk.sh" || true
else
  echo "Hint: ZooKeeper not auto-started. Ensure localhost:2181 is up."
fi

echo -n "Waiting for ZooKeeper on localhost:2181"
for i in {1..30}; do
  if nc -z localhost 2181 >/dev/null 2>&1; then
    echo " - ready"
    break
  fi
  echo -n "."
  sleep 1
  if [[ "$i" -eq 30 ]]; then
    echo "\nZooKeeper did not become ready. Proceeding anyway..."
  fi
done

# Build all modules
mvn -q -DskipTests clean package

echo "Launching Stage 2 services..."
COMMON_CP="common/target/classes:common/target/*"

# Pick metadata port: prefer 7000, fallback to 7005 if busy
META_PORT=7000
if nc -z localhost "$META_PORT" >/dev/null 2>&1; then
  META_PORT=7005
fi
echo "Using MetadataServer port: $META_PORT"

java -cp "metadata/target/classes:metadata/target/*:${COMMON_CP}" \
  com.ds.metadata.MetadataServer --port "$META_PORT" --zk localhost:2181 --replication 3 &

java -cp "storage/target/classes:storage/target/*:${COMMON_CP}" \
  com.ds.storage.StorageNode --port 8001 --data ./data/node1 --zone z1 --zk localhost:2181 &
java -cp "storage/target/classes:storage/target/*:${COMMON_CP}" \
  com.ds.storage.StorageNode --port 8002 --data ./data/node2 --zone z2 --zk localhost:2181 &
java -cp "storage/target/classes:storage/target/*:${COMMON_CP}" \
  com.ds.storage.StorageNode --port 8003 --data ./data/node3 --zone z3 --zk localhost:2181 &

wait
