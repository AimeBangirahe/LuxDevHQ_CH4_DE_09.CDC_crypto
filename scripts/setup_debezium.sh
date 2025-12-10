#!/bin/bash

echo "Waiting for Debezium Connect to be ready..."
for i in {1..30}; do
  if curl -f http://localhost:8083/ > /dev/null 2>&1; then
    echo "Debezium Connect is ready!"
    break
  fi
  echo "Attempt $i/30: Waiting..."
  sleep 2
done

echo "Registering Postgres connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @config/debezium-connector.json

echo ""
echo "Connector registered!"
echo "Check status: curl http://localhost:8083/connectors/crypto-postgres-connector/status"
