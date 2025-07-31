docker compose up -d
docker compose ps

docker cp ./config/connector.json debezium-mysql-connector-debezium-1:/connector.json
docker exec -it debezium-mysql-connector-debezium-1 curl -X POST -H "Content-Type: application/json" --data @/connector.json http://YOUR_DEBEZIUM_HOST:8083/connectors
docker exec -it debezium-mysql-connector-debezium-1 curl http://YOUR_DEBEZIUM_HOST:8083/connectors/mysql-connector/status

