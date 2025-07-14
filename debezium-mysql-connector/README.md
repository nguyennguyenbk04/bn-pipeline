# Debezium MySQL Connector Quick Start

## 1. Start MySQL Server
```bash
sudo systemctl start mysql
```
Verify MySQL is running and listening on port 3306:
```bash
sudo ss -tuln | grep 3306
```

## 2. Start Docker Compose Services
Navigate to your project directory:
```bash
cd ~/Desktop/DE_project/debezium-mysql-connector
docker compose up -d
```
Check that the Debezium, Kafka, and Zookeeper containers are running:
```bash
docker compose ps
```

## 3. Ensure MySQL User Privileges
Log in to MySQL:
```bash
mysql -u root -p
```
Run these SQL commands:
```sql
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '.Tldccmcbtldck2';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
exit
```

## 4. Register the Debezium Connector
Copy your connector config into the Debezium container:
```bash
docker cp ./config/connector.json debezium-mysql-connector-debezium-1:/connector.json
```
Register the connector:
```bash
docker exec -it debezium-mysql-connector-debezium-1 curl -X POST -H "Content-Type: application/json" --data @/connector.json http://localhost:8083/connectors
```

## 5. Restart the Connector (if needed)
```bash
docker exec -it debezium-mysql-connector-debezium-1 curl -X POST http://localhost:8083/connectors/mysql-connector/restart
```

## Delete connector
```bash
docker exec -it debezium-mysql-connector-debezium-1 curl -X DELETE http://localhost:8083/connectors/mysql-connector
```

## 6. Check Connector Status
```bash
docker exec -it debezium-mysql-connector-debezium-1 curl http://localhost:8083/connectors/mysql-connector/status
```

## 7. Changes
## 7. View All Database Changes

To see changes for all tables, use these commands (one per table):

```bash
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.Addresses
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.CartItems
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.Customers
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.Inventory
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.OrderItems
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.OrderStatus
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.Orders
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.PaymentMethods
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.Payments
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.ProductCategories
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.Products
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.Reasons
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.Reviews
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.Sellers
docker exec -it debezium-mysql-connector-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic online_store.online_store.ShoppingCarts
```

Or use **Kafka UI** at [http://localhost:8081](http://localhost:8081) to browse all topics
---

### Troubleshooting

- Check MySQL logs:  
  ```bash
  sudo tail -n 50 /var/log/mysql/error.log
  ```
- Check Docker logs:  
  ```bash
  docker compose logs debezium
  ```
- Ensure firewall allows port 3306:  
  ```bash
  sudo ufw allow