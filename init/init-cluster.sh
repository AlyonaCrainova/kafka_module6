#!/bin/bash
set -e

BOOTSTRAP=kafka-0:9093

cat > /tmp/admin.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";
ssl.truststore.location=/certs/truststore.jks
ssl.truststore.password=551288d
EOF

until kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties > /dev/null 2>&1; do
  echo "Ждем кластер..."
  sleep 3
done

echo "Создание топиков..."
kafka-topics --create --topic topic-1 --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --partitions 3 --replication-factor 2 --if-not-exists
kafka-topics --create --topic topic-2 --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --partitions 3 --replication-factor 2 --if-not-exists

echo "Конфигурация ACLs..."
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:consumer --operation Read --group my-consumer-group-1
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:consumer --operation Describe --topic topic-1
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:consumer --operation Read --topic topic-1

kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:producer --operation Describe --topic topic-1
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:producer --operation Write --topic topic-1

kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:producer --operation Describe --topic topic-2
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:producer --operation Write --topic topic-2

echo "Инициализация кластера завершена."
