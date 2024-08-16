kafka-topics --bootstrap-server localhost:9092 --delete --topic work-order
kafka-topics --bootstrap-server localhost:9092 --topic work-order --create --partitions 3 --replication-factor 1