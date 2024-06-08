# Kafka 

## Create producer using the console:
`docker exec -it {container id} kafka-console-producer --broker-list localhost:9092 --topic restaurant_views`

## Produce data:
{"restaurantId": "1", "eventType": "view", "timestamp": "2024-06-07T15:55:00Z"}

## Create consumer using the console:
`docker exec -it {container id} kafka-console-consumer --bootstrap-server localhost:9092 --topic restaurant_relevance --from-beginning`

## List topics:
`docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:9092`
