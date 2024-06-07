# Kafka 

## Create producer using the console:
`docker-compose exec kafka kafka-console-producer --broker-list localhost:9092 --topic restaurant_views`

## Produce data:
{"restaurantId": "1", "eventType": "view", "timestamp": "2024-06-07T15:55:00Z"}

## Create consumer using the console:
`docker exec -it 8c1b3ebe55de kafka-console-consumer --bootstrap-server localhost:9092 --topic restaurant_relevance --from-beginning`
