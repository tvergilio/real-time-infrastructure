# Real-Time Restaurant Relevance Calculator

This project is a Flink job that calculates the relevance of restaurants based on views and likes. 
It uses:
* **Kafka**, a distributed and highly scalable event streaming platform, to ingest the data.
* **Flink**, a framework and distributed processing engine for stateful computations over unbounded and bounded data streams, to process the data.
* **Zookeeper**, a centralised service for maintaining configuration information, naming, providing distributed synchronisation, and providing group services.
* **Docker** and **Docker Compose** to deploy the services required by the project.
* **Redis**, an in-memory data structure store, used as a cache to store the restaurant data.


## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Java 8 or higher
- Gradle
- Docker

### Installing

1. Clone the repository to your local machine.
2. Navigate to the project directory.
3. Run `gradle build` to build the project.

## Running the tests

The project uses JUnit for testing. Run `gradle test` to execute the tests.

## Deployment

The project uses Docker for easy deployment. The `docker-compose.yml` file contains the configuration for the services required by the project, including Zookeeper, Kafka, Redis, and Flink's JobManager and TaskManager.

To deploy the project, run `docker-compose up` in the project directory.

Then create the Kafka topics (see the Kafka section below).

## Code Overview

The main class of the project is `RestaurantRelevanceJob`. It sets up a Flink streaming job that reads restaurant view and like events from Kafka, calculates a relevance score for each restaurant, and writes the scores back to Kafka.

The job uses a sliding processing time window of 30 seconds with a slide of 5 seconds to calculate the relevance scores. The scores are calculated by the `RelevanceAggregate` and `RelevanceScoringFunction` classes.

The `RestaurantEventDeserializationSchema` class is a schema used to deserialise the Kafka messages into `RestaurantEvent` objects.

The `createKafkaSink` method creates a Kafka sink that writes the relevance scores back to Kafka.

## Authors

- [Thalita Vergilio](https://github.com/tvergilio)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

# Kafka

## Create topics (initial setup)

```bash
docker-compose exec kafka kafka-topics --create --topic restaurant_views --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092

docker-compose exec kafka kafka-topics --create --topic restaurant_likes --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092

docker-compose exec kafka kafka-topics --create --topic restaurant_relevance --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
```    

## Create producers using the console:
### Restaurant Views
```bash
docker exec -it {container id} kafka-console-producer --broker-list localhost:9092 --topic restaurant_views
```
### Restaurant Likes
```bash
docker exec -it {container id} kafka-console-producer --broker-list localhost:9092 --topic restaurant_likes
```
## Produce data:

```
{"restaurantId": "1", "eventType": "view", "timestamp": "2024-06-07T15:55:00Z"}
{"restaurantId": "1", "eventType": "like", "timestamp": "2024-06-07T15:55:00Z"}
```

## Create consumer using the console:

```bash
docker exec -it {container id} kafka-console-consumer --bootstrap-server localhost:9092 --topic restaurant_relevance --from-beginning
```

## List topics:

```bash
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:9092
```
