#!/bin/bash

docker-compose down
docker-compose build

echo "Starting Flink. See http://localhost:8082/#/overview"
docker-compose up -d flink-jm
docker-compose up -d flink-tm

echo "Starting ElasticSearch. See http://localhost:9200/_cluster/health?pretty"
docker-compose up -d elasticsearch

echo "Starting Kibana. See http://localhost:5601"
docker-compose up -d kibana
