#!/usr/bin/env bash
echo "Printing 'docker service ls | grep add-mqtt':"
docker service ls | grep add-mqtt
echo ""
echo "Printing 'docker stack ps add-mqtt':"
docker stack ps add-mqtt
