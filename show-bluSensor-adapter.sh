#!/usr/bin/env bash
echo "Printing 'docker service ls | grep add-bluSensor':"
docker service ls | grep add-bluSensor
echo ""
echo "Printing 'docker stack ps add-bluSensor':"
docker stack ps add-bluSensor
