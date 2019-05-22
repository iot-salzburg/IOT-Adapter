#!/usr/bin/env bash
echo "Printing 'docker service ls | grep mqtt':"
docker service ls | grep mqtt
echo ""
echo "Printing 'docker stack ps mqtt':"
docker stack ps mqtt

