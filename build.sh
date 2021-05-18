#!/bin/bash
git pull
docker rmi localhost/datacopy
docker build --squash -t datacopy:latest .
