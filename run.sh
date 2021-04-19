#!/bin/bash
docker run --rm --mount type=bind,source="$(pwd)",target=/app -it bdc:latest $*
