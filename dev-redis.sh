#!/bin/sh

cd "$(dirname "$0")"

docker network create redis-state-management || true

docker run --init --rm \
-p 6379:6379 \
--network redis-state-management \
--name redis-state-management-redis \
-it redis:7
