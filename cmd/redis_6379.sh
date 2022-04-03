#!/bin/sh
redis-cli -p 6379 shutdown
redis-server /usr/local/etc/redis.conf