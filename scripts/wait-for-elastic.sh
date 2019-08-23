#!/bin/sh

echo -n 'wait for elastic.'
while ! curl -sfo /dev/null 'http://localhost:9200/'; do echo -n '.' && sleep 1; done
echo 'done.'

