#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR && docker-compose -f docker-compose.nocode.yml pull && docker-compose -f docker-compose.nocode.yml up
