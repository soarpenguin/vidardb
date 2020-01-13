#!/bin/bash
docker run -v $PWD:/vidardb -w /vidardb buildpack-deps make
