#!/bin/bash

set -ex

wget https://downloads.datastax.com/labs/kafka-connect-dse.tar.gz

tar xzf kafka-connect-dse.tar.gz

wget http://apache-mirror.8birdsvideo.com/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz

tar xzf spark-2.4.4-bin-hadoop2.7.tgz