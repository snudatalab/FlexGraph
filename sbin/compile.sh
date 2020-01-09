#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
ROOT=$DIR/..
JAR_FILE=$ROOT/target/flexgraph-1.0.0-jar-with-dependencies.jar

if [ ! -f $JAR_FILE ]; then
    cd $ROOT
    mvn clean compile assembly:single
fi
