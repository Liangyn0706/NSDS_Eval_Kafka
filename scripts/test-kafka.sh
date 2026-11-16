#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
bash scripts/kafka-local.sh start
mvn -q package dependency:build-classpath -Dmdep.outputFile=target/runtime-classpath.txt
java --class-path "target/classes:$(cat target/runtime-classpath.txt)" scripts/KafkaSmokeTest.java

