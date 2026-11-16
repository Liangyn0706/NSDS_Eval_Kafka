#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION=4.4.0
LOCAL="$ROOT/.local"
KAFKA="$LOCAL/kafka_2.13-$VERSION"
CONFIG="$LOCAL/server.properties"
PID_FILE="$LOCAL/kafka.pid"
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256m -Xmx512m}"
export LOG_DIR="$LOCAL/logs"
mkdir -p "$LOCAL" "$LOG_DIR"

install_kafka() {
    command -v java >/dev/null || { echo 'Installare Java 17 o successivo.' >&2; exit 1; }
    if [[ ! -x "$KAFKA/bin/kafka-server-start.sh" ]]; then
        local archive="kafka_2.13-$VERSION.tgz"
        local downloads="$LOCAL/kafka-downloads"
        mkdir -p "$downloads"
        if [[ ! -f "$downloads/$archive" || ! -f "$downloads/$archive.sha512" ]]; then
            curl -fL --retry 2 "https://downloads.apache.org/kafka/$VERSION/$archive" -o "$downloads/$archive"
            curl -fL --retry 2 "https://downloads.apache.org/kafka/$VERSION/$archive.sha512" -o "$downloads/$archive.sha512"
        fi
        local expected actual
        expected="$(sed '1s/^[^:]*://' "$downloads/$archive.sha512" | tr '[:upper:]' '[:lower:]' | tr -cd '[:xdigit:]')"
        actual="$(shasum -a 512 "$downloads/$archive" | awk '{print $1}')"
        if [[ "$expected" != "$actual" ]]; then
            echo 'Checksum Kafka non valido: eliminare i file in .local/kafka-downloads e riprovare.' >&2
            exit 1
        fi
        tar -xzf "$downloads/$archive" -C "$LOCAL"
    fi
}

ready() {
    "$KAFKA/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1
}

start() {
    install_kafka
    command -v python3 >/dev/null || { echo 'Installare Python 3 per avviare il broker in background.' >&2; exit 1; }
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        echo 'Kafka locale già avviato.'
    else
        if lsof -nP -iTCP:9092 -sTCP:LISTEN >/dev/null 2>&1; then
            echo 'Porta 9092 occupata da un altro processo: liberarla prima di avviare Kafka locale.' >&2
            exit 1
        fi
        cat "$ROOT/config/kafka/server.properties" > "$CONFIG"
        printf '\nlog.dirs=%s/data\n' "$LOCAL" >> "$CONFIG"
        if [[ ! -f "$LOCAL/data/meta.properties" ]]; then
            local cluster_id
            cluster_id="$("$KAFKA/bin/kafka-storage.sh" random-uuid)"
            "$KAFKA/bin/kafka-storage.sh" format --standalone -t "$cluster_id" -c "$CONFIG"
        fi
        # A separate process session survives completion of the IDE's launch task.
        python3 - "$KAFKA" "$CONFIG" "$PID_FILE" "$LOG_DIR" <<'PY'
import os
import pathlib
import subprocess
import sys
kafka, config, pid_file, log_dir = sys.argv[1:]
with open(pathlib.Path(log_dir) / 'server.out', 'w') as log:
    process = subprocess.Popen(
        ['bash', kafka + '/bin/kafka-server-start.sh', config],
        stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
        start_new_session=True, env=os.environ.copy())
pathlib.Path(pid_file).write_text(str(process.pid) + '\n')
PY
    fi
    local attempt
    for attempt in {1..30}; do
        if lsof -nP -iTCP:9092 -sTCP:LISTEN >/dev/null 2>&1 && ready; then
            for topic in temperature humidity alerts processedAlerts; do
                local partitions=1
                case "$topic" in temperature|humidity) partitions=4 ;; esac
                "$KAFKA/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists \
                    --topic "$topic" --partitions "$partitions" --replication-factor 1
            done
            echo 'Kafka pronto su localhost:9092.'
            return
        fi
        if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
            tail -n 40 "$LOG_DIR/server.out" >&2
            exit 1
        fi
        sleep 1
    done
    echo 'Kafka non pronto. Consultare .local/logs/server.out.' >&2
    exit 1
}

case "${1:-start}" in
    install) install_kafka ;;
    start) start ;;
    stop)
        if [[ -f "$PID_FILE" ]]; then
            pid="$(cat "$PID_FILE")"
            if kill -0 "$pid" 2>/dev/null; then
                if ps -p "$pid" -o command= | grep -F -- "$CONFIG" >/dev/null; then
                    kill "$pid"
                    for attempt in {1..30}; do
                        kill -0 "$pid" 2>/dev/null || break
                        sleep 1
                    done
                    if kill -0 "$pid" 2>/dev/null; then
                        echo 'Kafka ancora in arresto; riprovare tra qualche secondo.' >&2
                        exit 1
                    fi
                else
                    echo 'PID non appartenente al broker del progetto; arresto annullato.' >&2
                    exit 1
                fi
            fi
            rm -f "$PID_FILE"
        fi
        echo 'Kafka locale arrestato (dati conservati).'
        ;;
    status)
        install_kafka
        if ! lsof -nP -iTCP:9092 -sTCP:LISTEN >/dev/null 2>&1; then
            echo 'Kafka non avviato. Eseguire: bash scripts/kafka-local.sh start' >&2
            exit 1
        fi
        "$KAFKA/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --describe
        ;;
    *) echo 'Uso: scripts/kafka-local.sh [install|start|stop|status]' >&2; exit 2 ;;
esac

