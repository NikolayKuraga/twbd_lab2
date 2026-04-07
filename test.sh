#!/bin/sh
set -eu


### PREPARE FOR THE SHOW.

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$SCRIPT_DIR"

DATA_DIR="./data"
LOG_DIR="./logs"
CSV_PATH="${DATA_DIR}/online_retail_II.csv"
XLSX_PATH="${DATA_DIR}/online_retail_II.xlsx"
DATASET_XLSX_URL="https://archive.ics.uci.edu/ml/machine-learning-databases/00502/online_retail_II.xlsx"

MODE="${1:-all}"

mkdir -p "$DATA_DIR" "$LOG_DIR"
chmod 777 "$LOG_DIR" 2>/dev/null || true

if [ ! -f "$CSV_PATH" ]; then
    curl -fL "$DATASET_XLSX_URL" -o "$XLSX_PATH"
    python3 "./scripts/preprocess_dataset.py" "$XLSX_PATH" "$CSV_PATH"
fi


### DEFINITE FUN.

run_case() {
    COMPOSE_FILE="$1"
    RUN_LABEL="$2"
    APP_SCRIPT_PATH="$3"

    echo "Starting Docker services"
    docker compose -f "$COMPOSE_FILE" up -d

    echo "Waiting for Docker services"
    for service in $(docker compose -f "$COMPOSE_FILE" ps --services)
    do
        while :
        do
            container_id=$(docker compose -f "$COMPOSE_FILE" ps -q "$service" 2>/dev/null || true)
            if [ -z "$container_id" ]; then
                sleep 2
                continue
            fi

            state=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id" 2>/dev/null || true)
            if [ "$state" = "healthy" ] || [ "$state" = "running" ]; then
                break
            fi
            sleep 2
        done
    done

    echo "Uploading CSV to HDFS"
    docker compose -f "$COMPOSE_FILE" exec -T namenode hdfs dfs -mkdir -p /data/online-retail
    docker compose -f "$COMPOSE_FILE" exec -T namenode hdfs dfs -put -f /workspace/data/online_retail_II.csv /data/online-retail/
    docker compose -f "$COMPOSE_FILE" exec -T namenode hdfs dfs -ls /data/online-retail

    echo "Starting Spark app"
    docker compose -f "$COMPOSE_FILE" exec -T spark-master spark-submit \
        --master spark://spark-master:7077 \
        "/workspace/${APP_SCRIPT_PATH}" \
        --run-label "$RUN_LABEL" &
    SPARK_PID=$!

    echo "All docker is started."
    echo "See in browser:"
    echo "    HDFS UI:  http://127.0.0.1:9870"
    echo "    Spark UI: http://127.0.0.1:8080"
    echo "    App UI:   http://127.0.0.1:4040"
    echo "Spark App is running. UI will be available until the Spark job is done."

    wait "$SPARK_PID"

    echo "Spark App is done."
    echo "To stop containers and continue, press <Enter>."
    read dummy

    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
    rm -rf "$XLSX_PATH" "./scripts/__pycache__"
    echo "Test finished. Environment cleaned."
}


### BLOODY ACTION.

if [ "$MODE" = "1DataNode" ]; then
    run_case "./docker-compose_1DataNode.yml" "1DataNode" "./scripts/spark_run.py"

elif [ "$MODE" = "1DataNode_opt" ]; then
    run_case "./docker-compose_1DataNode.yml" "1DataNode_opt" "./scripts/spark_run_opt.py"

elif [ "$MODE" = "3DataNode" ]; then
    run_case "./docker-compose_3DataNode.yml" "3DataNode" "./scripts/spark_run.py"

elif [ "$MODE" = "3DataNode_opt" ]; then
    run_case "./docker-compose_3DataNode.yml" "3DataNode_opt" "./scripts/spark_run_opt.py"

elif [ "$MODE" = "3DataNode_2Workers" ]; then
    run_case "./docker-compose_3DataNode_2Workers.yml" "3DataNode_2Workers" "./scripts/spark_run.py"

elif [ "$MODE" = "3DataNode_2Workers_opt" ]; then
    run_case "./docker-compose_3DataNode_2Workers.yml" "3DataNode_2Workers_opt" "./scripts/spark_run_opt.py"

elif [ "$MODE" = "all" ]; then
    run_case "./docker-compose_1DataNode.yml" "1DataNode" "./scripts/spark_run.py"
    run_case "./docker-compose_1DataNode.yml" "1DataNode_opt" "./scripts/spark_run_opt.py"
    run_case "./docker-compose_3DataNode.yml" "3DataNode" "./scripts/spark_run.py"
    run_case "./docker-compose_3DataNode.yml" "3DataNode_opt" "./scripts/spark_run_opt.py"
    run_case "./docker-compose_3DataNode_2Workers.yml" "3DataNode_2Workers" "./scripts/spark_run.py"
    run_case "./docker-compose_3DataNode_2Workers.yml" "3DataNode_2Workers_opt" "./scripts/spark_run_opt.py"
    echo "All experiments are finished."
    echo "Logs and metrics are in $LOG_DIR"

elif [ "$MODE" = "draw" ]; then
    echo "Gonna run no tests, just draw"

else
    echo "Unknown mode: $MODE"
    exit 1
fi


### PASTRY DRAWING.

echo "Building graphs from logs"
python3 "./scripts/process_logs.py"

exit 0
