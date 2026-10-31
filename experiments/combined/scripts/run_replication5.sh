#!/bin/bash
set -euo pipefail

TIMESTAMP=$(date +%Y%m%dT%H%M%S)
EXPERIMENT_DIR="$HOME/all7-experiment-v2"
RESULTS_DIR="$EXPERIMENT_DIR/runs/replication_$TIMESTAMP"
SMELLY_MANIFESTS="$EXPERIMENT_DIR/manifests/smelly"
REFACTORED_MANIFESTS="$EXPERIMENT_DIR/manifests/refactored"
TMP_DIR="$EXPERIMENT_DIR/tmp_manifests"
N_RUNS=5

mkdir -p "$RESULTS_DIR" "$TMP_DIR"

log() { echo "[$(date +%H:%M:%S)] $*"; }
divider() { echo "----------------------------------------"; }

wait_for_job() {
    local ns="$1"
    local job="$2"
    local timeout="${3:-300}"
    log "  Waiting for job/$job in $ns to complete (timeout=${timeout}s)..."
    microk8s kubectl wait --for=condition=complete job/"$job" -n "$ns" --timeout="${timeout}s"
    microk8s kubectl get job "$job" -n "$ns" -o wide
}

# NOTE: stage list order corrected to match stage_unified.py fix:
#   ingestion -> cleaning -> integration -> transformation -> loading
collect_metrics_smelly() {
    local run_id="$1"
    local out_file="$2"
    microk8s kubectl run "metrics-pull-$(date +%s)-$RANDOM" --rm -i --restart=Never \
        --image=10.2.0.145:32000/etl-pipeline:latest -- \
        python3 -c "
import boto3, json
s3 = boto3.client('s3', endpoint_url='http://localstack.default.svc.cluster.local:4566',
    aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
stages = ['ingestion', 'cleaning', 'integration', 'transformation', 'loading']
run_id = '$run_id'
for stage in stages:
    key = f'metrics/smelly/{run_id}/{stage}.json'
    try:
        body = s3.get_object(Bucket='staging-data', Key=key)['Body'].read().decode()
        print(body.replace(chr(10), ''))
    except Exception as e:
        print(json.dumps({'error': str(e), 'key': key}))
" 2>/dev/null | grep '^{' > "$out_file"
}

collect_metrics_refactored() {
    local run_id="$1"
    local out_file="$2"
    microk8s kubectl run "metrics-pull-$(date +%s)-$RANDOM" --rm -i --restart=Never \
        --image=10.2.0.145:32000/etl-pipeline:latest -- \
        python3 -c "
import boto3, json
s3 = boto3.client('s3', endpoint_url='http://localstack.default.svc.cluster.local:4566',
    aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
stages = ['ingestion', 'cleaning', 'integration', 'transformation', 'loading']
run_id = '$run_id'
for stage in stages:
    for ci in range(3):
        key = f'metrics/refactored/{run_id}/{stage}_chunk{ci}.json'
        try:
            body = s3.get_object(Bucket='staging-data', Key=key)['Body'].read().decode()
            print(body.replace(chr(10), ''))
        except Exception as e:
            print(json.dumps({'error': str(e), 'key': key}))
" 2>/dev/null | grep '^{' > "$out_file"
}

cleanup_smelly() {
    microk8s kubectl delete jobs --all -n etl-smelly --ignore-not-found=true 2>/dev/null || true
}

cleanup_refactored() {
    for ns in etl-ingestion etl-cleaning etl-integration etl-transformation etl-loading; do
        microk8s kubectl delete jobs --all -n "$ns" --ignore-not-found=true 2>/dev/null || true
    done
}

run_smelly_once() {
    local run_id="$1"
    log "=== SMELLY $run_id starting ==="
    cleanup_smelly
    local jobs=(stage-ingestion stage-cleaning stage-integration stage-transformation stage-loading)
    local files=(smelly-ingestion.yaml smelly-cleaning.yaml smelly-integration.yaml smelly-transformation.yaml smelly-loading.yaml)
    local start_ms=$(date +%s%3N)
    for i in "${!jobs[@]}"; do
        local job="${jobs[$i]}"
        local file="${files[$i]}"
        local tmp_file="$TMP_DIR/${file}.run"
        sed "s/value: \"smelly-001\"/value: \"$run_id\"/" "$SMELLY_MANIFESTS/$file" > "$tmp_file"
        microk8s kubectl apply -f "$tmp_file" > /dev/null
        wait_for_job "etl-smelly" "$job" 120 > /dev/null
    microk8s kubectl get pods -n etl-smelly -o wide --no-headers >> "$RESULTS_DIR/placement_${run_id}.txt"
        log "  $job done"
    done
    local end_ms=$(date +%s%3N)
    local elapsed=$(( end_ms - start_ms ))
    log "=== SMELLY $run_id complete: ${elapsed}ms ==="
    echo "$elapsed" > "$RESULTS_DIR/${run_id}_total_ms.txt"
    collect_metrics_smelly "$run_id" "$RESULTS_DIR/${run_id}_metrics.jsonl"
}

run_refactored_once() {
    local run_id="$1"
    log "=== REFACTORED $run_id starting ==="
    cleanup_refactored
    local ns_list=(etl-ingestion etl-cleaning etl-integration etl-transformation etl-loading)
    local jobs=(stage-ingestion stage-cleaning stage-integration stage-transformation stage-loading)
    local files=(refactored-ingestion.yaml refactored-cleaning.yaml refactored-integration.yaml refactored-transformation.yaml refactored-loading.yaml)
    local start_ms=$(date +%s%3N)
    for i in "${!ns_list[@]}"; do
        local ns="${ns_list[$i]}"
        local job="${jobs[$i]}"
        local file="${files[$i]}"
        local tmp_file="$TMP_DIR/${file}.run"
        sed "s/value: \"refactored-001\"/value: \"$run_id\"/" "$REFACTORED_MANIFESTS/$file" > "$tmp_file"
        microk8s kubectl apply -f "$tmp_file" > /dev/null
        wait_for_job "$ns" "$job" 300 > /dev/null
    microk8s kubectl get pods -n "$ns" -o wide --no-headers >> "$RESULTS_DIR/placement_${run_id}.txt"
        log "  $job done"
    done
    local end_ms=$(date +%s%3N)
    local elapsed=$(( end_ms - start_ms ))
    log "=== REFACTORED $run_id complete: ${elapsed}ms ==="
    echo "$elapsed" > "$RESULTS_DIR/${run_id}_total_ms.txt"
    collect_metrics_refactored "$run_id" "$RESULTS_DIR/${run_id}_metrics.jsonl"
}

divider
log "REPLICATION START: $N_RUNS runs each, interleaved"
divider

for n in $(seq 1 $N_RUNS); do
    divider
    log "ITERATION $n of $N_RUNS"
    divider
    run_smelly_once "smelly-run$n"
    run_refactored_once "refactored-run$n"
done

divider
log "REPLICATION COMPLETE. Results in: $RESULTS_DIR"
ls -la "$RESULTS_DIR"
divider
