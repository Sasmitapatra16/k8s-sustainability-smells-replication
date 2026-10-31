#!/bin/bash
set -euo pipefail

TIMESTAMP=$(date +%Y%m%dT%H%M%S)
EXPERIMENT_DIR="$HOME/all7-experiment-v2"
RESULTS_DIR="$EXPERIMENT_DIR/runs/smoke_smelly_$TIMESTAMP"
MANIFESTS_DIR="$EXPERIMENT_DIR/manifests/smelly"
TMP_DIR="$EXPERIMENT_DIR/tmp_manifests"

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

collect_metrics() {
    local mode="$1"
    local run_id="$2"
    local out_file="$RESULTS_DIR/${mode}_${run_id}_metrics.jsonl"
    log "Collecting metrics: mode=$mode run_id=$run_id"
    microk8s kubectl run "metrics-pull-$(date +%s)" --rm -i --restart=Never \
        --image=10.2.0.145:32000/etl-pipeline:latest -- \
        python3 -c "
import boto3, json
s3 = boto3.client('s3', endpoint_url='http://localstack.default.svc.cluster.local:4566',
    aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
stages = ['ingestion', 'integration', 'cleaning', 'transformation', 'loading']
mode = '$mode'
run_id = '$run_id'
for stage in stages:
    key = f'metrics/{mode}/{run_id}/{stage}.json'
    try:
        body = s3.get_object(Bucket='staging-data', Key=key)['Body'].read().decode()
        print(body.replace(chr(10), ''))
    except Exception as e:
        print(json.dumps({'error': str(e), 'key': key}))
" > "$out_file" 2>/dev/null
    log "Metrics saved -> $out_file"
}

cleanup_jobs() {
    log "Cleaning up previous smelly jobs..."
    microk8s kubectl delete jobs --all -n etl-smelly --ignore-not-found=true 2>/dev/null || true
}

divider
log "SMOKE TEST - smelly pipeline only, single run"
divider

cleanup_jobs

RUN_ID_SMOKE="smoke-smelly-$TIMESTAMP"
JOBS_SMELLY=(stage-ingestion stage-integration stage-cleaning stage-transformation stage-loading)
FILES_SMELLY=(smelly-ingestion.yaml smelly-integration.yaml smelly-cleaning.yaml smelly-transformation.yaml smelly-loading.yaml)

TOTAL_START=$(date +%s%3N)

for i in "${!JOBS_SMELLY[@]}"; do
    job="${JOBS_SMELLY[$i]}"
    file="${FILES_SMELLY[$i]}"
    tmp_file="$TMP_DIR/${file}.run"

    log "Preparing $job with RUN_ID=$RUN_ID_SMOKE..."
    sed "s/value: \"smelly-001\"/value: \"$RUN_ID_SMOKE\"/" "$MANIFESTS_DIR/$file" > "$tmp_file"

    log "Applying $tmp_file..."
    microk8s kubectl apply -f "$tmp_file"

    wait_for_job "etl-smelly" "$job" 120
    log "  $job complete. Proceeding to next stage."
done

TOTAL_END=$(date +%s%3N)
ELAPSED=$(( TOTAL_END - TOTAL_START ))
log "SMOKE TEST total wall time: ${ELAPSED}ms"

collect_metrics "smelly" "$RUN_ID_SMOKE"
echo "$ELAPSED" > "$RESULTS_DIR/smoke_total_ms.txt"

divider
log "SMOKE TEST COMPLETE. Results in: $RESULTS_DIR"
log "RUN_ID used: $RUN_ID_SMOKE"
divider
