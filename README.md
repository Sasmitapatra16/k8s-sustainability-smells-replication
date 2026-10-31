# Replication package: Sustainability Smells in Kubernetes-Native ETL Pipelines

Paper: "Sustainability Smells in Kubernetes-Native ETL Pipelines:
Prevalence and Runtime Impact" (INCOWOCO 2026, paper ID 1918).

## Contents
- corpus/            repository list, candidate file paths, download log
- detectors/         detectors for SS1-SS7 (Python, PyYAML plus regex fallback)
- results/           per-file flags (per_file_flags.csv), prevalence output,
                     summary files (results/summaries/)
- experiments/ss1/           SS1 sizing experiment (scripts and runs)
- experiments/ss2/           SS2 scaling experiment (scripts and runs)
- experiments/stage_cpu_cap/ stage CPU-cap experiment A-G (scripts, manifests, results)
- experiments/combined/      combined smelly vs refactored experiment (scripts, manifests)
- runs/              raw runs of the combined experiment (replication_20261021T163112)
- meta/              node description, input checksum, script hashes
- requirements.txt   Python libraries (pandas, boto3, pyyaml, scipy)

## Environment
MicroK8s, 1 master (untainted, runs LocalStack) and 3 workers; Intel Xeon
E5-2620 v3; 24 cores and 62 GB in total; Ubuntu 24.04. Data moves between
stages through LocalStack, an S3-compatible service. The credentials in the
scripts and manifests (access key "test", secret "test") are the LocalStack
default dummy values and are not real AWS credentials.

## Setup
    python3 -m venv venv
venv/bin/pip install -r requirements.txt

## Which script produces which table
- Prevalence table (Table IV): results/per_file_flags.csv and
  results/prevalence_all.txt
- SS1 sizing table (Table V): experiments/ss1/run_ss1.py;
  summary in results/summaries/ss1_full.txt
- SS2 scaling tables (Tables VI and VII): experiments/ss2/run_ss2.py;
  summary in results/summaries/ss2_cg_results.txt
- Individual smell CPU consumption (Table VIII):
  experiments/stage_cpu_cap/run_ag.py, then
  python experiments/stage_cpu_cap/scripts/aggregate_cg.py
  (run from experiments/stage_cpu_cap)
- All-7 pod-active time (Table IX) and end-to-end times:
  experiments/combined/scripts/run_replication.sh, then
  experiments/combined/scripts/aggregate_results.py (reads
  runs/replication_20261021T163112)

## Input data
The input is a synthetic IoT sensor dataset (50,000 rows; also 10,000 and
200,000 rows in the SS2 experiment) with the fields id, sensor, location,
temp, humidity, status, payload, generated with a fixed random seed.
The checksum of the 50,000-row file is in meta/input_md5.txt.

## Corpus
The corpus was collected in April 2026 with the GitHub Code Search API
(15 queries, at most 1,000 results per query). corpus/ lists the 480
repositories and the file paths. The third-party manifests themselves
are not redistributed; each retains its own license.

## License
See LICENSE.

