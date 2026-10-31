import boto3, os, math

ENDPOINT = os.environ["AWS_ENDPOINT_URL"]
TARGET_PARTITION_SIZE = int(os.environ.get("TARGET_PARTITION_SIZE", "20000"))
SRC_BUCKET = os.environ.get("SRC_BUCKET", "raw-data")
SRC_KEY = os.environ.get("SRC_KEY", "input.csv")
TARGET_BUCKET = os.environ.get("TARGET_BUCKET", "raw-data")
KEY_PREFIX = os.environ.get("KEY_PREFIX", "demo_chunk")

s3 = boto3.client("s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1")

raw = s3.get_object(Bucket=SRC_BUCKET, Key=SRC_KEY)["Body"].read().decode("utf-8")
lines = raw.splitlines()
header, body = lines[0], lines[1:]
total = len(body)

# This is the dynamic calculation -- the only real difference from prepare_chunks_v2.py
CHUNKS = math.ceil(total / TARGET_PARTITION_SIZE)

print(f"Source: {SRC_BUCKET}/{SRC_KEY}", flush=True)
print(f"Total rows: {total:,}", flush=True)
print(f"Target partition size: {TARGET_PARTITION_SIZE:,}", flush=True)
print(f"COMPUTED N (parallelism) = ceil({total} / {TARGET_PARTITION_SIZE}) = {CHUNKS}", flush=True)

chunk_size = (total + CHUNKS - 1) // CHUNKS
for i in range(CHUNKS):
    chunk_rows = body[i*chunk_size:(i+1)*chunk_size]
    out = header + "\n" + "\n".join(chunk_rows) + "\n"
    key = f"{KEY_PREFIX}_{i}.csv"
    s3.put_object(Bucket=TARGET_BUCKET, Key=key, Body=out.encode("utf-8"))
    print(f"  Uploaded {TARGET_BUCKET}/{key} with {len(chunk_rows):,} rows", flush=True)

print(f"\nRESULT: N={CHUNKS} chunks created dynamically (not hardcoded).", flush=True)

# Write the computed N somewhere readable, so a real orchestration script
# could read it back before building the parallelism: N manifest
result_key = f"demo/{KEY_PREFIX}_computed_n.txt"
s3.put_object(Bucket=TARGET_BUCKET, Key=result_key, Body=str(CHUNKS).encode())
print(f"Computed N written to s3://{TARGET_BUCKET}/{result_key} for downstream use.", flush=True)
