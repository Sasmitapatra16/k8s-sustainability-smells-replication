import boto3, os

ENDPOINT = os.environ["AWS_ENDPOINT_URL"]
CHUNKS = int(os.environ.get("CHUNKS", "3"))
SRC_BUCKET = "raw-data"
SRC_KEY = "input.csv"
TARGET_BUCKET = "raw-data"

s3 = boto3.client("s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1")

raw = s3.get_object(Bucket=SRC_BUCKET, Key=SRC_KEY)["Body"].read().decode("utf-8")
lines = raw.splitlines()
header, body = lines[0], lines[1:]
total = len(body)
print(f"Read {total:,} rows from {SRC_BUCKET}/{SRC_KEY}", flush=True)

chunk_size = (total + CHUNKS - 1) // CHUNKS
for i in range(CHUNKS):
    chunk_rows = body[i*chunk_size:(i+1)*chunk_size]
    out = header + "\n" + "\n".join(chunk_rows) + "\n"
    key = f"input_chunk_{i}.csv"
    s3.put_object(Bucket=TARGET_BUCKET, Key=key, Body=out.encode("utf-8"))
    print(f"Uploaded {TARGET_BUCKET}/{key} with {len(chunk_rows):,} rows", flush=True)

print(f"All {CHUNKS} chunks ready in {TARGET_BUCKET}", flush=True)
