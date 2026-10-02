#!/bin/sh
# lakehouse-init (curlimages/curl): bucket -> Lakekeeper bootstrap -> warehouse. Idempotent.
# Environment comes from docker-compose.yml (values from .env). POSIX sh, curl only.
# After setup it stays up (sleep, ~1 MB) and reports healthy, so `docker compose up --wait` has a
# health signal for "bootstrap done" (a one-shot that exits races with --wait in Compose v5) and
# spark / trino can depend on it. `--once` exits after setup:
#   docker compose run --rm lakehouse-init --once
set -eu

say() { printf '[lakehouse-init] %s\n' "$*"; }

# 1. Bucket, signed with AWS SigV4 by curl itself (no mc / aws-cli image needed).
#    200 = created, 409 = BucketAlreadyOwnedByYou / BucketAlreadyExists.
code=$(curl -sS -o /tmp/bucket.out -w '%{http_code}' -X PUT \
  --aws-sigv4 "aws:amz:${S3_REGION}:s3" --user "${S3_ACCESS_KEY}:${S3_SECRET_KEY}" \
  "${S3_INTERNAL_ENDPOINT}/${S3_BUCKET}")
case "$code" in
  200) say "bucket ${S3_BUCKET} created" ;;
  409) say "bucket ${S3_BUCKET} exists" ;;
  *) say "bucket ${S3_BUCKET}: HTTP $code"; cat /tmp/bucket.out; exit 1 ;;
esac

# 2. Bootstrap (accept terms, first admin). Already bootstrapped -> 400/409, which is fine.
code=$(curl -sS -o /tmp/boot.out -w '%{http_code}' -X POST "${LAKEKEEPER_URL}/management/v1/bootstrap" \
  -H 'Content-Type: application/json' --data '{"accept-terms-of-use": true}')
case "$code" in
  2*) say "lakekeeper bootstrapped" ;;
  400|409) say "lakekeeper already bootstrapped" ;;
  *) say "bootstrap: HTTP $code"; cat /tmp/boot.out; exit 1 ;;
esac

# 3. Warehouse. sts-enabled: Lakekeeper vends short-lived S3 credentials per table (AssumeRole on
#    the object store). endpoint = what every engine is told; sts-endpoint = how Lakekeeper itself
#    reaches STS. path-style: no virtual-host DNS for buckets.
if curl -fsS "${LAKEKEEPER_URL}/management/v1/warehouse" | grep -q "\"name\":\"${LAKEKEEPER_WAREHOUSE}\""; then
  say "warehouse ${LAKEKEEPER_WAREHOUSE} exists"
else
  cat > /tmp/warehouse.json <<JSON
{
  "warehouse-name": "${LAKEKEEPER_WAREHOUSE}",
  "storage-profile": {
    "type": "s3",
    "bucket": "${S3_BUCKET}",
    "key-prefix": "warehouse",
    "endpoint": "${S3_ENDPOINT}",
    "sts-endpoint": "${S3_INTERNAL_ENDPOINT}",
    "region": "${S3_REGION}",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": true
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "${S3_ACCESS_KEY}",
    "aws-secret-access-key": "${S3_SECRET_KEY}"
  }
}
JSON
  code=$(curl -sS -o /tmp/wh.out -w '%{http_code}' -X POST "${LAKEKEEPER_URL}/management/v1/warehouse" \
    -H 'Content-Type: application/json' --data @/tmp/warehouse.json)
  case "$code" in
    2*) say "warehouse ${LAKEKEEPER_WAREHOUSE} created (s3://${S3_BUCKET}/warehouse, endpoint ${S3_ENDPOINT})" ;;
    409) say "warehouse ${LAKEKEEPER_WAREHOUSE} exists" ;;
    *) say "warehouse: HTTP $code"; cat /tmp/wh.out; exit 1 ;;
  esac
fi
say "ready: catalog http://localhost:8181/catalog, warehouse ${LAKEKEEPER_WAREHOUSE}"
[ "${1:-}" = "--once" ] && exit 0
touch /tmp/ready
trap 'exit 0' TERM INT
while :; do sleep 3600 & wait $!; done
