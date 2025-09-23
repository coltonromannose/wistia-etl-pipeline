# wistia_pull.py — Glue PySpark job to pull raw Wistia data into Bronze (S3)
# Author: Colton Romannose

import sys, os, json, time
import urllib.request, urllib.error
import boto3
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession  # I fire up Spark so Glue knows this is a PySpark job

# ---------- Glue job args ----------
# These are the arguments I’ll pass in the Glue job config (job parameters)
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'secret_arn',
    'bucket',
    'control_key',
    'region'
])

# I load in my core configuration from Glue parameters
REGION       = args['region']
BUCKET       = args['bucket']
CONTROL_KEY  = args['control_key']
SECRET_ARN   = args['secret_arn']

# I keep a few knobs adjustable so I can tune behavior without editing code
MEDIA_IDS           = json.loads(os.environ.get('MEDIA_IDS_JSON', '["v08dlrgr7v","gskhw4w4lm"]'))
MAX_PAGES           = int(os.environ.get('MAX_PAGES', '100'))          # I never let one run go infinite
TIME_BUDGET_SECONDS = int(os.environ.get('TIME_BUDGET_SECONDS', '45')) # I cut off long pulls
EVENTS_PER_PAGE     = int(os.environ.get('EVENTS_PER_PAGE', '50'))     # small pages to avoid API errors
STATS_PER_PAGE      = int(os.environ.get('STATS_PER_PAGE', '100'))

# Spark session (right now unused, but needed for Glue)
spark = SparkSession.builder.getOrCreate()

# ---------- helpers ----------
def get_secret_token():
    # I grab my Wistia token from Secrets Manager (this keeps creds safe)
    sm = boto3.client("secretsmanager", region_name=REGION)
    s  = sm.get_secret_value(SecretId=SECRET_ARN)
    secret_str = s["SecretString"]
    try:
        return json.loads(secret_str)["WISTIA_API_TOKEN"].strip()
    except Exception:
        return secret_str.strip()

def _normalize_media_state(raw_val):
    # I make sure the watermark/control file always looks the same
    if isinstance(raw_val, dict):
        return {
            "updated": raw_val.get("updated", "1970-01-01T00:00:00Z"),
            "events_checkpoint": raw_val.get("events_checkpoint") or {}
        }
    return {"updated": raw_val or "1970-01-01T00:00:00Z", "events_checkpoint": {}}

def read_watermarks():
    # I read my control file from S3 so I know what was pulled last time
    s3 = boto3.client("s3", region_name=REGION)
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=CONTROL_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except s3.exceptions.NoSuchKey:
        return {}

def persist_watermarks(all_wm):
    # I overwrite the control file with updated checkpoints after each run
    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(
        Bucket=BUCKET,
        Key=CONTROL_KEY,
        Body=json.dumps(all_wm, indent=2).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    print("💾 Watermarks file updated.")

def iso_to_dt(s):
    # I convert ISO strings into proper UTC datetime objects
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

def http_get_json(url, token, timeout=30, retries=(1,2,4)):
    # I fetch JSON from the Wistia API with retries for reliability
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    for i, backoff in enumerate(list(retries)+[None]):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (500,502,503,504) and backoff is not None:
                time.sleep(backoff); continue
            raise
        except urllib.error.URLError:
            if backoff is not None:
                time.sleep(backoff); continue
            raise

def fetch_metadata(token, media_hashed_id):
    # I grab metadata about a video (title, id, timestamps, etc.)
    url = f"https://api.wistia.com/v1/medias/{media_hashed_id}.json"
    return http_get_json(url, token, timeout=30)

def fetch_stats_page(token, media_hashed_id, page=1, per_page=100):
    # I fetch the “aggregate stats” object for a video
    url = f"https://api.wistia.com/v1/stats/medias/{media_hashed_id}.json?page={page}&per_page={per_page}"
    return http_get_json(url, token, timeout=30)

def fetch_events_page(token, media_hashed_id, page=1, per_page=25):
    # I fetch visitor-level events (each row = a viewer doing something)
    url = (
        f"https://api.wistia.com/v1/stats/events.json"
        f"?media={media_hashed_id}&page={page}&per_page={per_page}"
    )
    return http_get_json(url, token, timeout=30)

def write_json_to_s3(obj, key_suffix, media_hashed_id):
    # I save JSON to Bronze in a partitioned layout: media + date
    s3 = boto3.client("s3", region_name=REGION)
    dt_part = datetime.utcnow().strftime("%Y-%m-%d")
    base_key = f"wistia/bronze/media_id={media_hashed_id}/dt={dt_part}/"
    s3.put_object(
        Bucket=BUCKET,
        Key=base_key + key_suffix,
        Body=json.dumps(obj).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    return f"s3://{BUCKET}/{base_key}{key_suffix}"

def write_metadata(meta, media_hashed_id):
    # I also save metadata (video-level info) alongside stats and events
    write_json_to_s3(meta, "metadata.json", media_hashed_id)

def _page_shape(items):
    # I normalize payload shapes so I can figure out when I’ve reached the last page
    if isinstance(items, list):
        return len(items), None, None, items
    arr = None
    for k in ("data","events","items","results"):
        if isinstance(items.get(k), list):
            arr = items[k]; break
    size = len(arr) if arr is not None else 0
    total = items.get("total")
    eff   = items.get("per_page")
    return size, total, eff, arr

# ---------- core ----------
def process_one_media(token, media_hashed_id, watermarks):
    # I wrap ingestion for one video at a time, honoring watermarks and checkpoints
    start = time.time()

    current_val = watermarks.get(media_hashed_id, "1970-01-01T00:00:00Z")
    state = _normalize_media_state(current_val)
    last_seen_iso = state["updated"]
    last_seen = iso_to_dt(last_seen_iso)
    resume_ckpt = state.get("events_checkpoint") or {}
    resume_next_page = resume_ckpt.get("next_page")
    print(f"[{media_hashed_id}] watermark:", last_seen_iso, f"(resume_next_page={resume_next_page})")

    # I fetch metadata to decide: do I need a fresh pull or just resume?
    meta = fetch_metadata(token, media_hashed_id)
    updated_iso = (
        meta.get("updated")
        or meta.get("updated_at")
        or meta.get("created")
        or meta.get("created_at")
        or "2099-01-01T00:00:00Z"
    )
    updated = iso_to_dt(updated_iso)
    print(f"[{media_hashed_id}] remote updated:", updated_iso)

    do_full_pull     = updated > last_seen
    do_resume_events = (not do_full_pull) and isinstance(resume_next_page, int) and resume_next_page >= 1

    if not do_full_pull and not do_resume_events:
        print(f"[{media_hashed_id}] nothing new — skipping.")
        return watermarks

    # STATS: I only do this if metadata changed
    if do_full_pull:
        s_page = 1
        while True:
            stats = fetch_stats_page(token, media_hashed_id, page=s_page, per_page=STATS_PER_PAGE)
            write_json_to_s3(stats, f"stats_page={s_page}.json", media_hashed_id)
            print(f"[{media_hashed_id}] ✅ wrote stats_page={s_page}")

            size, total, eff, _ = _page_shape(stats)
            eff = eff or STATS_PER_PAGE
            if (size < eff) or (isinstance(total,int) and s_page*eff >= total):
                break
            s_page += 1

            if (time.time() - start) > TIME_BUDGET_SECONDS:
                print(f"[{media_hashed_id}] ⏳ stopping stats — time budget hit")
                break

    # EVENTS: I either start from page 1 or resume from checkpoint
    e_page = 1 if do_full_pull else resume_next_page
    pages_written = 0
    last_received_at = None
    finished = False

    while True:
        if pages_written >= MAX_PAGES:
            print(f"[{media_hashed_id}] ⛳ MAX_PAGES hit — pausing here")
            break
        if (time.time() - start) > TIME_BUDGET_SECONDS:
            print(f"[{media_hashed_id}] ⏳ stopping events — time budget hit")
            break

        events = fetch_events_page(token, media_hashed_id, page=e_page, per_page=EVENTS_PER_PAGE)
        write_json_to_s3(events, f"events_page={e_page}.json", media_hashed_id)
        print(f"[{media_hashed_id}] 🎯 wrote events_page={e_page}")
        pages_written += 1

        _, _, _, arr = _page_shape(events)
        if arr:
            try:
                page_max = max([r.get("received_at") for r in arr if isinstance(r.get("received_at"), str)], default=None)
                if page_max and ((last_received_at is None) or (page_max > last_received_at)):
                    last_received_at = page_max
            except Exception:
                pass

        size, total, eff2, _ = _page_shape(events)
        eff2 = eff2 or EVENTS_PER_PAGE
        if (size < eff2) or (isinstance(total,int) and e_page*eff2 >= total):
            finished = True
            break

        e_page += 1

    if do_full_pull:
        write_metadata(meta, media_hashed_id)

    next_ckpt = {}
    if not finished:
        next_ckpt["next_page"] = e_page
    if last_received_at:
        next_ckpt["last_received_at"] = last_received_at

    new_state = {
        "updated": updated_iso if do_full_pull else last_seen_iso,
        "events_checkpoint": next_ckpt
    }
    watermarks[media_hashed_id] = new_state

    if next_ckpt:
        print(f"[{media_hashed_id}] resume checkpoint saved:", next_ckpt)
    print(f"[{media_hashed_id}] watermark -> {new_state['updated']} "
          f"{'(done)' if not next_ckpt else '(resume pending)'}")
    return watermarks

# ---------- main ----------
def main():
    # I keep main() clean so it’s easy to test this locally
    token = get_secret_token()
    watermarks = read_watermarks()

    for media_hashed_id in MEDIA_IDS:
        try:
            watermarks = process_one_media(token, media_hashed_id, watermarks)
        except Exception as e:
            print(f"[{media_hashed_id}] ERROR: {e}")

    persist_watermarks(watermarks)

if __name__ == "__main__":
    main()
