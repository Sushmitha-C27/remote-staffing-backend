import os
import json
import math
import csv
import tempfile
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3 = boto3.client("s3")
SECRETS = boto3.client("secretsmanager")

# ---------------- CONFIG ----------------
ML_S3_BUCKET = os.environ.get("ML_S3_BUCKET", "remote-staffing-ml")
ML_S3_PREFIX = os.environ.get("ML_S3_PREFIX", "artifacts/")
JOB_EMB_CSV_KEY = os.environ.get(
    "JOB_EMB_CSV_KEY",
    ML_S3_PREFIX.rstrip("/") + "/job_embeddings.csv"
)

TOP_N = int(os.environ.get("TOP_N", "200"))
MATCH_TABLE = os.environ.get("MATCH_TABLE", "MATCH_SCORES")

SF_SECRET_ARN = os.environ.get("SF_SECRET_ARN")
SF_DATABASE = os.environ.get("SF_DATABASE", "JOB_PORTAL_DB")
SF_SCHEMA = os.environ.get("SF_SCHEMA", "CLEAN")

# ---------------- COSINE SIMILARITY ----------------
def dot(a, b):
    return sum(x * y for x, y in zip(a, b))

def norm(a):
    return math.sqrt(sum(x * x for x in a))

def cosine(a, b):
    na, nb = norm(a), norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return dot(a, b) / (na * nb)

# ---------------- LOAD JOB EMBEDDINGS ----------------
def download_s3_to_tmp(key):
    tmp = tempfile.mktemp()
    logger.info("Downloading embeddings from s3://%s/%s", ML_S3_BUCKET, key)
    S3.download_file(ML_S3_BUCKET, key, tmp)
    return tmp

def load_job_embeddings():
    tmp = download_s3_to_tmp(JOB_EMB_CSV_KEY)
    rows = []

    with open(tmp, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rn = r["RN"]
            emb = [float(x) for x in r["embedding_str"].split(",")]
            rows.append((rn, emb))

    logger.info("Loaded %d job embeddings", len(rows))
    return rows

# ---------------- SNOWFLAKE CONNECTION ----------------
def get_snowflake_conn():
    import snowflake.connector

    sec = SECRETS.get_secret_value(SecretId=SF_SECRET_ARN)
    cred = json.loads(sec["SecretString"])

    return snowflake.connector.connect(
        user=cred["SF_USER"],
        password=cred["SF_PASSWORD"],
        account=cred["SF_ACCOUNT"],
        warehouse=cred.get("SF_WAREHOUSE", "COMPUTE_WH"),
        database=cred.get("SF_DATABASE", SF_DATABASE),
        schema=cred.get("SF_SCHEMA", SF_SCHEMA),
    )

# ---------------- WRITE MATCH SCORES ----------------
def persist_scores(candidate_id, matches):
    conn = get_snowflake_conn()
    cur = conn.cursor()

    logger.info("Writing %d matches to Snowflake", len(matches))

    for job_id, score in matches:
        cur.execute(
            f"""
            MERGE INTO {SF_DATABASE}.{SF_SCHEMA}.{MATCH_TABLE} t
            USING (
                SELECT %s AS JOB_ID, %s AS CANDIDATE_ID, %s AS SCORE
            ) s
            ON t.JOB_ID = s.JOB_ID AND t.CANDIDATE_ID = s.CANDIDATE_ID
            WHEN MATCHED THEN
                UPDATE SET SCORE = s.SCORE
            WHEN NOT MATCHED THEN
                INSERT (JOB_ID, CANDIDATE_ID, SCORE)
                VALUES (s.JOB_ID, s.CANDIDATE_ID, s.SCORE);
            """,
            (job_id, candidate_id, float(score))
        )

    conn.commit()
    cur.close()
    conn.close()

    logger.info("Snowflake write complete")


# ---------------- MAIN HANDLER ----------------
def lambda_handler(event, context):
    logger.info("ml-scoring-lambda invoked")

    detail = event.get("detail", {})
    candidate_id = detail.get("candidate_id")

    if not candidate_id:
        return {"status": "error", "message": "candidate_id missing"}

    # ---------- Fetch resume text ----------
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT RESUME_TEXT FROM CANDIDATE_DATA_CLEANED WHERE CANDIDATE_ID = %s LIMIT 1",
        (candidate_id,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row or not row[0]:
        return {"status": "error", "message": "resume_text not found"}

    resume_text = row[0]

    # ---------- Load job embeddings ----------
    job_embs = load_job_embeddings()
    if not job_embs:
        return {"status": "error", "message": "no job embeddings found"}

    # ---------- Dimension-safe resume embedding ----------
    embedding_dim = len(job_embs[0][1])
    resume_emb = [len(resume_text)] * embedding_dim

    # ---------- Compute similarities ----------
    raw_scores = []
    for job_id, job_emb in job_embs:
        score = cosine(resume_emb, job_emb)
        raw_scores.append((job_id, score))

    if not raw_scores:
        return {"status": "error", "message": "no matches computed"}

    # ---------- Normalize scores to 0–100 ----------
    max_score = max(s for _, s in raw_scores) or 1.0
    normalized = [
        (job_id, round((score / max_score) * 100, 2))
        for job_id, score in raw_scores
    ]

    normalized.sort(key=lambda x: x[1], reverse=True)
    top_matches = normalized[:TOP_N]

    # ---------- Persist ----------
    persist_scores(candidate_id, top_matches)

    return {
        "status": "ok",
        "candidate_id": candidate_id,
        "matches_written": len(top_matches),
        "top_matches": [
            {"job_id": job_id, "match_percent": score}
            for job_id, score in top_matches[:10]
        ]
    }
