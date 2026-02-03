import json
import os
import requests
import boto3
import hashlib
from datetime import datetime
from decimal import Decimal

# =====================================================
# AWS
# =====================================================
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("JobDescriptions")

# =====================================================
# ENV
# =====================================================
ADZUNA_APP_ID = os.environ["ADZUNA_APP_ID"]
ADZUNA_APP_KEY = os.environ["ADZUNA_APP_KEY"]
ADZUNA_COUNTRIES = os.environ.get(
    "ADZUNA_COUNTRIES",
    "gb,us,in,ca,au,de,fr,nl,es,it,br,mx"
).split(",")

JOOBLE_API_KEY = os.environ["JOOBLE_API_KEY"]

# =====================================================
# HELPERS
# =====================================================
def sha_job_id(source, title, company, city, country):
    raw = f"{source}|{title}|{company}|{city}|{country}".lower()
    return hashlib.sha256(raw.encode()).hexdigest()

def normalize_apply_url(job):
    # Priority: direct apply → employer site → provider landing
    return (
        job.get("apply_url")
        or job.get("redirect_url")
        or job.get("company_url")
        or job.get("url")
    )

def to_decimal(value):
    if value is None:
        return None
    return Decimal(str(value))

def compute_quality(apply_url, salary_min, salary_max, company, city):
    score = 0
    if apply_url:
        score += 50
    if salary_min or salary_max:
        score += 30
    if company:
        score += 10
    if city:
        score += 10
    return score

# =====================================================
# ADZUNA INGESTION
# =====================================================
def fetch_adzuna(query):
    jobs = []

    for country in ADZUNA_COUNTRIES:
        country = country.strip().lower()

        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1"
        params = {
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "what": query,
            "results_per_page": 20
        }

        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()

        for j in r.json().get("results", []):
            location = j.get("location", {})
            city = location.get("area", [None])[-1]

            jobs.append({
                "source": "adzuna",
                "title": j.get("title"),
                "company": j.get("company", {}).get("display_name"),
                "city": city,
                "country": country.upper(),
                "description": j.get("description", "")[:500],
                "apply_url": j.get("redirect_url"),
                "salary_min": j.get("salary_min"),
                "salary_max": j.get("salary_max"),
                "lat": j.get("latitude"),
                "lng": j.get("longitude")
            })

    return jobs


# =====================================================
# LAMBDA HANDLER
# =====================================================
def lambda_handler(event, context):
    query = event.get("query", "software engineer")

    all_jobs = []

    try:
        all_jobs.extend(fetch_adzuna(query))
    except Exception as e:
        print("Adzuna failed:", str(e))


    seen = set()
    stored = 0
    skipped = 0

    for job in all_jobs:
        job_id = sha_job_id(
            job["source"],
            job["title"],
            job["company"],
            job["city"],
            job["country"]
        )

        if job_id in seen:
            skipped += 1
            continue

        seen.add(job_id)

        apply_url = normalize_apply_url(job)

        item = {
            "job_id": job_id,
            "title": job["title"],
            "company": job["company"],
            "city": job["city"],
            "country": job["country"],
            "location": f"{job['city']}, {job['country']}" if job["city"] else job["country"],
            "apply_url": apply_url,

            "salary_min": to_decimal(job["salary_min"]),
            "salary_max": to_decimal(job["salary_max"]),

            "lat": to_decimal(job["lat"]),
            "lng": to_decimal(job["lng"]),

            "job_quality_score": compute_quality(
                apply_url,
                job["salary_min"],
                job["salary_max"],
                job["company"],
                job["city"]
            ),

            "source": job["source"],
            "description": job["description"],
            "created_at": datetime.utcnow().isoformat()
        }

        try:
            table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(job_id)"
            )
            stored += 1
        except Exception:
            skipped += 1

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Job ingestion completed",
            "jobs_stored": stored,
            "jobs_skipped": skipped,
            "sources": ["adzuna"]
        })
    }
