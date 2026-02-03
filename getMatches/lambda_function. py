import json
import os
import logging
import boto3
import re
import uuid
from datetime import datetime
from collections import Counter
from decimal import Decimal

# =====================================================
# CONFIG
# =====================================================

REGION = os.environ.get("AWS_REGION", "eu-north-1")
CORS_ORIGIN = os.environ.get("CORS_ORIGIN", "*")

JOB_TABLE = os.environ.get("JOB_TABLE", "JobDescriptions")
CANDIDATE_TABLE = os.environ.get("CANDIDATE_TABLE", "Candidates")

MATCHES_TABLE = os.environ.get("MATCHES_TABLE", "Matches")
MATCHES_LIVE_TABLE = os.environ.get("MATCHES_LIVE_TABLE", "Matches_LIVE")
MATCHES_HISTORY_TABLE = os.environ.get("MATCHES_HISTORY_TABLE", "Matches_HISTORY")
FEATURES_TABLE = os.environ.get("FEATURES_TABLE", "MATCH_FEATURES")

MIN_SCORE = float(os.environ.get("MIN_SCORE", "0.25"))
DEFAULT_TOP_N = int(os.environ.get("DEFAULT_TOP_N", "5"))

# =====================================================
# AWS
# =====================================================

dynamodb = boto3.resource("dynamodb", region_name=REGION)

job_table = dynamodb.Table(JOB_TABLE)
candidate_table = dynamodb.Table(CANDIDATE_TABLE)

matches_table = dynamodb.Table(MATCHES_TABLE)
matches_live_table = dynamodb.Table(MATCHES_LIVE_TABLE)
matches_history_table = dynamodb.Table(MATCHES_HISTORY_TABLE)
features_table = dynamodb.Table(FEATURES_TABLE)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =====================================================
# NLP CONFIG
# =====================================================

STOPWORDS = {
    "and", "or", "the", "a", "to", "of", "in", "for", "with",
    "on", "at", "as", "by", "is", "are", "this", "that"
}

SKILLS = {
    "python", "java", "sql", "aws", "azure", "gcp",
    "docker", "api", "backend", "cloud", "data", "ml"
}

SKILL_SYNONYMS = {
    "cloud": {"aws", "azure", "gcp"},
    "backend": {"api", "microservices"},
    "data": {"sql", "analytics"}
}

# =====================================================
# TEXT HELPERS
# =====================================================

def tokenize(text):
    words = re.findall(r"[a-zA-Z]+", text.lower())
    return [w for w in words if w not in STOPWORDS]

def expand_tokens(tokens):
    expanded = set(tokens)
    for t in tokens:
        if t in SKILL_SYNONYMS:
            expanded |= SKILL_SYNONYMS[t]
    return expanded

# =====================================================
# BM25-STYLE PROXY
# =====================================================

def bm25_proxy(tokens_a, tokens_b, k1=1.5):
    tf_a = Counter(tokens_a)
    tf_b = Counter(tokens_b)
    vocab = set(tf_a) | set(tf_b)

    score = 0.0
    for term in vocab:
        tf = min(tf_a.get(term, 0), tf_b.get(term, 0))
        if tf > 0:
            score += (tf * (k1 + 1)) / (tf + k1)

    return score / max(len(vocab), 1)

# =====================================================
# FEATURE SCORES
# =====================================================

def skill_score(job_tokens, resume_tokens):
    job_skills = job_tokens & SKILLS
    resume_skills = resume_tokens & SKILLS
    return len(job_skills & resume_skills) / max(len(job_skills | resume_skills), 1)

def title_score(job_title, resume_tokens):
    return 1.0 if set(tokenize(job_title)) & resume_tokens else 0.0

def seniority_score(job_text, resume_text):
    if "senior" in job_text and "junior" in resume_text:
        return 0.0
    return 1.0

# =====================================================
# USER-SAFE EXPLANATION HELPERS
# =====================================================

def confidence_badge(score):
    if score >= 0.6:
        return "Strong Match"
    if score >= 0.4:
        return "Good Match"
    return "Fair Match"

def build_user_explanation(skills, title, seniority, score):
    reasons = []

    if skills >= 0.7:
        reasons.append("Strong match with required skills")
    elif skills >= 0.4:
        reasons.append("Moderate match with required skills")
    elif skills > 0:
        reasons.append("Partial skill alignment")

    if title == 1:
        reasons.append("Resume aligns with job role")

    if seniority == 1:
        reasons.append("Experience level fits the role")

    if not reasons:
        reasons.append("Overall profile compatibility")

    return {
        "top_reason": reasons[0],
        "secondary_reasons": reasons[1:3]
    }

# =====================================================
# CANDIDATE NAME RESOLUTION
# =====================================================

def candidate_display_name(c):
    if c.get("name"):
        return c["name"]
    if c.get("full_name"):
        return c["full_name"]

    first = c.get("first_name", "")
    last = c.get("last_name", "")
    full = f"{first} {last}".strip()

    return full if full else c.get("email", "")

# =====================================================
# STORAGE HELPERS
# =====================================================

def store_match_features(job_id, candidate_id, bm25, skills, title, seniority, final_score):
    features_table.put_item(
        Item={
            "match_id": f"{job_id}#{candidate_id}",
            "job_id": job_id,
            "candidate_id": candidate_id,
            "bm25": Decimal(str(round(bm25, 4))),
            "skill_overlap": Decimal(str(round(skills, 4))),
            "title_match": int(title),
            "seniority_match": int(seniority),
            "final_score": Decimal(str(round(final_score, 4))),
            "created_at": datetime.utcnow().isoformat()
        }
    )

def store_canonical(job_id, candidate_id, score):
    matches_table.put_item(
        Item={
            "match_id": f"{job_id}#{candidate_id}",
            "job_id": job_id,
            "candidate_id": candidate_id,
            "match_score": Decimal(str(round(score, 4))),
            "created_at": datetime.utcnow().isoformat()
        }
    )

def store_live(job_id, candidate_id, score):
    matches_live_table.put_item(
        Item={
            "job_id": job_id,
            "candidate_id": candidate_id,
            "match_score": Decimal(str(round(score, 4))),
            "created_at": datetime.utcnow().isoformat()
        }
    )

def store_history(job_id, candidate_id, score, request_id):
    matches_history_table.put_item(
        Item={
            "match_id": str(uuid.uuid4()),
            "job_id": job_id,
            "candidate_id": candidate_id,
            "match_score": Decimal(str(round(score, 4))),
            "request_id": request_id,
            "created_at": datetime.utcnow().isoformat()
        }
    )

# =====================================================
# RESPONSE
# =====================================================

def build_response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": CORS_ORIGIN
        },
        "body": json.dumps(body)
    }

# =====================================================
# HANDLER
# =====================================================

def lambda_handler(event, context):
    params = event.get("queryStringParameters") or {}
    job_id = params.get("job_id")
    candidate_id = params.get("candidate_id")
    top_n = int(params.get("top_n", DEFAULT_TOP_N))
    request_id = context.aws_request_id

    if not job_id and not candidate_id:
        return build_response(400, {"error": "job_id or candidate_id required"})

    # ===================== JOB → CANDIDATES =====================

    if job_id:
        job = job_table.get_item(Key={"job_id": job_id}).get("Item")
        candidates = candidate_table.scan().get("Items", [])

        job_text = f"{job.get('title','')} {job.get('description','')}".lower()
        job_tokens = expand_tokens(tokenize(job_text))
        matches = []

        for c in candidates:
            resume_text = c.get("resume_text", "").lower()
            resume_tokens = expand_tokens(tokenize(resume_text))

            bm25 = bm25_proxy(job_tokens, resume_tokens)
            skills = skill_score(job_tokens, resume_tokens)
            if skills == 0:
                continue

            lexical_core = 0.6 * bm25 + 0.3 * skills
            if lexical_core < 0.15:
                continue

            title = title_score(job.get("title", ""), resume_tokens)
            seniority = seniority_score(job_text, resume_text)

            final_score = lexical_core + 0.05 * title + 0.05 * seniority
            if final_score < MIN_SCORE:
                continue

            store_match_features(job_id, c["candidate_id"], bm25, skills, title, seniority, final_score)

            matches.append({
                "candidate_id": c["candidate_id"],
                "name": candidate_display_name(c),
                "email": c.get("email", ""),
                "match_percent": round(final_score * 100, 1),
                "confidence": confidence_badge(final_score),
                "explanation": build_user_explanation(skills, title, seniority, final_score)
            })

        matches.sort(key=lambda x: x["match_percent"], reverse=True)

        return build_response(200, {
            "mode": "job_to_candidates",
            "total_matches": len(matches),
            "matches": matches[:top_n]
        })

    # ===================== CANDIDATE → JOBS =====================

    candidate = candidate_table.get_item(Key={"candidate_id": candidate_id}).get("Item")
    jobs = job_table.scan().get("Items", [])

    resume_text = candidate.get("resume_text", "").lower()
    resume_tokens = expand_tokens(tokenize(resume_text))
    matches = []

    for job in jobs:
        job_text = f"{job.get('title','')} {job.get('description','')}".lower()
        job_tokens = expand_tokens(tokenize(job_text))

        bm25 = bm25_proxy(job_tokens, resume_tokens)
        skills = skill_score(job_tokens, resume_tokens)
        if skills == 0:
            continue

        lexical_core = 0.6 * bm25 + 0.3 * skills
        if lexical_core < 0.15:
            continue

        title = title_score(job.get("title", ""), resume_tokens)
        seniority = seniority_score(job_text, resume_text)

        final_score = lexical_core + 0.05 * title + 0.05 * seniority
        if final_score < MIN_SCORE:
            continue

        store_match_features(job["job_id"], candidate_id, bm25, skills, title, seniority, final_score)

        matches.append({
            "job_id": job["job_id"],
            "title": job.get("title", ""),
            "company": job.get("company", ""),
            "location": job.get("location", ""),
            "apply_link": job.get("apply_link") or job.get("apply_url") or job.get("redirect_url"),
            "match_percent": round(final_score * 100, 1),
            "confidence": confidence_badge(final_score),
            "explanation": build_user_explanation(skills, title, seniority, final_score)
        })

    matches.sort(key=lambda x: x["match_percent"], reverse=True)

    return build_response(200, {
        "mode": "candidate_to_jobs",
        "total_matches": len(matches),
        "matches": matches[:top_n]
    })
