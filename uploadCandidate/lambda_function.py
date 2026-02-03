import json
import uuid
import logging
from datetime import datetime
from typing import Dict, Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource("dynamodb")
events = boto3.client("events")

# ENV
CANDIDATES_TABLE = "Candidates"
EVENT_BUS = "default"
CORS_ORIGIN = "*"

table = dynamodb.Table(CANDIDATES_TABLE)

# ---------- helpers ----------

def build_response(status: int, body: Any) -> Dict:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": CORS_ORIGIN,
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,POST"
        },
        "body": json.dumps(body)
    }

def parse_body(event: Dict) -> Dict:
    body = event.get("body", {})
    if isinstance(body, str):
        return json.loads(body)
    return body

# ---------- handler ----------

def lambda_handler(event, context):
    logger.info("POST /candidates invoked")

    if event.get("httpMethod") == "OPTIONS":
        return build_response(200, {"ok": True})

    try:
        payload = parse_body(event)

        full_name = payload.get("name")
        email = payload.get("email")
        resume_text = payload.get("resume_text")

        # 🔹 NEW: requested_role from frontend
        requested_role = payload.get("requested_role", "candidate")

        if not all([full_name, email, resume_text]):
            return build_response(400, {
                "error": "name, email, resume_text are required"
            })

        # 🔒 Validate requested_role (intent only)
        if requested_role not in ["candidate", "recruiter", "admin"]:
            requested_role = "candidate"

        candidate_id = str(uuid.uuid4())
        created_at = datetime.utcnow().isoformat()

        # ✅ Store candidate in DynamoDB
        table.put_item(
            Item={
                "candidate_id": candidate_id,
                "full_name": full_name,
                "email": email,
                "resume_text": resume_text,
                "source": "upload",
                "created_at": created_at,

                # 🔐 AUTHORITY (backend-controlled)
                "role": "candidate",

                # 🧠 INTENT (frontend-controlled)
                "requested_role": requested_role
            }
        )

        # ✅ Emit EventBridge event (for ML scoring later)
        events.put_events(Entries=[{
            "Source": "remote-staffing.candidates",
            "DetailType": "CandidateUploaded",
            "EventBusName": EVENT_BUS,
            "Detail": json.dumps({
                "candidate_id": candidate_id
            })
        }])

        return build_response(200, {
            "message": "Candidate uploaded successfully",
            "candidate_id": candidate_id,
            "requested_role": requested_role
        })

    except Exception as e:
        logger.exception("Upload candidate failed")
        return build_response(500, {"error": str(e)})
