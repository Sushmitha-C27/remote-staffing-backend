import os
import json
import traceback
import boto3
import snowflake.connector

# SSM prefix & region
SSM_PREFIX = os.environ.get("SSM_PREFIX", "/remote-staffing/snowflake")
REGION = os.environ.get("AWS_REGION", "ap-southeast-1")
ssm = boto3.client("ssm", region_name=REGION)

def get_param(name):
    key = f"{SSM_PREFIX}/{name}"
    resp = ssm.get_parameter(Name=key, WithDecryption=True)
    return resp["Parameter"]["Value"].strip()

def get_sf_conn():
    return snowflake.connector.connect(
        user=get_param("user"),
        password=get_param("password"),
        account=get_param("account"),
        role=get_param("role"),
        warehouse=get_param("warehouse"),
        database=get_param("database"),
        schema=get_param("schema"),
    )

def upload_candidate_handler(event, context):
    try:
        raw_body = event.get("body", "{}")
        body = json.loads(raw_body)

        name = body.get("name")
        email = body.get("email")
        resume_text = body.get("resume_text")

        if not (name and email and resume_text):
            return {"statusCode": 400, "body": "Missing name, email or resume_text"}

        conn = get_sf_conn()
        cur = conn.cursor()

        db = get_param("database")
        schema = get_param("schema")
        table = "CANDIDATE_DATA_CLEANED"

        insert_sql = f'INSERT INTO {db}.{schema}.{table} (NAME, EMAIL, RESUME_TEXT) VALUES (%s, %s, %s)'
        cur.execute(insert_sql, (name, email, resume_text))
        conn.commit()

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Candidate uploaded"})
        }

    except Exception as e:
        traceback.print_exc()
        return {"statusCode": 500, "body": f"Error: {str(e)}"}
