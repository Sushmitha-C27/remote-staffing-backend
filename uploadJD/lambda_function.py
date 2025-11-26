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

DESC_CANDIDATES = [
    "ROLES_AND_RESPONSIBILITIES",
    "REQUIRED_QUALIFICATIONS",
    "SKILLS",
    "EXPERIENCE_REQUIRED"
]

def find_desc_column(cur, db, schema, table):
    # Fetch all columns
    cur.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG=%s AND TABLE_SCHEMA=%s AND TABLE_NAME=%s",
        (db, schema, table)
    )
    cols = [r[0].upper() for r in cur.fetchall()]

    if "JOB_TITLE" not in cols:
        return None, cols

    for cand in DESC_CANDIDATES:
        if cand in cols:
            return cand, cols

    return None, cols

def lambda_handler(event, context):
    try:
        raw_body = event.get("body", "{}")
        body = json.loads(raw_body)

        title = body.get("title")
        description = body.get("description")

        if not title or not description:
            return {"statusCode": 400, "body": "Missing title or description"}

        conn = get_sf_conn()
        cur = conn.cursor()
        db = get_param("database")
        schema = get_param("schema")
        table = "JOB_DATA_CLEANED"

        desc_col, cols = find_desc_column(cur, db, schema, table)
        if not desc_col:
            return {
                "statusCode": 500,
                "body": f"Could not find suitable description column. Found columns: {cols}"
            }

        insert_sql = f'INSERT INTO {db}.{schema}.{table} ("JOB_TITLE", "{desc_col}") VALUES (%s, %s)'
        cur.execute(insert_sql, (title, description))
        conn.commit()

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "JD uploaded", "columns": ["JOB_TITLE", desc_col]})
        }

    except Exception as e:
        traceback.print_exc()
        return {"statusCode": 500, "body": f"Error: {str(e)}"}
