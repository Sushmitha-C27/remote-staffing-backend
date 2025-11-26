# uploadCandidate/lambda_function.py
import os
import json
import traceback
import boto3
import snowflake.connector

# SSM prefix & region
SSM_PREFIX = os.environ.get("SSM_PREFIX", "/remote-staffing/snowflake")
REGION = os.environ.get("AWS_REGION", "eu-north-1")  # change if your SSM is in different region
ssm = boto3.client("ssm", region_name=REGION)

def get_param(name: str) -> str:
    key = f"{SSM_PREFIX}/{name}"
    resp = ssm.get_parameter(Name=key, WithDecryption=True)
    return resp["Parameter"]["Value"].strip()

def get_sf_conn():
    # read credentials from SSM (account, user, password, role, warehouse, database, schema)
    account = get_param("account")
    user = get_param("user")
    password = get_param("password")
    role = get_param("role")
    warehouse = get_param("warehouse")
    database = get_param("database")
    schema = get_param("schema")
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    return conn

# candidate column candidates (uppercase)
NAME_CANDIDATES = ["NAME", "FULL_NAME", "CANDIDATE_NAME", "FIRST_NAME"]
EMAIL_CANDIDATES = ["EMAIL", "EMAIL_ADDRESS", "CONTACT_EMAIL"]
RESUME_CANDIDATES = ["RESUME_TEXT", "RESUME", "CV_TEXT", "SUMMARY", "RESUME_CONTENT"]

def list_columns(cur, db, schema, table):
    cur.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG=%s AND TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
        (db, schema, table)
    )
    return [r[0].upper() for r in cur.fetchall()]

def choose_column(cols, candidates):
    cols_set = set(cols)
    for c in candidates:
        if c in cols_set:
            return c
    # try substring matches (if candidate is part of column name)
    for col in cols:
        for c in candidates:
            if c in col:
                return col
    return None

def upload_candidate_handler(event, context):
    try:
        raw_body = event.get("body", "{}")
        body = json.loads(raw_body) if isinstance(raw_body, str) else raw_body

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

        cols = list_columns(cur, db, schema, table)
        if not cols:
            return {"statusCode": 500, "body": f"Could not list columns for {db}.{schema}.{table}"}

        name_col = choose_column(cols, NAME_CANDIDATES)
        email_col = choose_column(cols, EMAIL_CANDIDATES)
        resume_col = choose_column(cols, RESUME_CANDIDATES)

        if not (name_col and email_col and resume_col):
            return {"statusCode": 500, "body": f"Could not map candidate columns. Found: {cols}"}

        # Use quoted identifiers to preserve exact column names/case
        insert_sql = f'INSERT INTO {db}.{schema}.{table} ("{name_col}", "{email_col}", "{resume_col}") VALUES (%s, %s, %s)'

        cur.execute(insert_sql, (name, email, resume_text))
        conn.commit()
        cur.close()
        conn.close()

        return {"statusCode": 200, "body": json.dumps({"message": "Candidate uploaded", "columns": [name_col, email_col, resume_col]})}

    except Exception as e:
        traceback.print_exc()
        return {"statusCode": 500, "body": f"Error: {str(e)}"}
