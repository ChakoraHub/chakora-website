import sys
import io
import logging
import traceback
import boto3
import base64
import json
import time
import os
import re
import mimetypes
import urllib.parse
import calendar
import requests
#from scipy import msg
import oracledb
import pathlib
import msal
import hmac
import hashlib
import uuid
from kafka import KafkaProducer, KafkaConsumer
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, date
from threading import Lock, Thread
from flask import Flask, render_template, render_template_string, request, redirect, url_for, session, flash, send_from_directory, make_response, jsonify, send_file, current_app
from werkzeug.security import generate_password_hash
from werkzeug.utils import secure_filename
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from requests.auth import HTTPBasicAuth
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from waitress import serve
from dotenv import load_dotenv
from jinja2 import TemplateNotFound
from functools import lru_cache

# ================= SERVICE URLS =================

HOME_SERVICE_URL = os.getenv("HOME_SERVICE_URL","http://172.31.26.176:5001")
STUDENT_SERVICE_URL = os.getenv("STUDENT_SERVICE_URL","http://172.31.26.176:8001")
MEETING_SERVICE_URL = os.getenv("MEETING_SERVICE_URL","http://172.31.26.176:9000")
CHATBOT_SERVICE_URL = os.getenv("CHATBOT_SERVICE_URL","http://172.31.26.176:7600")
ASSET_SERVICE_URL = os.getenv("ASSET_SERVICE_URL","http://172.31.26.176:8090")
INTERNSHIP_SERVICE_URL = os.getenv("INTERNSHIP_SERVICE_URL","http://172.31.26.176:5050")
MS365_SERVICE_URL = os.getenv("MS365_SERVICE_URL","http://172.31.26.176:7700")
EMPLOYEE_SERVICE_URL = os.getenv("EMPLOYEE_SERVICE_URL","http://172.31.26.176:8002")
BLOGGER_SERVICE_URL = os.getenv("BLOGGER_SERVICE_URL","http://172.31.26.176:7500")
BRS_SERVICE_URL = os.getenv("BRS_SERVICE_URL","http://172.31.26.176:8020")
BILLING_SERVICE_URL = os.getenv("BILLING_SERVICE_URL","http://172.31.26.176:8010")
STM_INTERNAL_API_KEY = os.getenv("STM_INTERNAL_API_KEY", "").strip()
RAG_SERVICE_URL = os.getenv("RAG_SERVICE_URL","http://172.31.26.176:7900")
ONBOARDING_SERVICE_URL = os.getenv("ONBOARDING_SERVICE_URL", "http://172.31.26.176:8100")
OPE_SERVICE_URL = os.getenv("OPE_SERVICE_URL","http://172.31.26.176:8500")
WABA_SERVICE_URL = os.getenv("WABA_SERVICE_URL","http://172.31.26.176:2500").rstrip("/")
FASTAPI_BASE_URL = os.getenv("FASTAPI_BASE_URL", OPE_SERVICE_URL).rstrip("/")
sf_client = None
APPLICATION_SERVICE_URL = os.getenv("https://mobqdeus63.execute-api.eu-north-1.amazonaws.com/Prod", "http://172.31.26.176:8020")
LAMBDA_URL = 'https://lwug4xhfz27whiuu3acjfwsgtm0ttwja.lambda-url.eu-north-1.on.aws/'
STATIC_CDN = "https://d1pjjckqswt5z7.cloudfront.net"
STUDENT_INTERNAL_NO_PROXY = "172.31.26.176"
INTERNAL_NO_PROXY="172.31.26.176"
CANONICAL_HOST = os.getenv("CANONICAL_HOST","www.chakorahub.com").strip().lower()
INTERNSHIP_PUBLIC_HOST = os.getenv("INTERNSHIP_PUBLIC_HOST","api.chakorahub.com").strip().lower()
# SESSION_IDLE_TIMEOUT_MINUTES = _get_session_idle_timeout_minutes()
#_get_runtime_env_value

HOME_SERVICE_URL = "http://127.0.0.1:5001"
STUDENT_SERVICE_URL = "http://127.0.0.1:8001"
MEETING_SERVICE_URL = "http://127.0.0.1:9000"
CHATBOT_SERVICE_URL = "http://127.0.0.1:7600"
ASSET_SERVICE_URL = "http://127.0.0.1:8090"
INTERNSHIP_SERVICE_URL = "http://127.0.0.1:5050"
MS365_SERVICE_URL = "http://127.0.0.1:7700"
EMPLOYEE_SERVICE_URL = "http://127.0.0.1:8002"
BLOGGER_SERVICE_URL = "http://127.0.0.1:7500"
REDIS_SERVICE_URL = "http://127.0.0.1:6390"
BRS_SERVICE_URL = "http://127.0.0.1:8020"
BILLING_SERVICE_URL = "http://127.0.0.1:8010"
RAG_SERVICE_URL = "http://127.0.0.1:7900"
ONBOARDING_SERVICE_URL = os.getenv("ONBOARDING_SERVICE_URL", "http://127.0.0.1:8100")
OPE_SERVICE_URL = "http://127.0.0.1:8500"
WABA_SERVICE_URL = "http://127.0.0.1:2500"
FEEDBACK_SERVICE_URL = os.getenv("FEEDBACK_SERVICE_URL", "http://127.0.0.1:8003")
REDIS_HOST = "127.0.0.1"
APPLICATION_SERVICE_URL = "http://127.0.0.1:8020"
LAMBDA_URL = 'https://lwug4xhfz27whiuu3acjfwsgtm0ttwja.lambda-url.eu-north-1.on.aws/'
WABA_SERVICE_URL = os.getenv("WABA_SERVICE_URL", "http://127.0.0.1:2500").rstrip("/")
STATIC_CDN = "https://d1pjjckqswt5z7.cloudfront.net"
STUDENT_INTERNAL_NO_PROXY = "127.0.0.1"
INTERNAL_NO_PROXY="127.0.0.1"
CANONICAL_HOST = os.getenv("CANONICAL_HOST","www.chakorahub.com").strip().lower()
INTERNSHIP_PUBLIC_HOST = os.getenv("INTERNSHIP_PUBLIC_HOST","api.chakorahub.com").strip().lower()
STM_INTERNAL_API_KEY = os.getenv("STM_INTERNAL_API_KEY", "").strip()
sf_client = None

# STM_SERVICE_URL = os.environ.get("STM_SERVICE_URL", "http://127.0.0.1:7010")
# STM_INTERNAL_API_KEY = os.environ.get("STM_INTERNAL_API_KEY")  # optional shared secret
# SESSION_IDLE_TIMEOUT_MINUTES = _get_session_idle_timeout_minutes()
#_get_runtime_env_value

# ================= Global Configurations =================

# Load env from robust candidate list (supports common Windows " .env.txt " case).
_script_dir = pathlib.Path(__file__).resolve().parent
_ENV_PATH_CANDIDATES = [
    _script_dir / ".env",
    _script_dir / ".env.txt",
    pathlib.Path.cwd() / ".env",
    pathlib.Path.cwd() / ".env.txt",
    _script_dir.parent / ".env",
    _script_dir.parent / ".env.txt",
]

_loaded_env_path = None
for _candidate in _ENV_PATH_CANDIDATES:
    if _candidate.exists():
        load_dotenv(dotenv_path=_candidate, override=True)
        _loaded_env_path = _candidate
        break

print(f"[startup] dotenv selected: {_loaded_env_path if _loaded_env_path else 'NONE'}")
print(f"[startup] env candidates checked: {[str(p) for p in _ENV_PATH_CANDIDATES]}")

MS_TENANT_ID=os.getenv("MS_TENANT_ID")
MS_CLIENT_ID=os.getenv("MS_CLIENT_ID")
MS_CLIENT_SECRET=os.getenv("MS_CLIENT_SECRET")

RAZORPAY_KEY_ID = os.getenv("RZP_KEY_ID")
RAZORPAY_KEY_SECRET = os.getenv("RZP_KEY_SECRET")
RAZORPAY_ORDERS_URL = "https://api.razorpay.com/v1/orders"
key_id = os.getenv("RZP_KEY_ID")
key_secret = os.getenv("RZP_KEY_SECRET")

print(f"[startup] RZP_KEY_ID loaded: {'YES' if os.getenv('RZP_KEY_ID') else 'NO'}")
print(f"[startup] RZP_KEY_SECRET loaded: {'YES' if os.getenv('RZP_KEY_SECRET') else 'NO'}")
print(f"key_id: {key_id} and key_secret: {key_secret} also loaded")

# Redis is intentionally decoupled for now.
_local_cache_store = {}
_local_cache_lock = Lock()

# ── Kafka Producer ──────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
kafka_producer = None
try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",                  # wait for broker confirmation
        retries=3
    )
    print("Kafka producer connected")
except Exception as e:
    #print(f"Kafka producer unavailable: {e}")
    kafka_producer = None

AWS_ACCESS_KEY = (os.getenv("AWS_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID") or "").strip()
AWS_SECRET_KEY = (os.getenv("AWS_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY") or "").strip()
AWS_REGION     = (os.getenv("AWS_REGION") or "eu-north-1").strip()
ADMIN_EMAIL    = (os.getenv("ADMIN_EMAIL") or "admin@chakorahub.com").strip()

CACHE_TTL_SHORT = 300  # 5 minutes
CACHE_TTL_MEDIUM = 1800  # 30 minutes
CACHE_TTL_LONG = 3600  # 1 hour

# ================= Keys =================

script_dir = os.path.dirname(os.path.abspath(__file__))
key_path = os.path.join(script_dir, 'rsa_key.p8')

with open(key_path, 'rb') as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
        backend=default_backend()
    )
pkb = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# ================= App Initialization =================

app = Flask(__name__)
app.secret_key = "chakorahub-secret-key"
from werkzeug.middleware.proxy_fix import ProxyFix
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
# ... (Keep your secret_key and service URLs here) ...
app.secret_key = 'temporary123'
app.permanent_session_lifetime = timedelta(days=7)
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
app.jinja_env.auto_reload = True

# ================= Local configurations =================

UPLOAD_ROOT = os.path.join(script_dir, "uploads")
os.makedirs(UPLOAD_ROOT, exist_ok=True)

PRACTICE_TESTS_FOLDER = os.path.join(UPLOAD_ROOT, "practice_tests")
SYLLABUS_FOLDER = os.path.join(UPLOAD_ROOT, "syllabus")
os.makedirs(PRACTICE_TESTS_FOLDER, exist_ok=True)
os.makedirs(SYLLABUS_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDERS'] = {
    'profile_pics': os.path.join('static', 'profile_pics'),
    'practice_tests': PRACTICE_TESTS_FOLDER
}

app.config['UPLOAD_FOLDER'] = UPLOAD_ROOT
app.config['SYLLABUS_FOLDER'] = SYLLABUS_FOLDER

app.config['ALLOWED_SUBJECTS'] = [
    'Informatica', 'Informatica MDM', 'Unix', 'Oracle', 'IICS', 'Python for web development'
]

app.config['ALLOWED_EXTENSIONS'] = {
    'syllabus': {'pdf', 'docx'},
    'practice_test': {'pdf', 'txt', 'docx', 'xlsx'},
    'blogger': {'md', 'txt'},
    'ppt': {'ppt', 'pptx'},
    'interview': {'pdf', 'txt', 'docx'},
    'code': {'py', 'java', 'cpp', 'c', 'txt', 'xml', 'json',
         'properties', 'sh', 'sql', 'conf', 'map', 'xiwz', 'ksh', 'bash', 'csv', 'yml', 'html', 'js', 'css', 'avro', 'parquet'},
    'images': {'png', 'jpg', 'jpeg', 'gif', 'webp'}
}
app.config['SESSION_COOKIE_SECURE'] = False
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
# Use a dedicated cookie name and cross-subdomain scope for production hostnames.
# This avoids ambiguous old `session` cookies when users switch between old/new cookies.
# Do not force SESSION_COOKIE_DOMAIN here; localhost and direct-host testing will reject it.
app.config['SESSION_COOKIE_NAME'] = 'chakorahub_session'

COURSE_MAP = {
    "informatica": "Informatica",
    "unix": "Unix",
    "oracle": "Oracle(SQL & PLSQL)",
    "iics": "IICS",
    "python for web development": "Python for Web Development",
    "informatica mdm": "MDM",
    "informatica bdm": "BDM",
    "python for automation": "Python for Automation",
    "snowflake": "Snowflake",
}

# ── RBAC usertype helpers ─────────────────────────────────────────────────────
FULL_STUDENT_TYPES = {"student", "student_mi", "student_sh"}
CART_ONLY_TYPES    = {"student_sc"}
SHOP_ALLOWED_TYPES = {"student", "student_mi", "student_sh", "student_sc"}

ORACLE_HOST = os.getenv("ORACLE_HOST", "56.228.73.210")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "FREEPDB1")
ORACLE_USER = os.getenv("ORACLE_USER", "SUPPORT")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "Welcome123")
ORACLE_SCHEMA = os.getenv("ORACLE_SCHEMA", "CHAKORA").strip().upper()


DICT_CURSOR = object()


class _OracleCompatCursor:
    def __init__(self, raw_cursor, dict_mode=False):
        self._cursor = raw_cursor
        self._dict_mode = dict_mode

    @staticmethod
    def _rewrite_sql(sql, params):
        # Oracle doesn't support LIMIT. Normalize common trailing LIMIT clauses.
        sql = re.sub(
            r"\bLIMIT\s+(\d+)\s*;?\s*$",
            r"FETCH FIRST \1 ROWS ONLY",
            sql,
            flags=re.IGNORECASE,
        )

        if params is None or "%s" not in sql:
            return sql, params

        parts = sql.split("%s")
        rewritten = parts[0]
        for idx, tail in enumerate(parts[1:], start=1):
            rewritten += f":{idx}{tail}"
        return rewritten, params

    def execute(self, sql, params=None):
        rewritten_sql, rewritten_params = self._rewrite_sql(sql, params)
        if rewritten_params is None:
            self._cursor.execute(rewritten_sql)
        else:
            self._cursor.execute(rewritten_sql, rewritten_params)

        if self._dict_mode and self._cursor.description:
            columns = [desc[0] for desc in self._cursor.description]
            self._cursor.rowfactory = lambda *values, cols=columns: dict(zip(cols, values))

        return self

    def executemany(self, sql, seq_of_params):
        rewritten_sql, _ = self._rewrite_sql(sql, (0,))
        self._cursor.executemany(rewritten_sql, seq_of_params)
        return self

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _OracleCompatConnection:
    def __init__(self, raw_conn):
        self._conn = raw_conn

    def cursor(self, *args, **kwargs):
        dict_mode = bool(args)
        raw_cursor = self._conn.cursor()
        return _OracleCompatCursor(raw_cursor, dict_mode=dict_mode)

    def __getattr__(self, name):
        return getattr(self._conn, name)

# ================= Helper (or) Custom functions =================

def _clean_request_get(url, params=None, timeout=5):
    with requests.Session() as s:
        s.trust_env = False
        s.proxies = {"http": None, "https": None}
        return s.get(url, params=params, timeout=timeout)

def _clean_request_post(url, json=None, data=None, timeout=5):
    with requests.Session() as s:
        s.trust_env = False
        s.proxies = {"http": None, "https": None}
        return s.post(url, json=json, data=data, timeout=timeout)

def get_db_connection():
    """Standard Oracle connection."""
    dsn = oracledb.makedsn(
        host=ORACLE_HOST,
        port=ORACLE_PORT,
        service_name=ORACLE_SERVICE_NAME,
    )
    raw_conn = oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=dsn,
    )
    conn = _OracleCompatConnection(raw_conn)

    # Keep startup connectivity check
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM DUAL")
    cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {ORACLE_SCHEMA}")
    cur.close()


    return conn

def kafka_publish(topic: str, payload: dict) -> None:
    """Fire-and-forget. Falls back silently if Kafka is down."""
    if kafka_producer is None:
        print(f"⚠️  Kafka publish skipped [{topic}] because producer is unavailable")
        return
    try:
        print(f"📤 Kafka publish request → {topic} | keys={list(payload.keys())}")
        kafka_producer.send(topic, value=payload)
        kafka_producer.flush(timeout=2)
        print(f"📤 Kafka → {topic}: {payload}")
    except Exception as e:
        print(f"⚠️  Kafka publish failed [{topic}]: {e}")


def _run_shop_event_consumer():
    """Keep a local cache of shop order state from Kafka events."""
    try:
        consumer = KafkaConsumer(
            "order.placed",
            "shop_payment.completed",
            "order.confirmed",
            "shop_email.sent",
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            group_id="website-shop-events",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        print("✅ Website shop Kafka consumer connected")
    except Exception as e:
        print(f"⚠️  Website shop Kafka consumer unavailable @ {KAFKA_BOOTSTRAP_SERVERS}: {e}")
        return

    for msg in consumer:
        payload = msg.value or {}
        order_id = payload.get("order_id")
        if not order_id:
            continue

        cache_key = f"shop:order:{order_id}"
        cached = cache_get(cache_key) or {"order_id": order_id}

        if msg.topic == "shop_payment.completed":
            cached.update({
                "payment_id": payload.get("payment_id"),
                "payment_status": payload.get("payment_status"),
                "order_status": "CONFIRMED" if str(payload.get("payment_status") or "").upper() == "SUCCESS" else "FAILED",
                "updated_at": payload.get("timestamp") or datetime.utcnow().isoformat(),
            })
        elif msg.topic == "order.confirmed":
            cached.update({
                "order_status": "CONFIRMED",
                "confirmed_at": payload.get("timestamp") or datetime.utcnow().isoformat(),
            })
        elif msg.topic == "shop_email.sent":
            cached.update({
                "email_status": "SENT",
                "email_sent_at": payload.get("timestamp") or datetime.utcnow().isoformat(),
            })

        if msg.topic == "order.placed":
            cached.update({
                "order_status": "PENDING",
                "payment_status": "PENDING",
                "billing": payload.get("billing") or {},
                "items": payload.get("items") or [],
                "financials": payload.get("financials") or {},
                "payment_method": payload.get("payment_method"),
                "placed_at": payload.get("timestamp") or datetime.utcnow().isoformat(),
            })

        cache_set(cache_key, cached, ttl=CACHE_TTL_LONG)


Thread(target=_run_shop_event_consumer, daemon=True).start()

# -------------------------------------------------------------
# Diagnostic API Forwarder Helper
# -------------------------------------------------------------
def call_fastapi(method, endpoint, json=None, params=None):
    url = f"{OPE_SERVICE_URL}{endpoint}"
    print(f"[Flask Proxy] Forwarding {method} request -> FastAPI: {endpoint}")
    if json:
        logged_json = json.copy() if isinstance(json, dict) else json
        if isinstance(logged_json, dict) and "password" in logged_json:
            logged_json["password"] = "[REDACTED]"
        print(f"[Flask Proxy]   Payload: {logged_json}")
    try:
        if method == "GET":
            res = requests.get(url, params=params, timeout=5)
        elif method == "POST":
            res = requests.post(url, json=json, timeout=5)
        elif method == "PUT":
            res = requests.put(url, json=json, timeout=5)
        elif method == "DELETE":
            res = requests.delete(url, timeout=5)
        print(f"[Flask Proxy]   FastAPI Response Status: {res.status_code}")
        return res
    except Exception as e:
        print(f"[Flask Proxy]   ERROR: Connection failed to FastAPI at {url} | Details: {str(e)}")
        raise e

# -------------------------------------------------------------
# Graceful Error Handlers
# -------------------------------------------------------------
@app.errorhandler(requests.exceptions.ConnectionError)
@app.errorhandler(requests.exceptions.Timeout)
def handle_backend_connection_error(e):
    print(f"[Flask Proxy ERROR] FastAPI backend unreachable at {OPE_SERVICE_URL}")
    error_msg = (
        f"Backend microservice (FastAPI at {OPE_SERVICE_URL}) is currently unreachable. "
        "Please ensure the backend server (ope_service.py) is started and running on port 8000."
    )
    if request.path.startswith('/api/proxy/'):
        return jsonify({"error": error_msg}), 503
    return render_template("login.html", error=error_msg), 503

def is_logged_in():
    return 'user_id' in session

def get_role():
    return session.get('role')

def _wait_for_lookup_result(correlation_id: str, timeout_seconds: int = 10) -> dict:
    """Wait for a matching student.lookup.completed event by correlation_id."""
    consumer = KafkaConsumer(
        "student.lookup.completed",
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        group_id=None,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )
    deadline = time.time() + timeout_seconds
    try:
        while time.time() < deadline:
            for msg in consumer:
                print(f"📥 Kafka consume ← {msg.topic} | partition={msg.partition} offset={msg.offset}")
                event = msg.value or {}
                if event.get("correlation_id") == correlation_id:
                    return event
    finally:
        consumer.close()
    return {}

# Force UTF-8 output on Windows (fixes cp1252 UnicodeEncodeError from emoji in print statements)
def _ensure_console_streams_utf8() -> None:
    """Make stdout/stderr safe for print() in service environments.

    Some Windows service hosts expose closed/invalid stdio streams. We guard
    against that and avoid wrapping the same stream multiple times.
    """
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)

        # If stream is missing/closed, point it to devnull so print never crashes.
        if stream is None or getattr(stream, "closed", False):
            setattr(sys, stream_name, open(os.devnull, "w", encoding="utf-8"))
            continue

        try:
            # Preferred for Python 3.7+ without replacing stream objects.
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding="utf-8", errors="replace")
            elif hasattr(stream, "buffer"):
                current_encoding = (getattr(stream, "encoding", "") or "").lower()
                if current_encoding != "utf-8":
                    setattr(
                        sys,
                        stream_name,
                        io.TextIOWrapper(stream.buffer, encoding="utf-8", errors="replace")
                    )
        except Exception:
            # Never let stream setup stop app startup.
            pass

_ensure_console_streams_utf8()

def _stm_headers():
    headers = {"Content-Type": "application/json"}
    if STM_INTERNAL_API_KEY:
        headers["X-Internal-Api-Key"] = STM_INTERNAL_API_KEY
    return headers

def load_nrm_festivals_from_db():
    conn = get_db_connection()
    cursor = conn.cursor(DICT_CURSOR)
    cursor.execute("SELECT festival_name, festival_date FROM nrm_festivals")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {row['FESTIVAL_DATE'].strftime('%Y-%m-%d'): row['FESTIVAL_NAME'] for row in rows}
 
def normalize(name):
    return name.strip().lower().replace(' ', '').replace('_', '')

def allowed_file(filename, category='images'):
    ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''
    return ext in app.config['ALLOWED_EXTENSIONS'].get(category, set())

def _post_student_service_json(endpoint, payload, timeout=10):
    """Post JSON to student service using single configured service URL."""
    endpoint_path = endpoint if endpoint.startswith("/") else f"/{endpoint}"
    target_url = f"{STUDENT_SERVICE_URL}{endpoint_path}"

    os.environ["NO_PROXY"] = STUDENT_INTERNAL_NO_PROXY
    os.environ["no_proxy"] = STUDENT_INTERNAL_NO_PROXY
    try:
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.post(
                target_url,
                json=payload,
                timeout=timeout,
                allow_redirects=False,
            )

        try:
            data = response.json()
        except ValueError:
            data = {"success": False, "message": "Non-JSON response from student service"}

        return response.status_code, data
    except Exception as e:
        return 503, {"success": False, "message": f"Student service request failed: {e}"}

def _get_student_service_json(endpoint, timeout=10):
    """Get JSON from student service using single configured service URL."""
    endpoint_path = endpoint if endpoint.startswith("/") else f"/{endpoint}"
    target_url = f"{STUDENT_SERVICE_URL}{endpoint_path}"

    os.environ["NO_PROXY"] = STUDENT_INTERNAL_NO_PROXY
    os.environ["no_proxy"] = STUDENT_INTERNAL_NO_PROXY
    try:
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.get(
                target_url,
                timeout=timeout,
                allow_redirects=False,
            )

        try:
            data = response.json()
        except ValueError:
            data = {"success": False, "message": "Non-JSON response from student service"}

        return response.status_code, data
    except Exception as e:
        return 503, {"success": False, "message": f"Student service request failed: {e}"}

def _post_feedback_service_json(endpoint, payload, timeout=10):
    """Post JSON to Feedback Microservice (Port 8003)."""
    endpoint_path = endpoint if endpoint.startswith("/") else f"/{endpoint}"
    target_url = f"{FEEDBACK_SERVICE_URL}{endpoint_path}"
    try:
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.post(
                target_url,
                json=payload,
                timeout=timeout,
                allow_redirects=False,
            )
        try:
            return response.status_code, response.json()
        except ValueError:
            return response.status_code, {"success": False, "message": "Non-JSON response from Feedback Service"}
    except Exception as e:
        return 503, {"success": False, "message": f"Feedback service connection failed: {e}"}

def _get_feedback_service_json(endpoint, params=None, timeout=10):
    """Get JSON from Feedback Microservice (Port 8003)."""
    endpoint_path = endpoint if endpoint.startswith("/") else f"/{endpoint}"
    target_url = f"{FEEDBACK_SERVICE_URL}{endpoint_path}"
    try:
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.get(
                target_url,
                params=params,
                timeout=timeout,
                allow_redirects=False,
            )
        try:
            return response.status_code, response.json()
        except ValueError:
            return response.status_code, {"success": False, "message": "Non-JSON response from Feedback Service"}
    except Exception as e:
        return 503, {"success": False, "message": f"Feedback service connection failed: {e}"}


def _asset_service_request(method, endpoint, json_body=None, params=None, timeout=None):
    """Call the asset microservice with fast-fail behaviour.

    Timeout is (2s connect, 10s read) so an unreachable host fails in ~2s
    instead of blocking on TCP SYN retries.
    """
    if timeout is None:
        timeout = (2, 10)

    endpoint_path = endpoint if endpoint.startswith("/") else f"/{endpoint}"
    target_url = f"{ASSET_SERVICE_URL}{endpoint_path}"

    os.environ["NO_PROXY"] = ASSET_SERVICE_URL
    os.environ["no_proxy"] = ASSET_SERVICE_URL

    try:
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.request(
                method=method.upper(),
                url=target_url,
                json=json_body,
                params=params,
                timeout=timeout,
                allow_redirects=False,
            )

        try:
            data = response.json()
        except ValueError:
            data = {
                "success": False,
                "message": "Non-JSON response from asset service",
                "raw": (response.text or "")[:500],
            }
        return response.status_code, data

    except (requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
        print(f"⚠️ Asset service unreachable: {type(e).__name__}: {e}")
        return 503, {
            "success": False,
            "message": "Asset service is currently unreachable.",
        }

    except Exception as e:
        return 503, {"success": False, "message": f"Asset service request failed: {e}"}


def _employee_asset_service_request(method, endpoint, json_body=None, params=None, timeout=None):
    """Call employee_service asset endpoints as a pure proxy."""
    if timeout is None:
        timeout = (2, 15)

    endpoint_path = endpoint if endpoint.startswith("/") else f"/{endpoint}"
    target_url = f"{EMPLOYEE_SERVICE_URL}{endpoint_path}"

    os.environ["NO_PROXY"] = EMPLOYEE_SERVICE_URL
    os.environ["no_proxy"] = EMPLOYEE_SERVICE_URL

    try:
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.request(
                method=method.upper(),
                url=target_url,
                json=json_body,
                params=params,
                timeout=timeout,
                allow_redirects=False,
            )

        try:
            data = response.json()
        except ValueError:
            data = {
                "success": False,
                "message": "Non-JSON response from employee service",
                "raw": (response.text or "")[:500],
            }
        return response.status_code, data

    except (requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
        print(f"⚠️ Employee service unreachable: {type(e).__name__}: {e}")
        return 503, {
            "success": False,
            "message": "Employee service is currently unreachable.",
        }

    except Exception as e:
        return 503, {"success": False, "message": f"Employee service request failed: {e}"}

def user_has_paid_access(user_id):
    """Proxy payment access check to student_service."""
    if not user_id:
        return False

    endpoint = f"/api/student/access/paid?user_id={user_id}"
    status_code, payload = _get_student_service_json(endpoint, timeout=8)

    if status_code == 200:
        return bool(payload.get("paid", False))

    # Fail open: don't lock out users if student_service is unavailable.
    print(
        "⚠️ Payment access check failed via student_service "
        f"(status={status_code}, payload={payload}), failing open"
    )
    return True


def user_has_completed_resources_registration(user_id):
    """Check if user has completed profile details (Employed, Gothram, Experience etc.)
    Required before accessing /resources. Proxies to student_service.
    """
    if not user_id:
        return False

    endpoint = f"/api/student/access/registration-complete?user_id={user_id}"
    status_code, payload = _get_student_service_json(endpoint, timeout=8)

    if status_code == 200:
        return bool(payload.get("registration_complete", False))

    print(
        "⚠️ Registration completion check failed via student_service "
        f"for user_id={user_id} status={status_code} payload={payload}"
    )
    return False

try:
    ses_client = boto3.client(
        'ses',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
except Exception as e:
    print(f"⚠️ SES Warning: {e}")
    ses_client = None

def _get_session_idle_timeout_minutes() -> int:
    raw_value = str(os.environ.get("SESSION_IDLE_TIMEOUT_MINUTES", "30") or "30").strip()
    try:
        timeout_minutes = int(raw_value)
    except Exception:
        timeout_minutes = 30

    # Prevent accidental 0/negative/tiny values that force immediate logout loops.
    if timeout_minutes < 5:
        print(
            f"⚠️ Invalid SESSION_IDLE_TIMEOUT_MINUTES={raw_value!r}; "
            "using safe default=30"
        )
        return 30
    return timeout_minutes

SESSION_IDLE_TIMEOUT_MINUTES = _get_session_idle_timeout_minutes()

def _forwarded_proto() -> str:
    """Resolve original client protocol behind CDN/proxy hops."""
    proto = (
        request.headers.get("CloudFront-Forwarded-Proto")
        or request.headers.get("X-Forwarded-Proto")
        or request.scheme
        or ""
    )
    return proto.split(",")[0].strip().lower()

def cache_get(key: str):
    """Get value from local in-process cache."""
    try:
        now_ts = time.time()
        with _local_cache_lock:
            payload = _local_cache_store.get(str(key))
            if not payload:
                return None
            expires_at, value = payload
            if expires_at <= now_ts:
                _local_cache_store.pop(str(key), None)
                return None
            return value
    except Exception:
        return None

def cache_set(key: str, value: any, ttl: int = CACHE_TTL_MEDIUM):
    """Set value in local in-process cache with TTL seconds."""
    try:
        ttl_seconds = int(ttl) if ttl is not None else CACHE_TTL_MEDIUM
        if ttl_seconds <= 0:
            ttl_seconds = CACHE_TTL_MEDIUM
        with _local_cache_lock:
            _local_cache_store[str(key)] = (time.time() + ttl_seconds, value)
        return True
    except Exception:
        return False

def cache_delete(key: str):
    """Delete key from local in-process cache."""
    try:
        with _local_cache_lock:
            _local_cache_store.pop(str(key), None)
        return True
    except Exception:
        return False

def cache_delete_pattern(pattern: str):
    """Delete all keys matching a simple wildcard pattern from local cache."""
    try:
        from fnmatch import fnmatch

        with _local_cache_lock:
            if not pattern:
                _local_cache_store.clear()
            else:
                keys_to_delete = [k for k in _local_cache_store.keys() if fnmatch(k, pattern)]
                for key in keys_to_delete:
                    _local_cache_store.pop(key, None)
        return True
    except Exception:
        return False

def require_employee_login():
    """
    Helper function to verify employee or admin session.
    Returns True if valid employee or admin session exists, False otherwise.
    """
    login_type = str(session.get('login_type') or '').lower()
    usertype = str(session.get('usertype') or '').lower()
    role = str(session.get('role') or '').lower()
    
    is_admin = session.get('is_admin') or (login_type in ['admin', 'administrator']) or (usertype in ['admin', 'administrator']) or (role in ['admin', 'administrator'])
    is_employee = (login_type == 'employee') or bool(session.get('employee_id')) or (role == 'employee')
    
    return is_admin or is_employee


def _has_employee_admin_access() -> bool:
    """Allow admin and employee accounts to access admin/report tools."""
    return require_employee_login()


def get_blogger_service_health():
    """
    Check if blogger microservice is healthy
    Can be used in health check endpoints
    """
    try:
        response = requests.get(
            f"{BLOGGER_SERVICE_URL}/health",
            timeout=3
        )
        return response.status_code == 200
    except:
        return False

def get_teams_token():
    """Get app-level Graph API token using client credentials."""
    authority = f"https://login.microsoftonline.com/{MS_TENANT_ID}"
    app_msal = msal.ConfidentialClientApplication(
        client_id=MS_CLIENT_ID,
        client_credential=MS_CLIENT_SECRET,
        authority=authority
    )
    result = app_msal.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    if "access_token" in result:
        return result["access_token"]
    raise Exception(f"Token error: {result.get('error_description')}")

def _verify_razorpay_signature(order_id, payment_id, signature):
    key_secret = (os.getenv("RZP_KEY_SECRET") or "").strip()
    if not key_secret:
        return False

    message  = f"{order_id}|{payment_id}".encode("utf-8")
    expected = hmac.new(
        key_secret.encode("utf-8"),
        message,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)

def validate_email(email: str) -> bool:
    """RFC 5322 compliant email validation"""
    email_regex = r'^[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$'
    return bool(re.match(email_regex, email.strip().lower()))

def validate_e164_phone(phone: str) -> bool:
    """E.164 phone number validation: +[country code][number]"""
    e164_regex = r'^\+[1-9]\d{1,14}$'
    return bool(re.match(e164_regex, phone.strip()))

def _clean_env_value(raw_value):
    if raw_value is None:
        return ""
    value = str(raw_value).strip()
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        value = value[1:-1].strip()
    return value

# Set higher log level to reduce debug noise
logging.getLogger('oracledb').setLevel(logging.WARNING)

def configure_blogger_admin_logger():
    """Write focused blog admin diagnostics to a rotating file."""
    logger = logging.getLogger("blogger_admin")
    if logger.handlers:
        return logger

    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(logs_dir, exist_ok=True)

    handler = RotatingFileHandler(
        os.path.join(logs_dir, "blogger_admin.log"),
        maxBytes=2 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    ))

    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


blogger_admin_logger = configure_blogger_admin_logger()

def get_blogger_admin_session_snapshot():
    """Return only the session fields needed for blog admin diagnostics."""
    return {
        "login_type": session.get("login_type", ""),
        "employee_id": session.get("employee_id", ""),
        "employee_name": session.get("employee_name", ""),
        "user_id": session.get("user_id", ""),
        "user": session.get("user", ""),
        "usertype": session.get("usertype", ""),
        "admin_verified": bool(session.get("admin_verified", False)),
        "verified_at": session.get("verified_at", ""),
    }

def _sync_logout_state(login_type, user_id=None, employee_id=None, reason="logout"):
    """Best-effort sync so upstream auth state is cleared before local session data is dropped."""
    if login_type == "user":
        resolved_user_id = user_id or session.get("user_id") or session.get("track_user_id")
        if not resolved_user_id:
            print(f"⚠️ Logout sync skipped for user: missing user_id | reason={reason}")
            return False

        logout_targets = [
            ("gateway", f"{HOME_SERVICE_URL}/home/logout/user", {"user_id": int(resolved_user_id)}),
            ("direct", f"{HOME_SERVICE_URL}/home/logout/user", {"user_id": int(resolved_user_id)}),
        ]
    elif login_type == "employee":
        resolved_employee_id = employee_id or session.get("employee_id")
        if not resolved_employee_id:
            print(f"⚠️ Logout sync skipped for employee: missing employee_id | reason={reason}")
            return False

        logout_targets = [
            ("gateway", f"{HOME_SERVICE_URL}/home/logout/employee", {"employee_id": resolved_employee_id}),
        ]
    else:
        print(f"⚠️ Logout sync skipped: unsupported login_type={login_type!r} | reason={reason}")
        return False

    for target_name, target_url, payload in logout_targets:
        try:
            print(
                f"🚪 Logout sync attempt via {target_name}: "
                f"type={login_type} reason={reason} url={target_url} payload={payload}"
            )
            with requests.Session() as sync_session:
                sync_session.trust_env = False
                sync_session.proxies = {"http": None, "https": None}
                response = sync_session.post(target_url, json=payload, timeout=8)

            if response.status_code == 200:
                try:
                    response_body = response.json()
                except Exception:
                    response_body = response.text
                print(
                    f"✅ Logout sync success via {target_name}: "
                    f"type={login_type} reason={reason} payload={payload} response={response_body}"
                )
                return True

            try:
                response_body = response.json()
            except Exception:
                response_body = response.text
            print(
                f"⚠️ Logout sync non-200 via {target_name}: "
                f"type={login_type} reason={reason} status={response.status_code} body={response_body}"
            )
        except Exception as e:
            print(f"❌ Logout sync error via {target_name}: type={login_type} reason={reason} error={e}")

    print(f"❌ Logout sync failed on all targets: type={login_type} reason={reason}")
    return False

def send_shop_order_email(user_email, full_name, order_id, payment_id, items=None, financials=None):
    """
    Sends a ChakoraHub order-confirmation email for Course Store (/shop) purchases.
    Reuses the same visual card template as send_registration_email, with
    shop-relevant data (items purchased, GST breakdown, order/payment IDs)
    in place of registration/course-enrollment fields.
    """
    try:
        if ses is None:
            print("❌ SES email skipped: AWS SES client is not initialized. Check AWS credentials/region.")
            return False

        items = items or []
        financials = financials or {}
        display_name = (full_name or user_email or "Customer").strip()

        def _money(val):
            try:
                return f"{float(val):,.0f}"
            except (TypeError, ValueError):
                return "0"

        subject = f"✅ Order Confirmed - {order_id}"

        item_rows = ""
        for it in items:
            name = it.get("name") or "Course"
            qty = it.get("qty") or it.get("quantity") or 1
            amount = it.get("amount")
            if amount is None:
                amount = (it.get("price") or 0) * qty
            item_rows += f"""
                <tr>
                    <td style="padding:10px 8px;font-size:14px;color:#2d3748;border-bottom:1px solid #e7edf5;">{name}</td>
                    <td align="center" style="padding:10px 8px;font-size:14px;color:#2d3748;border-bottom:1px solid #e7edf5;">{qty}</td>
                    <td align="right" style="padding:10px 8px;font-size:14px;color:#2d3748;border-bottom:1px solid #e7edf5;">₹{_money(amount)}</td>
                </tr>"""

        html_content = f"""

<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Order Confirmation – ChakoraHub</title>
</head>
<body style="margin:0;padding:0;background-color:#eef2f7;font-family:Arial,Helvetica,sans-serif;">

  <!-- ═══════════════  OUTER WRAPPER  ═══════════════ -->
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background-color:#eef2f7;padding:32px 0;">
    <tr>
      <td align="center">

        <!-- ─── CARD ─── -->
        <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0"
               style="max-width:600px;width:100%;background-color:#ffffff;
                      border-radius:12px;overflow:hidden;
                      box-shadow:0 4px 24px rgba(0,0,0,0.08);">

          <!-- ══════════  HEADER  ══════════ -->
          <tr>
            <td align="center" style="padding:36px 32px 24px;">

              <div style="font-size:48px;line-height:1;margin-bottom:8px;">🛍️</div>

              <div style="font-size:18px;font-weight:700;color:#1a2340;
                          letter-spacing:0.04em;margin-bottom:10px;">
                ChakoraHub
              </div>

              <div style="font-size:15px;color:#5b9bd5;font-weight:500;">
                Your Order is Confirmed
              </div>

            </td>
          </tr>

          <!-- ══════════  BODY  ══════════ -->
          <tr>
            <td style="padding:0 32px 32px;">

              <p style="margin:0 0 20px;font-size:15px;color:#2d3748;line-height:1.6;">
                Hi <strong>{display_name}</strong>, thank you for your purchase! Your payment
                was successful and your enrollment is confirmed. Here are your order details:
              </p>

              <!-- ─── DETAILS CARD ─── -->
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
                     style="background-color:#f0f5fb;border-radius:10px;
                            border-left:4px solid #4a90d9;overflow:hidden;">
                <tr>
                  <td style="padding:20px 24px;">

                    <!-- Items table -->
                    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                      <tr style="background-color:#e7edf5;">
                        <td style="padding:8px 8px;font-size:11px;font-weight:700;color:#4a5568;text-transform:uppercase;letter-spacing:.04em;">Course</td>
                        <td align="center" style="padding:8px 8px;font-size:11px;font-weight:700;color:#4a5568;text-transform:uppercase;letter-spacing:.04em;">Qty</td>
                        <td align="right" style="padding:8px 8px;font-size:11px;font-weight:700;color:#4a5568;text-transform:uppercase;letter-spacing:.04em;">Amount</td>
                      </tr>
                      {item_rows}
                    </table>

                    <!-- Divider -->
                    <hr style="border:none;border-top:1px solid #d0dce8;margin:16px 0 14px;">

                    <!-- Totals -->
                    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                      <tr>
                        <td style="padding:4px 8px;font-size:13px;color:#718096;">Subtotal</td>
                        <td align="right" style="padding:4px 8px;font-size:13px;color:#718096;">₹{_money(financials.get("subtotal"))}</td>
                      </tr>
                      <tr>
                        <td style="padding:4px 8px;font-size:13px;color:#718096;">GST (18%)</td>
                        <td align="right" style="padding:4px 8px;font-size:13px;color:#718096;">₹{_money(financials.get("gst_amount"))}</td>
                      </tr>
                      <tr>
                        <td style="padding:10px 8px 0;font-size:15px;font-weight:700;color:#2d3748;">Total Paid</td>
                        <td align="right" style="padding:10px 8px 0;font-size:15px;font-weight:700;color:#c8932f;">₹{_money(financials.get("final_total"))}</td>
                      </tr>
                    </table>

                    <!-- Divider -->
                    <hr style="border:none;border-top:1px solid #d0dce8;margin:16px 0 14px;">

                    <!-- ID section -->
                    <p style="margin:0;font-size:12px;color:#718096;line-height:1.8;">
                      <strong style="color:#4a5568;">Order ID:</strong>
                      &nbsp;{order_id}<br>
                      <strong style="color:#4a5568;">Payment ID:</strong>
                      &nbsp;{payment_id}<br>
                      <strong style="color:#4a5568;">Email:</strong>
                      &nbsp;{user_email}
                    </p>

                  </td>
                </tr>
              </table>
              <!-- /DETAILS CARD -->

              <p style="margin:20px 0 0;font-size:13px;color:#718096;line-height:1.6;">
                You can access your courses anytime from the
                <strong style="color:#4a5568;">My Courses</strong> section of your ChakoraHub account.
              </p>

            </td>
          </tr>

          <!-- ══════════  FOOTER  ══════════ -->
          <tr>
            <td align="center"
                style="padding:20px 32px 36px;border-top:1px solid #edf2f7;">
              <p style="margin:0;font-size:13px;color:#a0aec0;line-height:1.7;">
                Questions? Reply to this email or write to<br>
                <a href="mailto:support@chakorahub.com"
                   style="color:#5b9bd5;text-decoration:none;font-weight:600;">
                  support@chakorahub.com
                </a>
              </p>
            </td>
          </tr>

        </table>
        <!-- /CARD -->

      </td>
    </tr>
  </table>

</body>
</html>
"""

        items_text_lines = []
        for it in items:
            name = it.get("name") or "Course"
            qty = it.get("qty") or it.get("quantity") or 1
            amount = it.get("amount")
            if amount is None:
                amount = (it.get("price") or 0) * qty
            items_text_lines.append(f"- {name} x{qty} — ₹{_money(amount)}")
        items_text = "\n".join(items_text_lines)

        text_content = f"""
Your order with ChakoraHub is confirmed!

Order ID: {order_id}
Payment ID: {payment_id}

ITEMS:
{items_text}

Subtotal: ₹{_money(financials.get("subtotal"))}
GST (18%): ₹{_money(financials.get("gst_amount"))}
Total Paid: ₹{_money(financials.get("final_total"))}

You can access your courses from the My Courses section.

Regards,
ChakoraHub Team
"""

        ses.send_email(
            Source=ADMIN_EMAIL,
            Destination={
                "ToAddresses": [user_email],
                "CcAddresses": [ADMIN_EMAIL]
            },
            Message={
                "Subject": {"Data": subject},
                "Body": {
                    "Html": {"Data": html_content},
                    "Text": {"Data": text_content}
                }
            }
        )

        print(f"✅ Shop order confirmation email sent to {user_email}")
        return True

    except Exception as e:
        import traceback
        print("❌ Shop order email sending failed:", e)
        print("❌ SES traceback:", traceback.format_exc())
        return False
    
def get_session_usertype() -> str:
    return (session.get("usertype") or "").strip().lower()

def require_usertype(*allowed_types):
    """Decorator: blocks the route if the logged-in user's usertype is not in allowed_types."""
    from functools import wraps
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if not is_logged_in():
                return redirect(url_for("home"))
            ut = get_session_usertype()
            if ut not in allowed_types and ut not in {"admin", "superadmin", "administrator"}:
                return jsonify({"success": False, "message": f"Access denied for role '{ut}'"}), 403
            return f(*args, **kwargs)
        return wrapped
    return decorator

# stdout/stderr encoding is configured once at startup by _ensure_console_streams_utf8().

@app.before_request
def enforce_canonical_host_redirect():
    if request.path.startswith("/stm/"):
        return
    
    # Keep all web traffic on one hostname so session cookies remain consistent.
    if request.path.startswith('/static'):
        return

    forwarded_host = (request.headers.get("X-Forwarded-Host") or "").split(",")[0].strip()
    host_header = (forwarded_host or request.headers.get("Host") or request.host or "").split(":")[0].lower()

    internship_host_paths = {"/internships", "/api/internship/apply"}
    api_host_allowed_paths = {"/internships", "/api/internship/apply", "/health"}

    if INTERNSHIP_PUBLIC_HOST and host_header == INTERNSHIP_PUBLIC_HOST and request.path not in api_host_allowed_paths:
        return redirect(f"https://{CANONICAL_HOST}/", code=308)

    if request.path in internship_host_paths and INTERNSHIP_PUBLIC_HOST and host_header != INTERNSHIP_PUBLIC_HOST and host_header not in {"localhost", "127.0.0.1", "0.0.0.0"}:
        target_url = f"https://{INTERNSHIP_PUBLIC_HOST}{request.path}"
        if request.query_string:
            target_url = f"{target_url}?{request.query_string.decode('utf-8', errors='ignore')}"
        return redirect(target_url, code=308)

    allowed_hosts = {"chakorahub.com", "www.chakorahub.com"}

    if CANONICAL_HOST and host_header in allowed_hosts and host_header != CANONICAL_HOST:
        target_url = f"https://{CANONICAL_HOST}{request.path}"
        if request.query_string:
            target_url = f"{target_url}?{request.query_string.decode('utf-8', errors='ignore')}"
        return redirect(target_url, code=308)

# ================= ROUTES (PROXY) =====================

@app.before_request
def log_blogger_admin_request_entry():
    if request.path.startswith("/blogger/admin") or request.path == "/admin/blogger":
        blogger_admin_logger.info(
            "Request entry | method=%s path=%s remote_addr=%s content_type=%s session=%s",
            request.method,
            request.path,
            request.headers.get("X-Forwarded-For", request.remote_addr),
            request.content_type,
            get_blogger_admin_session_snapshot(),
        )


@app.before_request
def enforce_idle_timeout_and_track_last_page():
    if request.path.startswith('/static'):
        return

    # Do not enforce inactivity timeout while user is trying to authenticate/reset.
    if request.path in {'/nrm_logins', '/forgot-password'} or request.path.startswith('/reset-password/'):
        return

    login_type = session.get("login_type")
    has_identity = bool(
        (login_type == "user" and (session.get("user_id") or session.get("track_user_id")))
        or (login_type == "employee" and session.get("employee_id"))
    )

    # Cleanup stale partial sessions so they do not repeatedly trigger idle-timeout flash.
    if login_type in {"user", "employee"} and not has_identity:
        session.pop("login_type", None)
        session.pop("last_activity_utc", None)
        session.pop("last_visited_path", None)
        session.modified = True
        return

    logged_in = has_identity
    if logged_in:
        now = datetime.utcnow()
        last_seen_raw = session.get("last_activity_utc")

        if last_seen_raw:
            try:
                last_seen = datetime.fromisoformat(last_seen_raw)
                idle_seconds = (now - last_seen).total_seconds()
                if idle_seconds > (SESSION_IDLE_TIMEOUT_MINUTES * 60):
                    _sync_logout_state(
                        session.get("login_type"),
                        user_id=session.get("user_id") or session.get("track_user_id"),
                        employee_id=session.get("employee_id"),
                        reason="idle-timeout",
                    )
                    session.clear()
                    flash("Session expired due to inactivity. Please login again.", "error")
                    if request.path != '/':
                        return redirect(url_for("home"))
            except Exception:
                # If parsing fails for any reason, reset activity timestamp below.
                pass

        session["last_activity_utc"] = now.isoformat()
        session.modified = True

        should_track_path = (
            request.method == "GET"
            and request.path not in {"/", "/logout", "/nrm_logins", "/home/enquiry"}
            and not request.path.startswith('/blogger')
        )
        if should_track_path:
            session["last_visited_path"] = request.path
            session.modified = True

@app.after_request
def disable_cache_for_dynamic_pages(response):
    """Force fresh HTML for frequently edited templates."""
    no_cache_paths = {"/resources", "/admin/upload"}
    if request.path in no_cache_paths:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

@app.route('/api/chatbot/message', methods=['POST'])
def chatbot_message():
    """Proxy chatbot message to FastAPI chatbot service."""
    try:
        data = request.get_json(silent=True) or {}

        if 'user_id' in session:
            data['user_id'] = session['user_id']
            data['user_name'] = session.get('username', 'User')
            data['user_type'] = session.get('login_type', 'anonymous')
        elif 'employee_id' in session:
            data['user_id'] = session['employee_id']
            data['user_name'] = session.get('employee_name', 'Employee')
            data['user_type'] = 'employee'

        os.environ["NO_PROXY"] = CHATBOT_SERVICE_URL
        os.environ["no_proxy"] = CHATBOT_SERVICE_URL

        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.post(
                f"{CHATBOT_SERVICE_URL}/chatbot/message",
                json=data,
                timeout=60,
                allow_redirects=False,
            )

        try:
            payload = response.json()
        except ValueError:
            preview = (response.text or "").replace("\n", " ").strip()[:300]
            payload = {
                "success": False,
                "response": "Chatbot service returned non-JSON response",
                "upstream_status": response.status_code,
                "upstream_preview": preview,
            }

        return jsonify(payload), response.status_code

    except requests.exceptions.Timeout:
        return jsonify({
            "success": False,
            "response": "I'm taking a bit longer to respond. Please try again."
        }), 504

    except Exception as e:
        print(f"Chatbot proxy error: {e}")
        return jsonify({
            "success": False,
            "response": "Sorry, I'm having technical difficulties. Please try again later."
        }), 500

@app.route('/api/chatbot/history', methods=['GET'])
def chatbot_history():
    """Get chat history for a conversation."""
    try:
        conversation_id = request.args.get('conversation_id')

        if not conversation_id:
            return jsonify({"success": False, "message": "No conversation ID"}), 400

        os.environ["NO_PROXY"] = CHATBOT_SERVICE_URL
        os.environ["no_proxy"] = CHATBOT_SERVICE_URL

        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.get(
                f"{CHATBOT_SERVICE_URL}/chatbot/history",
                params={'conversation_id': conversation_id},
                timeout=10,
                allow_redirects=False,
            )

        try:
            payload = response.json()
        except ValueError:
            payload = {"success": False, "messages": []}

        return jsonify(payload), response.status_code

    except Exception as e:
        print(f"Chatbot history error: {e}")
        return jsonify({"success": False, "messages": []}), 500

@app.route('/api/chatbot/clear', methods=['POST'])
def chatbot_clear():
    """Clear chatbot conversation history."""
    try:
        data = request.get_json(silent=True) or {}
        conversation_id = data.get('conversation_id')

        os.environ["NO_PROXY"] = CHATBOT_SERVICE_URL
        os.environ["no_proxy"] = CHATBOT_SERVICE_URL

        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.post(
                f"{CHATBOT_SERVICE_URL}/chatbot/clear",
                json={'conversation_id': conversation_id},
                timeout=10,
                allow_redirects=False,
            )

        try:
            payload = response.json()
        except ValueError:
            payload = {"success": False}

        return jsonify(payload), response.status_code

    except Exception as e:
        print(f"Chatbot clear error: {e}")
        return jsonify({"success": False}), 500

@app.route('/favicon.ico')
def favicon():
    """Serve favicon using existing logo asset to avoid 404 noise."""
    return redirect(url_for('static', filename='logo.png'), code=302)

@app.route("/")
def home():

    if (
        session.get("login_type") == "user" and session.get("user_id")
    ) or (
        session.get("login_type") == "employee" and session.get("employee_id")
    ):
        login_type = session.get("login_type")
        # Keep logged-in landing deterministic; stale last_visited_path values
        # can cause redirect loops and role-mismatch bounces.
        session.pop("last_visited_path", None)
        session.modified = True

        if login_type == "user":
            return redirect(url_for("resources"))
        if login_type == "employee":
            return redirect(url_for("employee_resources"))

    feedbacks = []
    current_batches = []
    upcoming_batches = []

    try:
        # ===== FEEDBACK =====
        feedback_response = _clean_request_get(
            f"{HOME_SERVICE_URL}/home/feedbacks",
            timeout=5
        )

        print("Feedback Status:", feedback_response.status_code)

        if feedback_response.status_code == 200:
            feedbacks = feedback_response.json().get("feedbacks", [])
        else:
            print("Feedback API Error:", feedback_response.text)

        print("Final Feedback Count:", len(feedbacks))

        # ===== BATCHES =====
        batches_response = _clean_request_get(
            f"{HOME_SERVICE_URL}/home/batches",
            timeout=5
        )

        if batches_response.status_code == 200:
            batches = batches_response.json()
            current_batches = batches.get("current_batches", [])
            upcoming_batches = batches.get("upcoming_batches", [])

    except Exception as e:
        print("HOME ERROR:", e)

    return render_template(
        "home.html",
        feedbacks=feedbacks,
        current_batches=current_batches,
        upcoming_batches=upcoming_batches
    )

@app.route('/api/home/batches')
def api_home_batches():
    """
    Public JSON endpoint for mobile app.
    Proxies to HOME_SERVICE_URL/home/batches and returns JSON directly.
    """
    try:
        batches_response = _clean_request_get(
            f"{HOME_SERVICE_URL}/home/batches",
            timeout=10
        )
        if batches_response.status_code == 200:
            batches = batches_response.json()
            return jsonify({
                "success": True,
                "current_batches": batches.get("current_batches", []),
                "upcoming_batches": batches.get("upcoming_batches", [])
            })
        else:
            return jsonify({
                "success": False,
                "current_batches": [],
                "upcoming_batches": [],
                "message": f"Upstream error {batches_response.status_code}"
            }), 200   # still 200 so mobile doesn't crash
    except Exception as e:
        print(f"❌ api_home_batches error: {e}")
        return jsonify({
            "success": False,
            "current_batches": [],
            "upcoming_batches": [],
            "message": str(e)
        }), 200
@app.route("/gallery")
def gallery():
    gallery_items = []

    try:
        gallery_response = _clean_request_get(
            f"{HOME_SERVICE_URL}/home/gallery",
            timeout=5
        )

        if gallery_response.status_code == 200:
            gallery_items = gallery_response.json().get("items", [])
        else:
            print("Gallery API Error:", gallery_response.text)

    except Exception as e:
        print("GALLERY ERROR:", e)

    return render_template("gallery.html", gallery_items=gallery_items)

@app.route("/home/enquiry", methods=["POST"])
def proxy_enquiry():
    try:
        # Accept both JSON & FormData
        if request.is_json:
            data = request.get_json()
        else:
            data = request.form.to_dict()

        print("🔥 APP PROXY DATA:", data)

        response = requests.post(
            f"{HOME_SERVICE_URL}/home/enquiry",
            json=data,   # Always send JSON forward
            timeout=10
        )

        return jsonify(response.json()), response.status_code

    except Exception as e:
        print("❌ APP PROXY ERROR:", e)
        return jsonify({
            "success": False,
            "message": "Server error"
        }), 500

@app.route("/resources")
def resources():
    route_start = time.perf_counter()

    print(
        "🏠 /resources requested | "
        f"login_type={session.get('login_type')} user_id={session.get('user_id')} session_keys={list(session.keys())}"
    )

    # ─────────────────────────────────────────
    # AUTH CHECK
    # ─────────────────────────────────────────
    if session.get("login_type") != "user":
        print(f"⏱️ [/resources] auth failed in {time.perf_counter() - route_start:.3f}s")
        session.clear()
        flash("Please login", "error")
        return redirect(url_for("home"))

    user_id = str(session.get("user_id"))
    print(f"⏱️ [/resources] auth/session setup took {time.perf_counter() - route_start:.3f}s")

    profile = {
        "username": session.get("username") or session.get("user", ""),
        "usertype": session.get("usertype", "student"),
        "profile_pic": session.get("profile_pic", ""),
    }

    shared = {
        "offers": {},
        "festival_today": None,
        "greeting": "Welcome",
    }

    courses_for_grid = []
    try:
        status_code, payload = _get_student_service_json("/api/student/resources-courses", timeout=10)
        if status_code == 200 and isinstance(payload, dict):
            raw_courses = payload.get("courses") or []
            normalized_courses = []
            for item in raw_courses:
                if not isinstance(item, dict):
                    continue
                image_url = str(item.get("image_url") or item.get("IMAGE_URL") or "").strip()
                if image_url:
                    low = image_url.lower()
                    if low.startswith("http://") or low.startswith("https://") or image_url.startswith("/"):
                        resolved_image_url = image_url
                    else:
                        resolved_image_url = url_for("static", filename=image_url)
                else:
                    resolved_image_url = ""

                normalized = dict(item)
                normalized["image_url"] = resolved_image_url
                normalized_courses.append(normalized)

            courses_for_grid = normalized_courses
        else:
            print(f"⚠️ [/resources] courses grid load failed: status={status_code} payload={payload}")
    except Exception as e:
        print(f"⚠️ [/resources] courses grid exception: {e}")

    # ─────────────────────────────────────────
    # RENDER
    # ─────────────────────────────────────────
    render_start = time.perf_counter()
    rendered = render_template(
        "resources.html",
        username=profile.get("username"),
        usertype=profile.get("usertype"),
        profile_pic=profile.get("profile_pic"),

        offers=shared.get("offers"),
        festival_today=shared.get("festival_today"),
        greeting=shared.get("greeting"),

        courses_for_grid=courses_for_grid,

        reg_id=user_id
    )
    print(f"⏱️ [/resources] render_template took {time.perf_counter() - render_start:.3f}s")
    print(f"🎯 [/resources] total route time {time.perf_counter() - route_start:.3f}s")
    return rendered

@app.route('/api/course-videos/<path:subject>')
def api_course_videos(subject):
    if session.get("login_type") != "user":
        return jsonify({"success": False, "message": "Please login as user"}), 401

    usertype = (session.get("usertype") or "").lower()
    if usertype not in ["admin", "administrator"] and not user_has_completed_resources_registration(session.get("user_id")):
        return jsonify({"success": False, "message": "Please complete ChakoraHub Register to access videos."}), 403

    lang = request.args.get("lang", "telugu").strip()
    endpoint = f"/api/student/course-videos?subject={requests.utils.quote(subject)}&lang={requests.utils.quote(lang)}"
    status_code, payload = _get_student_service_json(endpoint, timeout=10)

    if status_code == 200 and payload.get("success"):
        return jsonify({"success": True, "videos": payload.get("videos", [])}), 200

    print(f"❌ COURSE VIDEOS ERROR for '{subject}': status={status_code} payload={payload}")
    return jsonify({"success": False, "message": "Failed to load videos"}), 500


@app.route('/api/course-resources/<path:subject>')
def api_course_resources(subject):
    if session.get("login_type") != "user":
        return jsonify({"success": False, "message": "Please login as user"}), 401

    usertype = (session.get("usertype") or "").lower()
    if usertype not in ["admin", "administrator"] and not user_has_completed_resources_registration(session.get("user_id")):
        return jsonify({"success": False, "message": "Please complete registration to access resources."}), 403

    endpoint = (
        f"/api/student/course-resources?"
        f"subject={requests.utils.quote(subject)}"
        f"&user_id={session.get('user_id')}"
        f"&usertype={session.get('usertype')}"
    )
    status_code, payload = _get_student_service_json(endpoint, timeout=10)

    if status_code == 200 and payload.get("success"):
        return jsonify({
            "success":   True,
            "subject":   subject,
            "ppts":      payload.get("ppts", []),
            "code":      payload.get("code", []),
            "interview": payload.get("interview", [])
        }), 200

    print(f"❌ COURSE RESOURCES ERROR for '{subject}': status={status_code} payload={payload}")
    return jsonify({"success": False, "message": "Failed to load resources"}), 500



def _get_course_videos(subject, lang=None):
    """Fetch active course videos via student_service, optionally filtered by language."""
    endpoint = f"/api/student/course-videos?subject={requests.utils.quote(subject)}"
    if lang and str(lang).strip():
        endpoint = f"{endpoint}&lang={requests.utils.quote(str(lang))}"

    status_code, payload = _get_student_service_json(endpoint, timeout=10)
    if status_code == 200:
        return payload.get("resources") or payload.get("videos") or []

    print(
        f"_get_course_videos failed via student_service: "
        f"subject={subject} lang={lang} status={status_code} payload={payload}"
    )
    return []


def _list_uploaded_resources(subject, folder_name):
    """List uploaded files for a given subject and folder."""
    subject_folder = os.path.join(app.config['UPLOAD_FOLDER'], folder_name, subject)
    if not os.path.exists(subject_folder):
        return []

    files = []
    for filename in sorted(os.listdir(subject_folder)):
        if filename.startswith('.'):
            continue
        full_path = os.path.join(subject_folder, filename)
        if not os.path.isfile(full_path):
            continue
        files.append(
            {
                "name": filename,
                "url": url_for(
                    'serve_uploaded_file',
                    subpath=f"{folder_name}/{subject}",
                    filename=filename,
                    _external=False,
                ),
            }
        )
    return files


@app.route('/api/resources/<path:subject>/<resource_type>')
def api_resources_by_type(subject, resource_type):
    """Unified resources endpoint for videos/ppt/code/interview."""
    if session.get("login_type") != "user":
        return jsonify({"success": False, "message": "Please login as user"}), 401

    usertype = (session.get("usertype") or "").lower()
    if usertype not in ["admin", "administrator"] and not user_has_completed_resources_registration(session.get("user_id")):
        return jsonify({
            "success": False,
            "message": "Please complete ChakoraHub Register to access resources."
        }), 403

    normalized_type = (resource_type or '').strip().lower()
    if normalized_type in {'ppt', 'ppts'}:
        resources = _list_uploaded_resources(subject, 'ppt')
    elif normalized_type in {'code', 'codes'}:
        resources = _list_uploaded_resources(subject, 'code')
    elif normalized_type in {'interview', 'interviews', 'interview_questions'}:
        resources = _list_uploaded_resources(subject, 'interview_questions')
    elif normalized_type in {'video', 'videos'}:
        lang = (request.args.get('lang') or '').strip()
        resources = _get_course_videos(subject, lang=lang if lang else None)
    else:
        return jsonify({"success": False, "message": "Invalid resource type"}), 400

    return jsonify(
        {
            "success": True,
            "subject": subject,
            "type": normalized_type,
            "resources": resources,
        }
    )


def _asset_master_defaults(master_payload):
    payload = master_payload or {}
    return {
        "asset_types": payload.get("asset_types", []),
        "asset_models": payload.get("asset_models", []),
        "asset_serials": payload.get("asset_serials", []),
        "asset_vendors": payload.get("asset_vendors", []),
        "users": payload.get("users", []),
    }

# Hub Store Flask Proxy routes

@app.route("/shop")
#@require_usertype(*SHOP_ALLOWED_TYPES)
def shop():
    """Serve the ChakoraHub Shopping Cart storefront."""
    return render_template(
        "chakora_shop.html",
        payment_key_id=(os.getenv("RZP_KEY_ID") or "").strip(),
        user_name=(session.get('user') or "").strip(),
        user_email=(session.get('email') or "").strip(),
        user_phone=(session.get('phone') or "").strip(),
    )

@app.route("/api/shop/courses", methods=["GET"])
def shop_courses():
    """Return shop course catalogue from Flask /api/courses."""
    courses_resp = get_courses()
    courses = courses_resp.get_json(silent=True) or []
    return jsonify({"success": True, "courses": courses}), 200


@app.route("/api/shop/checkout", methods=["POST"])
def shop_checkout():
    """Publish a shop order event and return the generated IDs."""
    payload = request.get_json(silent=True) or {}
    order_id = str(payload.get("order_id") or f"SHOP-{uuid.uuid4().hex[:10].upper()}")
    payment_id = str(payload.get("payment_id") or f"PAY-{uuid.uuid4().hex[:10].upper()}")
    billing = payload.get("billing") or {}
    items = payload.get("items") or []
    financials = payload.get("financials") or {}
    payment_method = str(payload.get("payment_method") or "Razorpay").upper()
    timestamp = datetime.utcnow().isoformat()

    cache_set(f"shop:order:{order_id}", {
        "order_id": order_id,
        "payment_id": payment_id,
        "order_status": "PENDING",
        "payment_status": "PENDING",
        "billing": billing,
        "items": items,
        "financials": financials,
        "payment_method": payment_method,
        "updated_at": timestamp,
    }, ttl=CACHE_TTL_LONG)

    kafka_publish("order.placed", {
        "event_id": str(uuid.uuid4()),
        "event_type": "order.placed",
        "correlation_id": order_id,
        "timestamp": timestamp,
        "order_id": order_id,
        "payment_id": payment_id,
        "user_id": payload.get("user_id"),
        "billing": billing,
        "financials": financials,
        "items": items,
        "payment_method": payment_method,
    })
    kafka_publish("shop_payment.created", {
        "event_id": str(uuid.uuid4()),
        "event_type": "shop_payment.created",
        "correlation_id": order_id,
        "timestamp": timestamp,
        "order_id": order_id,
        "payment_id": payment_id,
        "user_id": payload.get("user_id"),
        "amount": financials.get("final_total"),
        "currency": payload.get("currency") or "INR",
        "payment_status": "PENDING",
    })

    return jsonify({
        "success": True,
        "status": "queued",
        "order_id": order_id,
        "payment_id": payment_id,
        "message": "Order queued for Kafka processing",
    }), 202


@app.route("/api/shop/payment/webhook", methods=["POST"])
def shop_payment_webhook():
    """
    Proxy: payment gateway → billing_service POST /payment/webhook

    Also verifies the Razorpay signature (same RZP_KEY_SECRET used across
    the app — see _verify_razorpay_signature / /create-payment-order) before
    trusting a SUCCESS claim, and — once the order is confirmed — sends an
    order-confirmation email using the same visual template as the
    Registration page's confirmation email, populated with this order's
    items/billing/financials.
    """
    payload = request.get_json(silent=True) or {}

    order_id        = payload.get("order_id")
    payment_id      = payload.get("payment_id")
    claimed_status  = str(payload.get("payment_status") or "").strip().upper()
    rzp_order_id    = str(payload.get("razorpay_order_id") or "").strip()
    rzp_payment_id  = str(payload.get("razorpay_payment_id") or "").strip()
    rzp_signature   = str(payload.get("razorpay_signature") or "").strip()

    payment_status = claimed_status
    failure_reason = payload.get("failure_reason")

    if claimed_status == "SUCCESS":
        signature_ok = bool(rzp_order_id and rzp_payment_id and rzp_signature) and \
            _verify_razorpay_signature(rzp_order_id, rzp_payment_id, rzp_signature)
        if not signature_ok:
            print(f"❌ /api/shop/payment/webhook: signature verification failed for order {order_id}")
            payment_status = "FAILED"
            failure_reason = "Payment signature verification failed."

    kafka_publish("shop_payment.completed", {
        "event_id": str(uuid.uuid4()),
        "event_type": "shop_payment.completed",
        "correlation_id": order_id,
        "timestamp": datetime.utcnow().isoformat(),
        "order_id": order_id,
        "payment_id": payment_id,
        "payment_status": payment_status,
        "gateway_ref": rzp_payment_id or payload.get("gateway_ref"),
        "upi_txn_id": payload.get("upi_txn_id"),
        "failure_reason": failure_reason,
        "billing": payload.get("billing") or {},
        "financials": payload.get("financials") or {},
        "items": payload.get("items") or [],
        "payment_method": payload.get("payment_method"),
    })

    cached = cache_get(f"shop:order:{order_id}") or {"order_id": order_id}
    cached.update({
        "payment_id": payment_id,
        "payment_status": payment_status,
        "order_status": "CONFIRMED" if payment_status == "SUCCESS" else "FAILED",
        "updated_at": datetime.utcnow().isoformat(),
    })
    cache_set(f"shop:order:{order_id}", cached, ttl=CACHE_TTL_LONG)

    if claimed_status == "SUCCESS" and payment_status == "FAILED":
        # The caller claimed success but the signature didn't check out — never
        # report success to the frontend, regardless of what billing_service did.
        return jsonify({"status": "error", "detail": failure_reason}), 400

    return jsonify({
        "success": True,
        "status": payment_status,
        "order_id": order_id,
        "payment_id": payment_id,
        "message": "Payment event queued for Kafka processing",
    }), 202


@app.route("/api/shop/order/<order_id>", methods=["GET"])
def shop_order_status(order_id):
    """Return cached shop order state updated from Kafka events."""
    cached = cache_get(f"shop:order:{order_id}")
    if cached:
        return jsonify({"success": True, "order": cached}), 200
    return jsonify({
        "success": True,
        "order": {
            "order_id": order_id,
            "order_status": "PENDING",
            "payment_status": "PENDING",
        },
    }), 200

# @app.route("/cart/action", methods=["POST"])
# def cart_action():
#     """
#     Frontend analytics → two destinations, both fire-and-forget:
#       1. pricing_service (Flask proxy on PRICING_SERVICE_URL) → DynamoDB
#       2. Kafka topic "shop.cart" — same topic/payload shape billing_service.py's
#          own /cart/action route publishes, so any downstream consumer of
#          shop.cart sees a consistent event regardless of which producer fired.

#     This is fire-and-forget analytics; we always return 200 to the frontend so
#     a DynamoDB/Kafka hiccup never breaks the shopping UX.
#     """
#     data = request.get_json(silent=True) or {}

#     # ── Kafka: publish shop.cart (non-blocking, never raises) ────────────
#     kafka_publish("shop.cart", {
#         "event_id":    str(uuid.uuid4()),
#         "event_type":  data.get("action_type"),
#         "timestamp":   datetime.utcnow().isoformat() + "Z",
#         "session_id":  data.get("session_id"),
#         "user_id":     data.get("user_id"),
#         "course_id":   data.get("course_id"),
#         "qty":         data.get("qty"),
#         "price":       data.get("price"),
#         "metadata":    data.get("metadata") or {},
#     })

#     try:
#         resp = requests.post(
#             f"{BILLING_SERVICE_URL}/cart/action",
#             json=data,
#             timeout=5,
#         )
#         return jsonify(resp.json()), resp.status_code
#     except Exception as e:
#         # Non-fatal — analytics failure must never surface to the user
#         print(f"[cart_action] pricing service unavailable (non-fatal): {e}")
#         return jsonify({"status": "ok", "note": "analytics queued"}), 200

# Asset Flask Proxy routes

@app.route("/asset/register", methods=["GET", "POST"])
def register_asset():
    if request.method == "POST":
        payload = request.get_json(silent=True)
        if payload is None:
            payload = request.form.to_dict(flat=True)
        status_code, response_data = _employee_asset_service_request(
            "POST", "/asset/register", json_body=payload
        )
        return jsonify(response_data), status_code

    return render_template("asset_register.html")


@app.route("/asset/<asset_id>", methods=["GET"])
def proxy_asset_get(asset_id):
    status_code, response_data = _employee_asset_service_request("GET", f"/asset/{asset_id}")
    return jsonify(response_data), status_code


@app.route("/assets/list", methods=["GET"])
def proxy_assets_list():
    status_code, response_data = _employee_asset_service_request(
        "GET",
        "/assets/list",
        params={
            "status": request.args.get("status"),
            "assigned_to": request.args.get("assigned_to"),
            "type_id": request.args.get("type_id"),
            "limit": request.args.get("limit"),
            "offset": request.args.get("offset"),
        },
    )
    return jsonify(response_data), status_code


@app.route("/asset/<asset_id>", methods=["DELETE"])
def proxy_asset_delete(asset_id):
    status_code, response_data = _employee_asset_service_request(
        "DELETE",
        f"/asset/{asset_id}",
        params={"actioned_by": request.args.get("actioned_by")},
    )
    return jsonify(response_data), status_code


@app.route("/assets/employee/<employee_id>", methods=["GET"])
def proxy_assets_by_employee(employee_id):
    status_code, response_data = _employee_asset_service_request(
        "GET", f"/assets/employee/{employee_id}"
    )
    return jsonify(response_data), status_code


@app.route("/assets/stats", methods=["GET"])
def proxy_assets_stats():
    status_code, response_data = _employee_asset_service_request("GET", "/assets/stats")
    return jsonify(response_data), status_code


@app.route("/asset/tracker")
def asset_tracker():
    if not _has_employee_admin_access():
        flash("Admin access required", "error")
        return redirect(url_for("home"))

    # AJAX pattern: render shell only; data is fetched client-side via /api/asset/tracker.
    return render_template("asset_tracker.html")


@app.route("/api/asset/tracker", methods=["GET"])
def api_asset_tracker():
    """JSON endpoint for the asset tracker page.
    TODO: migrate to the asset microservice (currently proxied through Flask)."""
    if not _has_employee_admin_access():
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    status_code, response_data = _asset_service_request("GET", "/api/assets/tracker")
    if status_code != 200:
        return jsonify({
            "success": False,
            "message": response_data.get("message") or "Unable to load asset tracker.",
            "asset_types": [],
            "assets": [],
            "audit_log": [],
            "stats": {},
            "today": date.today().isoformat(),
        }), status_code

    return jsonify({
        "success": True,
        "asset_types": response_data.get("asset_types", []),
        "assets": response_data.get("assets", []),
        "audit_log": response_data.get("audit_log", []),
        "stats": response_data.get("stats", {}),
        "today": date.today().isoformat(),
    }), 200


@app.route("/asset/edit/<asset_id>", methods=["GET", "POST"])
def edit_asset(asset_id):
    if request.method == "POST":
        payload = {
            "TYPE_ID": request.form.get("TYPE_ID"),
            "MODEL_ID": request.form.get("MODEL_ID"),
            "SERIAL_ID": request.form.get("SERIAL_ID"),
            "NEW_SERIAL_NO": request.form.get("NEW_SERIAL_NO"),
            "VENDOR_ID": request.form.get("VENDOR_ID"),
            "OS": request.form.get("OS"),
            "RAM": request.form.get("RAM"),
            "SSD": request.form.get("SSD"),
            "OWNED_BY": request.form.get("OWNED_BY"),
            "LOCATION": request.form.get("LOCATION"),
            "PURCHASE_DATE": request.form.get("PURCHASE_DATE"),
            "WARRANTY_EXPIRY": request.form.get("WARRANTY_EXPIRY"),
            "PRICE": request.form.get("PRICE"),
            "CONDITION": request.form.get("CONDITION"),
            "STATUS": request.form.get("STATUS"),
            "NOTES": request.form.get("NOTES"),
            "ACTIONED_BY": session.get("employee_id") or session.get("user") or "system",
        }

        status_code, response_data = _asset_service_request("PUT", f"/api/assets/{asset_id}", json_body=payload)
        if status_code == 200 and response_data.get("success"):
            flash(response_data.get("message") or f"Asset {asset_id} updated.", "success")
            return redirect(url_for("asset_tracker"))

        flash(response_data.get("message") or f"Failed to update asset {asset_id}.", "error")

    master_status, master_data = _asset_service_request("GET", "/api/assets/master-data")
    if master_status != 200:
        master_data = {}
        flash("Unable to fetch master data for asset edit.", "error")

    asset_status, asset_data = _asset_service_request("GET", f"/api/assets/{asset_id}")
    if asset_status != 200 or not asset_data.get("success"):
        flash(asset_data.get("message") or f"Asset {asset_id} not found.", "error")
        return redirect(url_for("asset_tracker"))

    entered_data = asset_data.get("asset") or {}
    return render_template(
        "asset_register.html",
        **_asset_master_defaults(master_data),
        entered=entered_data,
        form_action=url_for("edit_asset", asset_id=asset_id),
    )


@app.route("/asset/retire/<asset_id>", methods=["POST"])
def retire_asset(asset_id):
    payload = {"ACTIONED_BY": session.get("employee_id") or session.get("user") or "system"}
    status_code, response_data = _asset_service_request("POST", f"/api/assets/{asset_id}/retire", json_body=payload)
    return jsonify(response_data), status_code


@app.route("/asset/assign/<asset_id>")
def assign_asset(asset_id):
    flash(f"Asset assignment flow for {asset_id} will be added in a follow-up step.", "error")
    return redirect(url_for("asset_tracker"))

# ====== Privacy policy route (static page) ======
@app.route("/privacy-policy")
def privacy_policy():
    return render_template("privacy_policy.html")


# ================= SIMPLIFIED LOGIN PROXY (delegates auth to home_service) =================
@app.route("/nrm_logins", methods=["POST"])
def nrm_logins():
    """Proxy login route - calls home_service microservice"""
    start_time = time.time()
    route_start = time.perf_counter()
    
    username = (
        request.form.get("username", "").strip()
        or request.form.get("employee_id", "").strip()
        or request.form.get("email_or_phone", "").strip()
        or request.form.get("admin_username", "").strip()
    )
    password = request.form.get("password", "").strip()
    login_type = request.form.get("login_type", "user").lower()
    employee_id = request.form.get("employee_id", "").strip() or username

    print(f"🔐 Login proxy attempt - Type: {login_type}, Username: {username}")
    print(f"🔐 Login proxy payload keys: {sorted(list(request.form.keys()))}")
    print(f"⏱️ [/nrm_logins] payload parse took {time.perf_counter() - route_start:.3f}s")

    try:
        # Call home_service microservice
        home_login_start = time.perf_counter()
        print(f"➡️ Calling home_service /home/login | url={HOME_SERVICE_URL}/home/login")
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.post(
                f"{HOME_SERVICE_URL}/home/login",
                json={
                    "username": username,
                    "password": password,
                    "login_type": login_type,
                    "employee_id": employee_id
                },
                timeout=45,
                allow_redirects=False,
            )
            print(f"⏱️ [/nrm_logins] home_service call took {time.perf_counter() - home_login_start:.3f}s")
        
        print(f"   Microservice response: {response.status_code}")
        print(f"   Microservice body preview: {response.text[:300]}")
        
        if response.status_code == 200:
            parse_start = time.perf_counter()
            data = response.json()
            print(f"⏱️ [/nrm_logins] response parse took {time.perf_counter() - parse_start:.3f}s")
            print(f"   Parsed login response keys: {sorted(list(data.keys()))}")
            
            if data.get("success"):
                # ========== USER LOGIN SUCCESS ==========
                if login_type == "user":
                    user = data.get("user", {})
                    print(f"✅ Login proxy user payload: user_keys={sorted(list(user.keys())) if isinstance(user, dict) else type(user).__name__}")
                    session.clear()
                    session["login_type"] = "user"
                    session["user_id"] = user.get("id")
                    session["user"] = user.get("username") or user.get("email") or user.get("phone")
                    session["usertype"] = user.get("usertype", "student")
                    
                    pic = user.get("profile_pic")
                    session["profile_pic"] = pic if pic and pic.startswith("http") else "https://chakorahub-student-s3.s3.eu-north-1.amazonaws.com/defaultpicture.jpg"
                    session.permanent = True
                    session.modified = True

                    usertype = (session.get("usertype") or "").lower()
                    paid_check_start = time.perf_counter()
                    if usertype not in ["admin", "administrator"] and not user_has_paid_access(session.get("user_id")):
                        print(f"⏱️ [/nrm_logins] paid access check took {time.perf_counter() - paid_check_start:.3f}s")
                        flash("Please complete your registration payment before accessing resources.", "error")
                        return redirect(url_for("register"), code=303)
                    print(f"⏱️ [/nrm_logins] paid access check took {time.perf_counter() - paid_check_start:.3f}s")
                    
                    print(f"✅ User login SUCCESS: {session['user']}")
                    print(f"🎯 Total login time: {time.time() - start_time:.2f}s")
                    print(f"🎯 [/nrm_logins] total route time {time.perf_counter() - route_start:.3f}s")
                    resp = redirect(url_for("resources"))
                    return resp
                # ========== EMPLOYEE LOGIN SUCCESS ==========
                elif login_type == "employee":
                    emp = data.get("employee", {})
                    print(f"✅ Login proxy employee payload: emp_keys={sorted(list(emp.keys())) if isinstance(emp, dict) else type(emp).__name__}")
                    session.clear()
                    normalized_emp_id = str(emp.get("employee_id") or "").strip().upper()
                    session["login_type"] = "employee"
                    session["employee_id"] = normalized_emp_id
                    session["employee_name"] = emp.get("employee_name")
                    session["employee_email"] = emp.get("email")
                    session["employee_admin_access"] = normalized_emp_id == "CH25006"
                    session.permanent = True
                    
                    print(f"✅ Employee login SUCCESS: {session['employee_id']}")
                    print(f"🎯 Total login time: {time.time() - start_time:.2f}s")
                    print(f"🎯 [/nrm_logins] total route time {time.perf_counter() - route_start:.3f}s")
                    return redirect(url_for("employee_resources"))
            else:
                # Login failed
                message = data.get("message", "Login failed")
                print(f"❌ Login failed: {message}")
                print(f"🎯 [/nrm_logins] total route time {time.perf_counter() - route_start:.3f}s")
                flash(message, "error")
                return redirect(url_for("home"))
        
        elif response.status_code == 401:
            # Invalid credentials
            data = response.json()
            print(f"❌ Invalid credentials: {data.get('message')}")
            print(f"🎯 [/nrm_logins] total route time {time.perf_counter() - route_start:.3f}s")
            flash("Incorrect password", "error")
            return redirect(url_for("home"))
        
        elif response.status_code == 404:
            # User/Employee not found
            data = response.json()
            print(f"❌ User not found: {data.get('message')}")
            print(f"🎯 [/nrm_logins] total route time {time.perf_counter() - route_start:.3f}s")
            flash("User not found", "error")
            return redirect(url_for("home"))
        
        else:
            # Other error
            upstream_message = ""
            try:
                upstream_message = (response.json() or {}).get("message") or ""
            except Exception:
                upstream_message = ""

            print(
                f"❌ Microservice error: {response.status_code} "
                f"body={response.text[:300]}"
            )
            print(f"🎯 [/nrm_logins] total route time {time.perf_counter() - route_start:.3f}s")
            flash(upstream_message or "Login service unavailable", "error")
            return redirect(url_for("home"))
            
    except requests.exceptions.Timeout:
        print("❌ Microservice timeout")
        print(f"🎯 [/nrm_logins] total route time {time.perf_counter() - route_start:.3f}s")
        flash("Login service timed out. Please try again.", "error")
        return redirect(url_for("home"))
        
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to login service")
        print(f"🎯 [/nrm_logins] total route time {time.perf_counter() - route_start:.3f}s")
        flash("Login service unavailable", "error")
        return redirect(url_for("home"))
        
    except Exception as e:
        print(f"❌ Login error: {e}")
        print(f"🎯 [/nrm_logins] total route time {time.perf_counter() - route_start:.3f}s")
        import traceback
        traceback.print_exc()
        flash("Login error. Please try again.", "error")
        return redirect(url_for("home"))

@app.route("/logout")
def logout():
    """User logout - calls home_service to update IS_ACTIVE."""
    user_id = session.get("user_id") or session.get("track_user_id")
    print(f"🔐 Logout hit: user_id={user_id} | session_keys={list(session.keys())}")
    _sync_logout_state("user", user_id=user_id, reason="manual-logout")
    
    session.clear()
    flash("Logged out successfully", "success")
    return redirect(url_for("home"))


@app.route('/api/student/dashboard-data', methods=['GET'])
def student_dashboard_data():
    """Debug/proxy endpoint to trigger student_service stored-proc backed dashboard."""
    if session.get("login_type") != "user":
        return jsonify({"success": False, "message": "Please login as user"}), 401

    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"success": False, "message": "Missing session user_id"}), 400

    force_refresh = str(request.args.get("force_refresh", "false")).strip().lower() in {"1", "true", "yes", "y"}

    try:
        response = requests.post(
            f"{STUDENT_SERVICE_URL}/api/student/dashboard",
            json={"user_id": int(user_id), "force_refresh": force_refresh},
            timeout=15,
        )

        try:
            payload = response.json()
        except Exception:
            payload = {"success": False, "message": response.text}

        print(
            f"📊 Dashboard proxy called for user_id={user_id}, force_refresh={force_refresh}, "
            f"status={response.status_code}, from_cache={payload.get('from_cache')}"
        )
        return jsonify(payload), response.status_code
    except Exception as exc:
        print(f"❌ Dashboard proxy error for user_id={user_id}: {exc}")
        return jsonify({"success": False, "message": str(exc)}), 500


# =========================================================
# FORGOT PASSWORD + OTP (EMPLOYEE + USER)
# =========================================================

@app.route("/forgot-password", methods=["GET","POST"])
def forgot_password():

    def _forgot_password_request_value(*keys):
        json_payload = request.get_json(silent=True) or {}
        if isinstance(json_payload, dict):
            for key in keys:
                value = json_payload.get(key)
                if value:
                    return str(value)

        try:
            for key in keys:
                value = request.form.get(key)
                if value:
                    return str(value)
        except Exception as form_exc:
            print(f"⚠️ forgot-password form parsing issue: {form_exc}")

        for key in keys:
            value = request.args.get(key)
            if value:
                return str(value)

        raw_body = request.get_data(cache=True, as_text=True) or ""
        if raw_body:
            parsed_body = urllib.parse.parse_qs(raw_body, keep_blank_values=True)
            for key in keys:
                values = parsed_body.get(key)
                if values and values[0]:
                    return str(values[0])

        return ""

    if request.method == "POST":
        try:
            login_type = (
                _forgot_password_request_value("login_type", "type")
            ).strip().lower()
            username = (
                _forgot_password_request_value("username", "email")
            ).strip()

            print(
                "🔐 forgot-password request | "
                f"method={request.method} content_type={request.content_type} "
                f"login_type={login_type!r} username={username!r}"
            )

            if login_type not in {"user", "employee"}:
                return render_template(
                    "forgot_password.html",
                    error_message="Please select a valid account type.",
                    selected_login_type=login_type,
                    entered_username=username,
                ), 400
            if not username:
                return render_template(
                    "forgot_password.html",
                    error_message="Please enter your registered email.",
                    selected_login_type=login_type,
                    entered_username=username,
                ), 400

            canonical_host = (globals().get("CANONICAL_HOST") or "").strip().lower()
            reset_base_url = f"https://{canonical_host}" if canonical_host else request.url_root.rstrip("/")

            with requests.Session() as internal_session:
                internal_session.trust_env = False
                internal_session.proxies = {"http": None, "https": None}
                response = internal_session.post(
                    f"{HOME_SERVICE_URL}/home/forgot-password",
                    json={
                        "login_type": login_type,
                        "username": username,
                        "reset_base_url": reset_base_url,
                    },
                    timeout=12,
                )

            try:
                payload = response.json()
            except Exception:
                payload = {
                    "success": False,
                    "message": (response.text or "").strip() or "Upstream forgot-password failed",
                }

            message = payload.get("message") or "Request processed"
            if response.status_code == 200:
                return render_template(
                    "forgot_password.html",
                    success_message=message,
                    selected_login_type=login_type,
                    entered_username=username,
                ), 200

            return render_template(
                "forgot_password.html",
                error_message=message,
                selected_login_type=login_type,
                entered_username=username,
            ), response.status_code
        except Exception as exc:
            print(f"❌ forgot-password proxy error: {exc}")
            return render_template(
                "forgot_password.html",
                error_message=f"Server error: {exc}",
            ), 500

    return render_template("forgot_password.html")

@app.route("/reset-password/<token>", methods=["GET","POST"])
def reset_password(token):
    if request.method == "POST":
        new_password = (request.form.get("password") or "").strip()
        if not new_password:
            return "Password required", 400

        try:
            with requests.Session() as internal_session:
                internal_session.trust_env = False
                internal_session.proxies = {"http": None, "https": None}
                response = internal_session.post(
                    f"{HOME_SERVICE_URL}/home/reset-password/{token}",
                    json={"password": new_password},
                    timeout=12,
                )

            if response.status_code == 200:
                return redirect(url_for("home"))

            try:
                payload = response.json()
                message = payload.get("message") or "Password reset failed"
            except Exception:
                message = "Password reset failed"
            return message, response.status_code
        except Exception as exc:
            print(f"❌ reset-password proxy error: {exc}")
            return "Server error", 500

    try:
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.get(
                f"{HOME_SERVICE_URL}/home/reset-password/validate",
                params={"token": token},
                timeout=10,
            )

        response_preview = (response.text or "").replace("\n", " ").strip()[:300]
        print(
            "🔎 reset-password validate upstream | "
            f"status={response.status_code} token_len={len(token)} preview={response_preview!r}"
        )

        if response.status_code != 200:
            try:
                payload = response.json()
                return payload.get("message") or "Invalid reset link", response.status_code
            except Exception:
                return "Invalid reset link", response.status_code

        try:
            payload = response.json()
        except Exception:
            print("❌ reset-password validate returned non-JSON on 200")
            return "Invalid reset link", 400

        if not isinstance(payload, dict):
            print(f"❌ reset-password validate returned non-object payload: {type(payload)}")
            return "Invalid reset link", 400

        if not payload.get("success"):
            return payload.get("message") or "Invalid reset link", 400

        try:
            return render_template("reset_password.html", email=payload.get("email", ""))
        except TemplateNotFound:
            print("⚠️ reset_password.html not found; serving inline fallback form")
            return render_template_string(
                """
                <!doctype html>
                <html lang="en">
                <head>
                    <meta charset="utf-8" />
                    <meta name="viewport" content="width=device-width, initial-scale=1" />
                    <title>Reset Password</title>
                    <style>
                        body { font-family: Arial, sans-serif; margin: 2rem; max-width: 520px; }
                        .field { margin: 0.75rem 0; }
                        input, button { width: 100%; padding: 0.75rem; font-size: 1rem; }
                        button { cursor: pointer; }
                    </style>
                </head>
                <body>
                    <h2>Reset Password</h2>
                    <p>Account: {{ email }}</p>
                    <form method="post">
                        <div class="field">
                            <input type="password" name="password" placeholder="Enter new password" required />
                        </div>
                        <button type="submit">Update Password</button>
                    </form>
                </body>
                </html>
                """,
                email=payload.get("email", ""),
            )
    except Exception as exc:
        print(f"❌ reset-password validate proxy error: {exc}")
        return "Server error", 500


# ================= HEALTH ===========================
@app.route("/health")
def health():
    return jsonify({"service": "proxy", "status": "healthy"})
@app.route('/api/home/feedback')
def home_feedback():
    feedbacks = []
    conn = get_db_connection()

    if not conn:
        return jsonify({"feedbacks": []})

    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            FEEDBACK_MESSAGE,
            COALESCE(NAME, 'Anonymous')
        FROM NRM_FEEDBACK
        WHERE FEEDBACK_MESSAGE IS NOT NULL
        ORDER BY SUBMITTED_AT DESC
        LIMIT 5
    """)

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    for r in rows:
        feedbacks.append({
            "message": r[0],
            "username": r[1]
        })

    return jsonify({"feedbacks": feedbacks})



# ==========================================
# EMPLOYEE ROUTES - DIRECT DATABASE ACCESS
# ==========================================
# Then your routes should be:
@app.route("/employee-resources")
def employee_resources():
    """Employee resources page - Direct database access.

    This route runs ~15 sequential Snowflake queries to build the view bundle
    (personal, job, dept/desig/manager, salary, bank, leaves, ID card, queries,
    appraisal, history, profile pic, festival).
    """
    if session.get("login_type") != "employee":
        session.pop("last_visited_path", None)
        session.modified = True
        flash("Please login as employee first", "error")
        return redirect(url_for("home"))

    employee_id = session.get("employee_id")

    if not employee_id:
        session.clear()
        flash("Session expired. Please login again.", "error")
        return redirect(url_for("home"))

    try:
        conn = get_db_connection()
        if not conn:
            flash("Database connection failed", "error")
            return redirect(url_for("home"))
            
        cursor = conn.cursor(DICT_CURSOR)
        
        # Get today's festival (Oracle-safe date bind)
        today = datetime.now().date()
        festival_today = None
        cursor.execute(
            "SELECT FESTIVAL_NAME FROM EMP_NRM_FESTIVALS WHERE TRUNC(FESTIVAL_DATE) = TRUNC(%s)",
            (today,)
        )
        festival_row = cursor.fetchone()
        if festival_row:
            festival_today = festival_row['FESTIVAL_NAME']
        
        # Single query to EMPLOYEE_REGISTRATIONS (same table ID card uses - all data is here)
        employee_data = {}
        cursor.execute("""
            SELECT
                FIRST_NAME, LAST_NAME, FULL_NAME,
                EMAIL, PHONE, GENDER,
                DATE_OF_BIRTH      AS DOB,
                ADDRESS            AS CURRENT_ADDRESS,
                PERSONAL_LOCATION  AS PERMANENT_ADDRESS,
                EMERGENCY_CONTACT_NAME,
                EMERGENCY_CONTACT_PHONE,
                DEPARTMENT_NAME    AS DEPARTMENT,
                DESIGNATION_TITLE  AS DESIGNATION,
                MANAGER_NAME       AS MANAGER,
                JOINING_DATE       AS DATE_OF_JOINING,
                EMPLOYMENT_TYPE,
                WORK_LOCATION_NAME AS WORK_LOCATION,
                STATUS
            FROM EMPLOYEE_REGISTRATIONS
            WHERE EMPLOYEE_ID = %s
            LIMIT 1
        """, (employee_id,))
        reg_row = cursor.fetchone()
       
        if reg_row:
            employee_data = {
                'first_name': reg_row.get('FIRST_NAME'),
                'last_name': reg_row.get('LAST_NAME'),
                'full_name': reg_row.get('FULL_NAME'),
                'email': reg_row.get('EMAIL'),
                'phone': reg_row.get('PHONE'),
                'gender': reg_row.get('GENDER'),
                'dob': reg_row.get('DOB'),
                'current_address': reg_row.get('CURRENT_ADDRESS'),
                'permanent_address': reg_row.get('PERMANENT_ADDRESS'),
                'department': reg_row.get('DEPARTMENT'),
                'designation': reg_row.get('DESIGNATION'),
                'manager': reg_row.get('MANAGER'),
                'date_of_joining': reg_row.get('DATE_OF_JOINING'),
                'employment_type': reg_row.get('EMPLOYMENT_TYPE'),
                'work_location': reg_row.get('WORK_LOCATION'),
                'status': reg_row.get('STATUS'),
            }
    
        # Profile pic from EMP_NRM_PERSONAL
        cursor.execute(
            "SELECT PROFILE_PIC FROM EMP_NRM_PERSONAL WHERE EMPLOYEE_ID = %s LIMIT 1",
            (employee_id,)
        )
        pic_row = cursor.fetchone()

        def fmt_date(val):
            if not val:
                return 'Not specified'
            try:
                return val.strftime('%Y-%m-%d')
            except Exception:
                return str(val)

        if reg_row:
            full_name = (
                reg_row.get('FULL_NAME')
                or f"{reg_row.get('FIRST_NAME', '')} {reg_row.get('LAST_NAME', '')}".strip()
                or session.get("employee_name", "Employee")
            )
            employee_data = {
                'full_name':               full_name,
                'employee_id':             employee_id,
                'dob':                     fmt_date(reg_row.get('DOB')),
                'gender':                  reg_row.get('GENDER')        or 'Not specified',
                'email':                   reg_row.get('EMAIL')         or session.get("employee_email", "Not specified"),
                'phone':                   reg_row.get('PHONE')         or 'Not specified',
                'department':              reg_row.get('DEPARTMENT')    or 'N/A',
                'designation':             reg_row.get('DESIGNATION')   or 'N/A',
                'date_of_joining':         fmt_date(reg_row.get('DATE_OF_JOINING')),
                'manager':                 reg_row.get('MANAGER')       or 'Not specified',
                'current_address':         reg_row.get('CURRENT_ADDRESS')   or 'Not specified',
                'permanent_address':       reg_row.get('PERMANENT_ADDRESS') or 'Not specified',
                'emergency_contact_name':  reg_row.get('EMERGENCY_CONTACT_NAME')  or 'Not specified',
                'emergency_contact_phone': reg_row.get('EMERGENCY_CONTACT_PHONE') or 'Not specified',
                'employment_type':         reg_row.get('EMPLOYMENT_TYPE') or 'Not specified',
                'work_location':           reg_row.get('WORK_LOCATION')   or 'Not specified',
            }
        else:
            employee_data = {
                'full_name':               session.get("employee_name", "Employee"),
                'employee_id':             employee_id,
                'dob':                     'Not specified',
                'gender':                  'Not specified',
                'email':                   session.get("employee_email", "Not specified"),
                'phone':                   'Not specified',
                'department':              session.get("employee_department", "N/A"),
                'designation':             session.get("employee_designation", "N/A"),
                'date_of_joining':         'Not specified',
                'manager':                 'Not specified',
                'current_address':         'Not specified',
                'permanent_address':       'Not specified',
                'emergency_contact_name':  'Not specified',
                'emergency_contact_phone': 'Not specified',
                'employment_type':         'Not specified',
                'work_location':           'Not specified',
            }
        
        # Fetch salary information from EMP_NRM_SALARY
        salary_data = {}
        cursor.execute("""
            SELECT 
                BASIC, HRA, ALLOWANCES,
                DEDUCTIONS, NET_SALARY
            FROM EMP_NRM_SALARY 
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        salary_row = cursor.fetchone()
        
        if salary_row:
            basic = salary_row.get('BASIC', 0) or 0
            hra = salary_row.get('HRA', 0) or 0
            allowances = salary_row.get('ALLOWANCES', 0) or 0
            deductions = salary_row.get('DEDUCTIONS', 0) or 0
            net_salary = salary_row.get('NET_SALARY', 0) or 0
            
            # Calculate gross salary
            gross_salary = basic + hra + allowances
            
            # Fetch bank details from EMP_NRM_PROFILE
            
            bank_info = {}
            bank_details = "Not specified"
            try:
                cursor.execute(
                    "SELECT BANK_DETAILS FROM EMP_NRM_PROFILE WHERE EMP_ID = %s",
                    (employee_id,)
                )

                profile_row = cursor.fetchone()

                if profile_row and profile_row.get('BANK_DETAILS'):
                    bank_details = profile_row.get('BANK_DETAILS')

            except Exception as e:
                print(f"EMP_NRM_PROFILE fetch skipped: {e}")
            salary_data = {
                'basic_salary': f"₹{basic:,.2f}",
                'hra': f"₹{hra:,.2f}",
                'special_allowance': f"₹{allowances:,.2f}",
                'transport_allowance': '₹0.00',  # Not in schema
                'medical_allowance': '₹0.00',    # Not in schema
                'gross_salary': f"₹{gross_salary:,.2f}",
                'pf': f"₹{deductions/2:,.2f}",  # Assuming half of deductions is PF
                'professional_tax': f"₹{deductions/4:,.2f}",  # Assuming quarter is PT
                'tds': f"₹{deductions/4:,.2f}",  # Assuming quarter is TDS
                'net_salary': f"₹{net_salary:,.2f}",
                'bank_name': 'Not specified',
                'account_number': 'Not specified',
                'ifsc_code': 'Not specified'
            }
            
            # Try to parse bank details (assuming format: Bank Name - Account Number - IFSC)
            if bank_details and ' - ' in bank_details:
                parts = bank_details.split(' - ')
                if len(parts) >= 3:
                    salary_data['bank_name'] = parts[0]
                    salary_data['account_number'] = parts[1]
                    salary_data['ifsc_code'] = parts[2]
        else:
            # Default salary data
            salary_data = {
                'basic_salary': '₹45,000',
                'hra': '₹15,000',
                'special_allowance': '₹10,000',
                'transport_allowance': '₹3,000',
                'medical_allowance': '₹2,000',
                'gross_salary': '₹75,000',
                'pf': '₹5,400',
                'professional_tax': '₹200',
                'tds': '₹8,500',
                'net_salary': '₹60,900',
                'bank_name': 'HDFC Bank',
                'account_number': 'XXXX XXXX 1234',
                'ifsc_code': 'HDFC0001234'
            }
        
        # Fetch leave data from EMP_NRM_LEAVE
        leave_data = {'casual_leave': 12, 'sick_leave': 8, 'privilege_leave': 3}  # Default
        leave_history = []
        
        try:
            # Count approved leaves by type (simple logic)
            cursor.execute("""
                SELECT 
                    STATUS,
                    COUNT(*) as count,
                    SUM(DATEDIFF(day, START_DATE, END_DATE) + 1) as total_days
                FROM EMP_NRM_LEAVE 
                WHERE EMPLOYEE_ID = %s AND STATUS = 'Approved'
                GROUP BY STATUS
            """, (employee_id,))
            leave_stats = cursor.fetchall()
            
            # Fetch leave history
            cursor.execute("""
                SELECT 
                    LEAVE_ID,
                    START_DATE,
                    END_DATE,
                    REASON,
                    STATUS,
                    APPLIED_AT
                FROM EMP_NRM_LEAVE 
                WHERE EMPLOYEE_ID = %s
                ORDER BY APPLIED_AT DESC
                LIMIT 10
            """, (employee_id,))
            leave_history_rows = cursor.fetchall()
            
            for row in leave_history_rows:
                from_date = row.get('START_DATE', 'Unknown')
                to_date = row.get('END_DATE', 'Unknown')
                
                # Calculate days
                days = 1
                if from_date and to_date and from_date != 'Unknown' and to_date != 'Unknown':
                    try:
                        if isinstance(from_date, str):
                            from_date = datetime.strptime(from_date, '%Y-%m-%d')
                        if isinstance(to_date, str):
                            to_date = datetime.strptime(to_date, '%Y-%m-%d')
                        days = (to_date - from_date).days + 1
                    except:
                        days = 1
                
                # Determine leave type from reason
                reason = row.get('REASON', '').lower()
                leave_type = 'Casual'
                if 'sick' in reason:
                    leave_type = 'Sick'
                elif 'privilege' in reason or 'annual' in reason:
                    leave_type = 'Privilege'
                elif 'maternity' in reason or 'paternity' in reason:
                    leave_type = 'Special'
                
                leave_history.append({
                    'type': leave_type,
                    'from_date': row.get('START_DATE', 'Unknown'),
                    'to_date': row.get('END_DATE', 'Unknown'),
                    'days': str(days),
                    'status': row.get('STATUS', 'Pending')
                })
            
        except Exception as e:
            print(f"Error fetching leave data: {e}")
            leave_history = [
                {'type': 'Casual', 'from_date': 'Dec 20, 2025', 'to_date': 'Dec 22, 2025', 'days': '3', 'status': '✅ Approved'},
                {'type': 'Sick', 'from_date': 'Nov 15, 2025', 'to_date': 'Nov 15, 2025', 'days': '1', 'status': '✅ Approved'}
            ]
        
        # ID Card data from EMP_NRM_IDCARD
        id_card_data = {}
        cursor.execute("SELECT ID_NUMBER, ISSUE_DATE, EXPIRY_DATE FROM EMP_NRM_IDCARD WHERE EMPLOYEE_ID = %s", (employee_id,))
        id_card_row = cursor.fetchone()
        
        if id_card_row:
            valid_until = id_card_row.get('EXPIRY_DATE', 'Dec 31, 2026')
            id_card_data = {
                'name': employee_data['full_name'],
                'designation': employee_data['designation'],
                'department': employee_data['department'],
                'employee_id': employee_id,
                'valid_until': valid_until
            }
        else:
            id_card_data = {
                'name': employee_data['full_name'],
                'designation': employee_data['designation'],
                'department': employee_data['department'],
                'employee_id': employee_id,
                'valid_until': 'Dec 31, 2026'
            }
        
        # Employee queries from EMP_NRM_QUERIES
        queries = []
        cursor.execute("""
            SELECT 
                QUERY_ID, QUERY_TEXT, STATUS, CREATED_AT
            FROM EMP_NRM_QUERIES 
            WHERE EMPLOYEE_ID = %s
            ORDER BY CREATED_AT DESC
            LIMIT 5
        """, (employee_id,))
        query_rows = cursor.fetchall()
        
        for row in query_rows:
            # Extract subject from query text (first 50 chars)
            query_text = row.get('QUERY_TEXT', 'No description')
            subject = query_text[:50] + '...' if len(query_text) > 50 else query_text
            
            queries.append({
                'query_id': row.get('QUERY_ID', 'N/A'),
                'subject': subject,
                'date': row.get('CREATED_AT', 'Unknown'),
                'status': row.get('STATUS', 'Pending')
            })
        
        if not queries:
            queries = [
                {'query_id': '#Q001', 'subject': 'Laptop issues', 'date': 'Jan 3, 2026', 'status': '🟡 In Progress'},
                {'query_id': '#Q002', 'subject': 'Leave balance query', 'date': 'Dec 28, 2025', 'status': '✅ Resolved'}
            ]
        
        # Appraisal data from EMP_NRM_APPRAISAL_SUMMARY
        appraisal_data = {}
        appraisal_history = []
        
        cursor.execute("""
            SELECT 
                FINAL_RATING, COMMENTS, APPRAISAL_DATE, SUBMITTED_BY
            FROM EMP_NRM_APPRAISAL_SUMMARY 
            WHERE EMPLOYEE_ID = %s
            ORDER BY APPRAISAL_DATE DESC
            LIMIT 1
        """, (employee_id,))
        appraisal_row = cursor.fetchone()
        
        if appraisal_row:
            rating = appraisal_row.get('FINAL_RATING', 0) or 0
            appraisal_data = {
                'overall_rating': f"{rating} / 5.0",
                'review_period': 'Latest Review',
                'reviewer': appraisal_row.get('SUBMITTED_BY', 'Manager'),
                'feedback': appraisal_row.get('COMMENTS', 'No feedback available.')
            }
            
            # Fetch appraisal history
            cursor.execute("""
                SELECT 
                    FINAL_RATING, APPRAISAL_DATE
                FROM EMP_NRM_APPRAISAL_SUMMARY 
                WHERE EMPLOYEE_ID = %s
                ORDER BY APPRAISAL_DATE DESC
                LIMIT 3
            """, (employee_id,))
            history_rows = cursor.fetchall()
            
            year_counter = datetime.now().year
            for row in history_rows:
                rating_val = row.get('FINAL_RATING', 0) or 0
                increment = min(20, max(5, int(rating_val * 2)))  # Simple increment calculation
                
                appraisal_history.append({
                    'year': str(year_counter),
                    'rating': f"{rating_val} / 5.0",
                    'increment': f"{increment}%",
                    'status': '✅ Completed'
                })
                year_counter -= 1
        
        if not appraisal_data:
            appraisal_data = {
                'overall_rating': '4.5 / 5.0',
                'review_period': 'Jan 2025 - Dec 2025',
                'reviewer': employee_data.get('manager') or 'Manager',
                'feedback': 'Employee has consistently demonstrated exceptional technical skills and dedication to projects.'
            }
            appraisal_history = [
                {'year': '2025', 'rating': '4.5 / 5.0', 'increment': '12%', 'status': '✅ Completed'},
                {'year': '2024', 'rating': '4.2 / 5.0', 'increment': '10%', 'status': '✅ Completed'}
            ]
        
        # Profile data
        profile_data = {
            'username': employee_data['full_name'].lower().replace(' ', '.'),
            'email': employee_data['email'],
            'account_status': '✅ Active',
            'last_login': datetime.now().strftime("%b %d, %Y %I:%M %p")
        }
        
        cursor.close()
        conn.close()
        
        # Profile pic: prefer EMP_NRM_PERSONAL, fall back to session
        profile_pic = (
            (pic_row.get('PROFILE_PIC') if pic_row else None)
            or session.get("profile_pic")
            or "https://chakorahub-student-s3.s3.eu-north-1.amazonaws.com/defaultpicture.jpg"
        )

        bank_data = {
            'has_bank': bool(salary_data.get('bank_name') and salary_data.get('bank_name') != 'Not specified'),
            'bank_name': salary_data.get('bank_name', 'Not specified'),
            'account_number': salary_data.get('account_number', 'Not specified'),
            'ifsc_code': salary_data.get('ifsc_code', 'Not specified'),
        }

        salary_view_data = dict(salary_data)
        salary_view_data['has_salary'] = bool(salary_row)
        salary_view_data['allowances'] = salary_data.get('special_allowance', salary_data.get('allowances', '₹0.00'))

        view_data = {
            "Employee_name": employee_data['full_name'],
            "employee_name": employee_data['full_name'],
            "employee_id": employee_id,
            "employee_email": employee_data.get('email', session.get('employee_email', 'Not specified')),
            "department": employee_data.get('department', session.get('employee_department', 'N/A')),
            "designation": employee_data.get('designation', session.get('employee_designation', 'N/A')),
            "employee_profile_pic": profile_pic,
            "profile_pic": profile_pic,
            "festival_today": festival_today,
            "reg_id": employee_id,
            "employee_data": employee_data,
            "salary_data": salary_view_data,
            "bank_data": bank_data,
            "leave_data": leave_data,
            "leave_history": leave_history,
            "id_card_data": id_card_data,
            "queries": queries,
            "appraisal_data": appraisal_data,
            "appraisal_history": appraisal_history,
            "profile_data": profile_data,
        }

        return render_template("employee-resources.html", **view_data)

    except Exception as e:
        print("❌ Error in employee_resources:", e)
        traceback.print_exc()
        
        # Fallback with basic data
        employee_name = session.get("employee_name", "Employee")
        employee_data = {
            'full_name': employee_name,
            'employee_id': employee_id,
            'dob': 'Not specified',
            'email': session.get("employee_email", "Not specified"),
            'phone': 'Not specified',
            'department': session.get("employee_department", "N/A"),
            'designation': session.get("employee_designation", "N/A"),
            'date_of_joining': 'Not specified',
            'manager': 'Not specified',
            'current_address': 'Not specified',
            'permanent_address': 'Not specified'
        }
        
        return render_template(
            "employee-resources.html",
            Employee_name=employee_name,
            employee_name=employee_name,
            employee_id=employee_id,
            employee_email=session.get("employee_email", "Not specified"),
            department=session.get("employee_department", "N/A"),
            designation=session.get("employee_designation", "N/A"),
            employee_profile_pic=session.get("profile_pic", "https://chakorahub-student-s3.s3.eu-north-1.amazonaws.com/defaultpicture.jpg"),
            profile_pic=session.get("profile_pic", "https://chakorahub-student-s3.s3.eu-north-1.amazonaws.com/defaultpicture.jpg"),
            festival_today=None,
            reg_id=employee_id,
            employee_data=employee_data,
            salary_data={"has_salary": False, "allowances": "₹0.00"},
            bank_data={"has_bank": False, "bank_name": "Not specified", "account_number": "Not specified", "ifsc_code": "Not specified"},
            leave_data={"casual_leave": 0, "sick_leave": 0, "privilege_leave": 0},
            leave_history=[],
            id_card_data={},
            queries=[],
            appraisal_data={},
            appraisal_history=[],
            profile_data={},
            error="Database connection failed, using minimal data"
        )

# ==========================================
# EMPLOYEE TIMESHEET DATABASE HELPERS
# ==========================================

def get_timesheet_db():
    import sqlite3
    db_path = os.path.join(current_app.root_path, "timesheets_fallback.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Create table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS EMP_NRM_TIMESHEETS (
            EMPLOYEE_ID TEXT NOT NULL,
            WORK_DATE TEXT NOT NULL,
            LOGIN_TIME TEXT,
            LOGOUT_TIME TEXT,
            NOTES TEXT,
            CREATED_AT TEXT DEFAULT CURRENT_TIMESTAMP,
            UPDATED_AT TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (EMPLOYEE_ID, WORK_DATE)
        )
    """)
    conn.commit()
    return conn

def resolve_timesheet_employee_id(oracle_conn, employee_id):
    """
    Resolve employee id against Oracle parent table used by FK_TIMESHEETS_EMP.
    FK_TIMESHEETS_EMP points to EMPLOYEE_REGISTRATIONS.EMPLOYEE_ID.
    Returns canonical EMPLOYEE_ID from EMPLOYEE_REGISTRATIONS or None when not found.
    """
    clean_id = (employee_id or "").strip().upper()
    if not clean_id:
        return None

    cur = oracle_conn.cursor()
    try:
        cur.execute(
            """
                SELECT EMPLOYEE_ID
                FROM EMPLOYEE_REGISTRATIONS
                WHERE UPPER(TRIM(EMPLOYEE_ID)) = %s
                FETCH FIRST 1 ROWS ONLY
            """,
            (clean_id,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0]).strip()

        compact = clean_id.replace(" ", "")
        cur.execute(
            """
                SELECT EMPLOYEE_ID
                FROM EMPLOYEE_REGISTRATIONS
                WHERE REPLACE(UPPER(TRIM(EMPLOYEE_ID)), ' ', '') = %s
                FETCH FIRST 1 ROWS ONLY
            """,
            (compact,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0]).strip()
    finally:
        cur.close()

    return None

def fetch_timesheet_data(employee_id, start_date, end_date):
    """
    Fetch timesheet data for an employee between start_date and end_date.
    """
    oracle_conn = None
    use_oracle = False
    try:
        oracle_conn = get_db_connection()
        # Verify table exists in Oracle
        cur = oracle_conn.cursor()
        cur.execute("SELECT 1 FROM EMP_NRM_TIMESHEETS WHERE ROWNUM = 1")
        cur.close()
        use_oracle = True
    except Exception as e:
        print(f"[Timesheet] Oracle table EMP_NRM_TIMESHEETS unavailable. Falling back to SQLite. Error: {e}")
        if oracle_conn:
            try:
                oracle_conn.close()
            except Exception:
                pass

    employee_id = (employee_id or "").strip().upper()

    if use_oracle:
        try:
            resolved_employee_id = resolve_timesheet_employee_id(oracle_conn, employee_id)
            if not resolved_employee_id:
                oracle_conn.close()
                print(f"[Timesheet ERROR] Employee not found in EMP_NRM_EMPLOYEES for id={employee_id}")
                return []

            cur = oracle_conn.cursor(DICT_CURSOR)
            cur.execute("""
                SELECT
                    TO_CHAR(WORK_DATE, 'YYYY-MM-DD') as WORK_DATE_STR,
                    LOGIN_TIME,
                    LOGOUT_TIME,
                    NOTES
                FROM EMP_NRM_TIMESHEETS
                WHERE EMPLOYEE_ID = %s
                  AND WORK_DATE BETWEEN TO_DATE(%s, 'YYYY-MM-DD') AND TO_DATE(%s, 'YYYY-MM-DD')
            """, (resolved_employee_id, start_date, end_date))
            rows = cur.fetchall()
            cur.close()
            oracle_conn.close()
            return [
                {
                    "date": row.get("WORK_DATE_STR") or row.get("WORK_DATE"),
                    "login": row.get("LOGIN_TIME") or "",
                    "logout": row.get("LOGOUT_TIME") or "",
                    "note": row.get("NOTES") or ""
                }
                for row in rows
            ]
        except Exception as e:
            print(f"[Timesheet ERROR] Failed to fetch from Oracle: {e}")
            if oracle_conn:
                try:
                    oracle_conn.close()
                except Exception:
                    pass

    # Fallback to local SQLite
    try:
        sqlite_conn = get_timesheet_db()
        cur = sqlite_conn.cursor()
        cur.execute("""
            SELECT WORK_DATE, LOGIN_TIME, LOGOUT_TIME, NOTES
            FROM EMP_NRM_TIMESHEETS
            WHERE EMPLOYEE_ID = ?
              AND WORK_DATE BETWEEN ? AND ?
        """, (employee_id, start_date, end_date))
        rows = cur.fetchall()
        sqlite_conn.close()
        return [
            {
                "date": row["WORK_DATE"],
                "login": row["LOGIN_TIME"] or "",
                "logout": row["LOGOUT_TIME"] or "",
                "note": row["NOTES"] or ""
            }
            for row in rows
        ]
    except Exception as e:
        print(f"[Timesheet ERROR] Failed to fetch from SQLite: {e}")
        return []

def _normalize_time_str(time_val):
    """Normalize time strings like '10:00 AM' -> '10:00', '06:00 PM' -> '18:00', '10:00' -> '10:00' to fit VARCHAR2(5)."""
    if not time_val:
        return ""
    val = str(time_val).strip()
    if not val:
        return ""
    for fmt in ("%I:%M %p", "%I:%M%p", "%H:%M", "%H:%M:%S"):
        try:
            dt_obj = datetime.strptime(val, fmt)
            return dt_obj.strftime("%H:%M")
        except ValueError:
            pass
    return val[:5]

def save_timesheet_data(employee_id, entries):
    """
    Save timesheet entries.
    """
    employee_id = (employee_id or "").strip().upper()
    oracle_conn = None
    use_oracle = False
    try:
        oracle_conn = get_db_connection()
        # Verify table exists in Oracle
        cur = oracle_conn.cursor()
        cur.execute("SELECT 1 FROM EMP_NRM_TIMESHEETS WHERE ROWNUM = 1")
        cur.close()
        use_oracle = True
    except Exception as e:
        print(f"[Timesheet] Oracle table EMP_NRM_TIMESHEETS unavailable. Falling back to SQLite. Error: {e}")
        if oracle_conn:
            try:
                oracle_conn.close()
            except Exception:
                pass

    if use_oracle:
        try:
            resolved_employee_id = resolve_timesheet_employee_id(oracle_conn, employee_id)
            if not resolved_employee_id:
                oracle_conn.close()
                return False, f"Employee ID {employee_id} is not present in EMPLOYEE_REGISTRATIONS (parent table for FK_TIMESHEETS_EMP)."

            cur = oracle_conn.cursor()
            for entry in entries:
                dt = entry.get("date")
                login = _normalize_time_str(entry.get("login"))
                logout = _normalize_time_str(entry.get("logout"))
                note = entry.get("note") or ""

                if not login and not logout and not note:
                    cur.execute("""
                        DELETE FROM EMP_NRM_TIMESHEETS
                            WHERE EMPLOYEE_ID = %s AND WORK_DATE = TO_DATE(%s, 'YYYY-MM-DD')
                    """, (resolved_employee_id, dt))
                else:
                    cur.execute("""
                        SELECT count(*) FROM EMP_NRM_TIMESHEETS
                        WHERE EMPLOYEE_ID = %s AND WORK_DATE = TO_DATE(%s, 'YYYY-MM-DD')
                    """, (resolved_employee_id, dt))
                    exists = cur.fetchone()[0]

                    if exists > 0:
                        cur.execute("""
                            UPDATE EMP_NRM_TIMESHEETS
                            SET LOGIN_TIME = %s, LOGOUT_TIME = %s, NOTES = %s, UPDATED_AT = SYSTIMESTAMP
                                WHERE EMPLOYEE_ID = %s AND WORK_DATE = TO_DATE(%s, 'YYYY-MM-DD')
                        """, (login, logout, note, resolved_employee_id, dt))
                    else:
                        cur.execute("""
                            INSERT INTO EMP_NRM_TIMESHEETS (EMPLOYEE_ID, WORK_DATE, LOGIN_TIME, LOGOUT_TIME, NOTES)
                               VALUES (%s, TO_DATE(%s, 'YYYY-MM-DD'), %s, %s, %s)
                        """, (resolved_employee_id, dt, login, logout, note))
            oracle_conn.commit()
            cur.close()
            oracle_conn.close()
            return True, None
        except Exception as e:
            print(f"[Timesheet ERROR] Failed to save to Oracle: {e}")
            if oracle_conn:
                try:
                    oracle_conn.close()
                except Exception:
                    pass
            # If Oracle table exists but write fails (e.g., FK issue), surface failure.
            return False, str(e)

    # Fallback to local SQLite
    try:
        sqlite_conn = get_timesheet_db()
        cur = sqlite_conn.cursor()
        for entry in entries:
            dt = entry.get("date")
            login = _normalize_time_str(entry.get("login"))
            logout = _normalize_time_str(entry.get("logout"))
            note = entry.get("note") or ""

            if not login and not logout and not note:
                cur.execute("""
                    DELETE FROM EMP_NRM_TIMESHEETS
                    WHERE EMPLOYEE_ID = ? AND WORK_DATE = ?
                """, (employee_id, dt))
            else:
                cur.execute("""
                    INSERT OR REPLACE INTO EMP_NRM_TIMESHEETS (EMPLOYEE_ID, WORK_DATE, LOGIN_TIME, LOGOUT_TIME, NOTES, UPDATED_AT)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (employee_id, dt, login, logout, note))
        sqlite_conn.commit()
        sqlite_conn.close()
        return True, None
    except Exception as e:
        print(f"[Timesheet ERROR] Failed to save to SQLite: {e}")
        return False, str(e)

# ==========================================
# EMPLOYEE TIMESHEET ROUTES
# ==========================================

@app.route('/employee-time-sheet')
def employee_time_sheet():
    if session.get('login_type') != 'employee':
        flash("Please login as employee first", "error")
        return redirect(url_for('employee_home'))

    employee_id = session.get('employee_id')
    employee_name = session.get('employee_name', 'Employee')

    if not employee_id:
        flash("Session expired. Please login again.", "error")
        return redirect(url_for('home'))

    return render_template('employee_time_sheet.html',
                           employee_id=employee_id,
                           employee_name=employee_name)

@app.route('/api/employee/timesheet', methods=['GET'])
def get_timesheet():
    if session.get('login_type') != 'employee':
        print("[Timesheet API GET] Unauthorized access attempt")
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

    employee_id = session.get('employee_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    print(f"[Timesheet API GET] emp_id: {employee_id}, start: {start_date}, end: {end_date}")

    if not start_date or not end_date:
        return jsonify({"status": "error", "message": "Missing dates"}), 400

    data = fetch_timesheet_data(employee_id, start_date, end_date)
    print(f"[Timesheet API GET] fetched data count: {len(data)}, entries: {data}")
    return jsonify({"status": "success", "timesheet": data})

@app.route('/api/employee/timesheet/save', methods=['POST'])
def save_timesheet():
    if session.get('login_type') != 'employee':
        print("[Timesheet API SAVE] Unauthorized save attempt")
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

    employee_id = session.get('employee_id')
    req_data = request.get_json() or {}
    entries = req_data.get('timesheet', [])

    print(f"[Timesheet API SAVE] emp_id: {employee_id}, saving entries count: {len(entries)}, entries: {entries}")

    success, error_message = save_timesheet_data(employee_id, entries)
    print(f"[Timesheet API SAVE] save status: {success}")
    if success:
        return jsonify({"status": "success"})
    return jsonify({"status": "error", "message": error_message or "Failed to save timesheet"}), 500

@app.route('/employee/personal-details', methods=['GET', 'POST'])
def personal_details():
    if session.get("login_type") != "employee":
        flash("Please login as employee first", "error")
        return redirect(url_for('home'))

    employee_id = session.get("employee_id")
    if not employee_id:
        flash("Employee ID not found in session", "error")
        return redirect(url_for('home'))

    clean_id = employee_id.strip().upper()

    # ── POST: save edits ────────────────────────────────────────────────
    if request.method == 'POST':
        form_section = request.form.get('form_section')
        conn = get_db_connection()
        if not conn:
            flash("Database connection failed", "error")
            return redirect(url_for('personal_details'))
        try:
            cur = conn.cursor()
            if form_section == 'basic':
                first_name = request.form.get('firstName', '').strip()
                last_name  = request.form.get('lastName',  '').strip()
                email      = request.form.get('email',     '').strip()
                phone      = request.form.get('phone',     '').strip()
                dob        = request.form.get('dob',       '').strip() or None
                gender     = request.form.get('gender',    '').strip() or None
                if not all([first_name, last_name, email, phone]):
                    flash("Please fill in all required fields", "error")
                    return redirect(url_for('personal_details'))
                cur.execute("""
                    UPDATE EMPLOYEE_REGISTRATIONS SET
                        FIRST_NAME = %s, LAST_NAME = %s,
                        FULL_NAME  = %s, EMAIL = %s,
                        PHONE = %s, DATE_OF_BIRTH = %s,
                        GENDER = %s, UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE EMPLOYEE_ID = %s
                """, (first_name, last_name,
                      f"{first_name} {last_name}".strip(),
                      email, phone, dob, gender, clean_id))

            elif form_section == 'address':
                address   = request.form.get('address',           '').strip()
                perm_addr = request.form.get('permanent_address', '').strip()
                cur.execute("""
                    UPDATE EMPLOYEE_REGISTRATIONS SET
                        ADDRESS = %s, PERSONAL_LOCATION = %s,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE EMPLOYEE_ID = %s
                """, (address, perm_addr, clean_id))

            elif form_section == 'emergency':
                ec_name  = request.form.get('emergency_contact_name',  '').strip()
                ec_phone = request.form.get('emergency_contact_phone', '').strip()
                cur.execute("""
                    UPDATE EMPLOYEE_REGISTRATIONS SET
                        EMERGENCY_CONTACT_NAME  = %s,
                        EMERGENCY_CONTACT_PHONE = %s,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE EMPLOYEE_ID = %s
                """, (ec_name, ec_phone, clean_id))

            conn.commit()
            cur.close()
            conn.close()

            flash("Updated successfully", "success")
        except Exception as e:
            print(f"❌ personal_details POST error: {e}")
            traceback.print_exc()
            flash(f"Error saving: {str(e)}", "error")
        return redirect(url_for('personal_details'))

    # ── GET: load data from EMPLOYEE_REGISTRATIONS ──────────────────────
    conn = get_db_connection()
    if not conn:
        flash("Database connection failed", "error")
        return redirect(url_for('home'))
    try:
        cur = conn.cursor(DICT_CURSOR)
        cur.execute("""
            SELECT
                FIRST_NAME, LAST_NAME, FULL_NAME,
                EMAIL, PHONE, GENDER,
                DATE_OF_BIRTH      AS DOB,
                ADDRESS,
                PERSONAL_LOCATION  AS PERMANENT_ADDRESS,
                EMERGENCY_CONTACT_NAME,
                EMERGENCY_CONTACT_PHONE,
                DEPARTMENT_NAME    AS DEPARTMENT,
                DESIGNATION_TITLE  AS DESIGNATION,
                MANAGER_NAME       AS MANAGER,
                JOINING_DATE       AS DATE_OF_JOINING,
                EMPLOYMENT_TYPE,
                WORK_LOCATION_NAME AS WORK_LOCATION,
                STATUS
            FROM EMPLOYEE_REGISTRATIONS
            WHERE EMPLOYEE_ID = %s
            LIMIT 1
        """, (clean_id,))
        row = cur.fetchone()

        cur.execute(
            "SELECT PROFILE_PIC FROM EMP_NRM_PERSONAL WHERE EMPLOYEE_ID = %s LIMIT 1",
            (clean_id,)
        )
        pic_row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            flash("Employee record not found", "error")
            return redirect(url_for('home'))

        def fmt(val):
            if not val:
                return None
            try:
                return val.strftime('%Y-%m-%d')
            except Exception:
                return str(val)

        personal_data = {
            "first_name":               row.get("FIRST_NAME") or "",
            "last_name":                row.get("LAST_NAME")  or "",
            "full_name":                row.get("FULL_NAME")  or "",
            "email":                    row.get("EMAIL")      or "",
            "phone":                    row.get("PHONE")      or "",
            "dob":                      fmt(row.get("DOB")),
            "gender":                   row.get("GENDER")     or "",
            "address":                  row.get("ADDRESS")    or "",
            "permanent_address":        row.get("PERMANENT_ADDRESS") or "",
            "emergency_contact_name":   row.get("EMERGENCY_CONTACT_NAME")  or "",
            "emergency_contact_phone":  row.get("EMERGENCY_CONTACT_PHONE") or "",
            "department":               row.get("DEPARTMENT")     or "N/A",
            "designation":              row.get("DESIGNATION")    or "N/A",
            "manager":                  row.get("MANAGER")        or "Not specified",
            "date_of_joining":          fmt(row.get("DATE_OF_JOINING")) or "Not specified",
            "employment_type":          row.get("EMPLOYMENT_TYPE") or "Not specified",
            "work_location":            row.get("WORK_LOCATION")   or "Not specified",
            "status":                   row.get("STATUS") or "",
            "profile_pic":              (pic_row.get("PROFILE_PIC") if pic_row else None)
                                        or session.get("profile_pic")
                                        or "https://chakorahub-student-s3.s3.eu-north-1.amazonaws.com/defaultpicture.jpg",
        }

        return render_template('employee-personal-details.html',
                               personal_data=personal_data,
                               employee_id=employee_id)

    except Exception as e:
        print(f"❌ personal_details GET error: {e}")
        traceback.print_exc()
        flash(f"Error loading: {str(e)}", "error")
        return redirect(url_for('home'))
    
@app.route('/employee/salary')
def salary_info():
    if session.get("login_type") != "employee":
        return redirect(url_for('home'))

    employee_id = session.get("employee_id")

    conn = get_db_connection()
    if not conn:
        return render_template(
            'employee-salary.html',
            salary_info=None,
            salary_slips=[],
            error="Database connection failed"
        )

    try:
        cursor = conn.cursor(DICT_CURSOR)

        # ✅ Employee + Department (FIXED COLUMN NAMES)
        cursor.execute("""
            SELECT 
                e.EMPLOYEE_NAME,
                e.STATUS,
                d.DEPT_NAME
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_JOB_WORK jw ON e.EMPLOYEE_ID = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS d ON jw.DEPT_ID = d.DEPT_ID
            WHERE e.EMPLOYEE_ID = %s
            LIMIT 1
        """, (employee_id,))
        emp = cursor.fetchone()

        # ✅ Salary
        cursor.execute("""
            SELECT BASIC, HRA, ALLOWANCES, DEDUCTIONS, NET_SALARY
            FROM EMP_NRM_SALARY
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        salary = cursor.fetchone()

        # ✅ Salary slips
        cursor.execute("""
            SELECT SLIP_ID, MONTH, YEAR, FILE_PATH, GENERATED_AT
            FROM EMP_NRM_SALARY_SLIPS
            WHERE EMPLOYEE_ID = %s
            ORDER BY YEAR DESC, MONTH DESC
        """, (employee_id,))
        slips = cursor.fetchall()

        cursor.close()
        conn.close()

        salary_info = None
        if emp and salary:
            salary_info = {
                'id': employee_id,
                'name': emp.get('EMPLOYEE_NAME', ''),
                'department': emp.get('DEPT_NAME', 'N/A'),
                'basic': float(salary['BASIC'] or 0),
                'hra': float(salary['HRA'] or 0),
                'allowances': float(salary['ALLOWANCES'] or 0),
                'deductions': float(salary['DEDUCTIONS'] or 0),
                'net_salary': float(salary['NET_SALARY'] or 0),
                'status': emp.get('STATUS', 'ACTIVE')
            }

        return render_template(
            'employee-salary.html',
            salary_info=salary_info,
            salary_slips=slips,
            error=None
        )

    except Exception as e:
        print("❌ Salary error:", e)
        return render_template(
            'employee-salary.html',
            salary_info=None,
            salary_slips=[],
            error=str(e)
        )

@app.route("/leave-tracker")
@app.route("/emp-leave")
def leave_tracker():
    if session.get("login_type") != "employee":
        return redirect(url_for("home"))

    employee_id = session.get("employee_id")
    month = request.args.get("month", datetime.now().month, type=int)
    year  = request.args.get("year",  datetime.now().year,  type=int)

    leave_rows   = []
    festival_map = {}

    # Helper: microservice DictCursor returns UPPERCASE keys; old proxy code
    # read lowercase keys. Accept either so we work regardless of casing changes.
    def _g(row, key):
        return row.get(key.upper()) if row.get(key.upper()) is not None else row.get(key.lower())

    # ── fetch leave history from microservice ──
    # NOTE: Correct URL is /api/employee/leave/history (with slashes), NOT
    # /api/employee/leave-history. The hyphenated form 404s.
    try:
        resp = requests.get(
            f"{EMPLOYEE_SERVICE_URL}/api/employee/leave/history",
            params={"employee_id": employee_id},
            timeout=10,
        )
        if resp.status_code == 200:
            leave_rows = resp.json()   # list of dicts w/ LEAVE_ID, START_DATE, ...
        else:
            print(f"❌ leave/history HTTP {resp.status_code}: {(resp.text or '')[:200]}")
            flash("Unable to load leave history", "error")
    except requests.exceptions.RequestException as e:
        print(f"❌ leave/history request error: {e}")
        flash("Employee service unavailable", "error")

    # ── fetch festivals from microservice ──
    # Service returns rows with FESTIVAL_NAME / FESTIVAL_DATE (uppercase).
    try:
        resp = requests.get(
            f"{EMPLOYEE_SERVICE_URL}/api/employee/festivals",
            params={"year": year, "month": month},
            timeout=10,
        )
        if resp.status_code == 200:
            for f in resp.json():
                fest_date = _g(f, "FESTIVAL_DATE") or _g(f, "date")
                fest_name = _g(f, "FESTIVAL_NAME") or _g(f, "name")
                if fest_date:
                    # FESTIVAL_DATE arrives as ISO string from FastAPI JSON encoder.
                    festival_map[str(fest_date)[:10]] = fest_name
    except requests.exceptions.RequestException:
        pass  # calendar renders without festival markers

    # ── build leave date set for calendar highlighting ──
    leave_date_set = set()
    for row in leave_rows:
        try:
            start_raw = _g(row, "START_DATE")
            end_raw   = _g(row, "END_DATE")
            if not start_raw or not end_raw:
                continue
            start = date.fromisoformat(str(start_raw)[:10])
            end   = date.fromisoformat(str(end_raw)[:10])
            d = start
            while d <= end:
                leave_date_set.add(d.isoformat())
                d += timedelta(days=1)
        except Exception as e:
            print(f"leave-history row parse error: {e} (row={row})")

    # ── build leave_data as list-of-tuples matching the template's leave[0..5] ──
    leave_data = []
    for row in leave_rows:
        start_raw   = _g(row, "START_DATE")
        end_raw     = _g(row, "END_DATE")
        applied_raw = _g(row, "APPLIED_AT")
        leave_data.append((
            _g(row, "LEAVE_ID"),
            date.fromisoformat(str(start_raw)[:10])    if start_raw   else None,
            date.fromisoformat(str(end_raw)[:10])      if end_raw     else None,
            _g(row, "REASON"),
            _g(row, "STATUS"),
            datetime.fromisoformat(str(applied_raw).replace("Z", "")) if applied_raw else None,
        ))

    # ── build calendar grid (Sun-first) ──
    first_weekday, num_days = calendar.monthrange(year, month)
    start_offset = (first_weekday + 1) % 7   # Mon=0 → shift so Sun=col 0

    calendar_data = []
    day_counter   = 1 - start_offset
    for _ in range(6):
        week = []
        for _ in range(7):
            if day_counter < 1 or day_counter > num_days:
                week.append({"date": None, "type": None, "festival": None, "leave": False})
            else:
                d     = date(year, month, day_counter)
                d_str = d.isoformat()
                is_festival = d_str in festival_map
                is_leave    = d_str in leave_date_set
                if is_festival and is_leave:
                    day_type = "both"
                elif is_festival:
                    day_type = "festival"
                elif is_leave:
                    day_type = "leave"
                else:
                    day_type = None
                week.append({
                    "date":     d,
                    "type":     day_type,
                    "festival": festival_map.get(d_str),
                    "leave":    is_leave,
                })
            day_counter += 1
        calendar_data.append(week)
        if day_counter > num_days:
            break

    prev_month = month - 1 if month > 1 else 12
    prev_year  = year  if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_year  = year  if month < 12 else year + 1

    return render_template(
        "emp-leave.html",
        leave_data    = leave_data,
        calendar_data = calendar_data,
        month         = month,
        year          = year,
        month_name    = calendar.month_name[month],
        prev_month    = prev_month,
        prev_year     = prev_year,
        next_month    = next_month,
        next_year     = next_year,
        employee_id   = employee_id,
        employee_name = session.get("employee_name", "Employee"),
        today         = datetime.today().strftime("%Y-%m-%d"),
    )

def _is_leave_admin():
    """Authorized to approve/reject leaves.

    Required: logged in as employee AND profile is marked admin.
    The 'admin' marker comes from `session['usertype']` which is set when the
    employee profile is saved/marked as admin (matches the convention used by
    /admin/upload, /admin/batch-schedule, etc. throughout the codebase).
    Also accepts `session['admin_verified'] = True` which is set by the
    /admin_employee_login re-authentication flow.
    """
    if session.get("login_type") != "employee":
        return False
    usertype = (session.get("usertype") or session.get("role") or "").strip().lower()
    if usertype in ("admin", "administrator"):
        return True
    if session.get("admin_verified") is True:
        return True
    return False


@app.route('/approve-leave/<int:leave_id>', methods=['POST'])
def approve_leave(leave_id):
    if session.get("login_type") != "employee":
        flash("Please login as an employee to approve leaves.", "error")
        return redirect(url_for("home"))
    if not _is_leave_admin():
        flash("Admin profile required to approve leaves.", "error")
        return redirect(url_for("employee_resources"))

    try:
        resp = requests.post(
            f"{EMPLOYEE_SERVICE_URL}/api/manager/approve-leave/{leave_id}",
            timeout=10,
        )
        if resp.status_code == 200:
            try:
                payload = resp.json()
            except Exception:
                payload = {}
            if payload.get("success"):
                flash(payload.get("message") or f"Leave #{leave_id} approved.", "success")
            else:
                flash(payload.get("message") or f"Could not approve leave #{leave_id}.", "error")
        else:
            print(f"❌ Approve leave HTTP {resp.status_code}: {(resp.text or '')[:200]}")
            flash(f"Approve failed (HTTP {resp.status_code}).", "error")
    except requests.exceptions.Timeout:
        flash("Employee service timed out. Please try again.", "error")
    except Exception as e:
        print(f"❌ Approve leave proxy error: {e}")
        flash("Approve failed — service unreachable.", "error")
    return redirect(url_for('admin_leave_approval'))


@app.route('/reject-leave/<int:leave_id>', methods=['POST'])
def reject_leave(leave_id):
    if session.get("login_type") != "employee":
        flash("Please login as an employee to reject leaves.", "error")
        return redirect(url_for("home"))
    if not _is_leave_admin():
        flash("Admin profile required to reject leaves.", "error")
        return redirect(url_for("employee_resources"))

    try:
        resp = requests.post(
            f"{EMPLOYEE_SERVICE_URL}/api/manager/reject-leave/{leave_id}",
            timeout=10,
        )
        if resp.status_code == 200:
            try:
                payload = resp.json()
            except Exception:
                payload = {}
            if payload.get("success"):
                flash(payload.get("message") or f"Leave #{leave_id} rejected.", "success")
            else:
                flash(payload.get("message") or f"Could not reject leave #{leave_id}.", "error")
        else:
            print(f"❌ Reject leave HTTP {resp.status_code}: {(resp.text or '')[:200]}")
            flash(f"Reject failed (HTTP {resp.status_code}).", "error")
    except requests.exceptions.Timeout:
        flash("Employee service timed out. Please try again.", "error")
    except Exception as e:
        print(f"❌ Reject leave proxy error: {e}")
        flash("Reject failed — service unreachable.", "error")
    return redirect(url_for('admin_leave_approval'))


@app.route('/admin-leave-approval')
def admin_leave_approval():
    if session.get("login_type") != "employee":
        flash("Please login as an employee to access the leave approval queue.", "error")
        return redirect(url_for("home"))
    if not _is_leave_admin():
        flash("This page is restricted to admin profiles only.", "error")
        return redirect(url_for("employee_resources"))

    manager_id = session.get('employee_id')
    pending_leaves = []
    approved_today = 0
    rejected_today = 0

    if manager_id:
        try:
            resp = requests.get(
                f"{EMPLOYEE_SERVICE_URL}/api/manager/pending-leaves",
                params={"manager_id": manager_id},
                timeout=20,
            )
            rows = resp.json() if resp.content else []

            if resp.status_code == 200 and isinstance(rows, list):
                for row in rows:
                    start_date = row.get("START_DATE")
                    end_date = row.get("END_DATE")
                    applied_at = row.get("APPLIED_AT")

                    pending_leaves.append({
                        "id": row.get("LEAVE_ID"),
                        "emp_id": row.get("EMPLOYEE_ID"),
                        "emp_name": row.get("EMPLOYEE_NAME") or "N/A",
                        "leave_type": (row.get("LEAVE_TYPE") or "Leave").strip(),
                        "start_date": datetime.fromisoformat(str(start_date)).date() if start_date else None,
                        "end_date": datetime.fromisoformat(str(end_date)).date() if end_date else None,
                        "reason": row.get("REASON") or "",
                        "applied_at": datetime.fromisoformat(str(applied_at)) if applied_at else datetime.now(),
                        "duration": row.get("DURATION") or 0,
                    })
            else:
                print(f"❌ pending-leaves HTTP {resp.status_code}: {(resp.text or '')[:200]}")
                flash("Could not load pending leaves from employee service.", "error")

            # Fetch today's approved/rejected counts so both stat tiles are accurate.
            try:
                ar = requests.get(
                    f"{EMPLOYEE_SERVICE_URL}/api/manager/approved-today",
                    timeout=10,
                )
                if ar.status_code == 200:
                    approved_today = int((ar.json() or {}).get("approved_today", 0))
            except Exception as ar_e:
                print(f"approved-today fetch error: {ar_e}")

            try:
                rr = requests.get(
                    f"{EMPLOYEE_SERVICE_URL}/api/manager/rejected-today",
                    timeout=10,
                )
                if rr.status_code == 200:
                    rejected_today = int((rr.json() or {}).get("rejected_today", 0))
            except Exception as rr_e:
                print(f"rejected-today fetch error: {rr_e}")

        except Exception as e:
            print(f"❌ Admin leave approval page error: {e}")
            flash("Employee service unavailable. Pending list may be stale.", "error")

    pending_count = len(pending_leaves)

    return render_template(
        'admin_leave_approval.html',
        pending_leaves=pending_leaves,
        pending_count=pending_count,
        approved_today=approved_today,
        rejected_today=rejected_today,
    )

@app.route("/apply-leave", methods=["POST"])
def apply_leave_proxy():
    if session.get("login_type") != "employee":
        return redirect(url_for("home"))

    try:
        leave_type = (request.form.get("leave_type") or "").strip()
        start_date = (request.form.get("start_date") or "").strip()
        end_date   = (request.form.get("end_date")   or "").strip()
        reason     = (request.form.get("reason")     or "").strip()

        if not leave_type:
            flash("Leave type is required", "error")
            return redirect(url_for("leave_tracker"))
        if not start_date or not end_date:
            flash("Start and end dates are required", "error")
            return redirect(url_for("leave_tracker"))

        # The microservice expects:
        #   URL    : /api/employee/leave/apply   (NOT /api/employee/apply-leave)
        #   Body   : JSON (NOT form-encoded)
        #   Fields : from_date, to_date         (NOT start_date, end_date)
        # All three mismatches caused "Not Found" + silent failures previously.
        payload = {
            "employee_id": session.get("employee_id"),
            "leave_type":  leave_type,
            "from_date":   start_date,
            "to_date":     end_date,
            "reason":      reason or None,
        }
        resp = requests.post(
            f"{EMPLOYEE_SERVICE_URL}/api/employee/leave/apply",
            json=payload,
            timeout=10,
        )
        if resp.status_code == 200:
            try:
                body = resp.json()
            except Exception:
                body = {}
            if body.get("success", True):
                flash("Leave applied successfully! Your manager has been notified.", "success")
            else:
                flash(body.get("message") or "Leave request failed", "error")
        else:
            try:
                detail = resp.json()
                # FastAPI HTTPException → {"detail": "..."}; pydantic validation → {"detail": [...]}
                d = detail.get("detail")
                if isinstance(d, list):
                    msg = "; ".join(str(item.get("msg", item)) for item in d)
                else:
                    msg = d or detail.get("message") or "Leave request failed"
            except Exception:
                msg = f"Leave request failed (HTTP {resp.status_code})"
            print(f"❌ apply-leave HTTP {resp.status_code}: {(resp.text or '')[:300]}")
            flash(msg, "error")

    except requests.exceptions.Timeout:
        flash("Employee service timed out. Please try again.", "error")
    except Exception as e:
        import traceback
        print("❌ Apply leave proxy error:", traceback.format_exc())
        flash("Unable to submit leave. Please try again.", "error")

    return redirect(url_for("leave_tracker"))

'''@app.route('/employee/id-card')
def id_card():
    if session.get("login_type") != "employee":
        return redirect(url_for('home'))

    employee_id = session.get("employee_id")

    try:
        resp = requests.get(
            f"{EMPLOYEE_SERVICE_URL}/api/employee/id-card/{employee_id}",
            timeout=20,
        )
        payload = resp.json() if resp.content else {}

        if resp.status_code != 200 or not payload.get("success"):
            raise Exception(payload.get("error") or payload.get("detail") or "Failed to load ID card data")

        return render_template(
            'employee-idcard.html',
            id_card_data=payload.get("id_card_data"),
            personal_data=payload.get("personal_data"),
            employee_data=payload.get("employee_data"),
            error=None
        )

    except Exception as e:
        print("❌ ID card error:", e)
        return render_template(
            'employee-idcard.html',
            id_card_data=None,
            personal_data=None,
            employee_data=None,
            error=str(e)
        )'''
@app.route('/employee-idcard')
def employee_idcard():
    if session.get('login_type') != 'employee':
        flash('Please login as employee', 'error')
        return redirect(url_for('home'))

    employee_id = session.get('employee_id')

    conn = None
    cur = None

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Fetch ID Card Data
        cur.execute("""
            SELECT EMPLOYEE_ID,
                   ID_NUMBER,
                   ISSUE_DATE,
                   EXPIRY_DATE
            FROM EMP_NRM_IDCARD
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))

        id_card_data = cur.fetchone()
        cur.execute("""
            SELECT
                FULL_NAME,
                DEPARTMENT_NAME,
                DESIGNATION_TITLE,
                JOINING_DATE,
                STATUS,        
            FROM EMPLOYEE_REGISTRATIONS
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))

        employee_data = cur.fetchone()
        # If no record exists
        if not id_card_data:
            flash('ID card details not found', 'error')
            return render_template(
                'employee-idcard.html',
                id_card_data=None
            )

        return render_template(
            'employee-idcard.html',
            id_card_data=id_card_data,
            employee_data=employee_data,
            employee_id=employee_id,
            today_date=date.today(),
            datetime=datetime
        )

    except Exception as e:
        print('Employee ID Card Error:', e)
        flash('Unable to load ID card', 'error')

        return render_template(
            'employee-idcard.html',
            id_card_data=None,
            error=str(e)
        )

    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except:
            pass

@app.route('/employee/queries', methods=['GET', 'POST'])
def employee_queries():
    """Employee queries page + proxy to employee_service for data changes."""
    if session.get("login_type") != "employee":
        return redirect(url_for('home'))
    
    employee_id = session.get("employee_id")
    
    if request.method == 'POST':
        try:
            resp = requests.post(
                f"{EMPLOYEE_SERVICE_URL}/api/employee/queries/{employee_id}",
                data={"query_text": request.form.get('query_text', '')},
                timeout=20,
            )
            payload = resp.json() if resp.content else {}
            if resp.status_code != 200 or not payload.get("success"):
                raise Exception(payload.get("error") or payload.get("detail") or "Failed to submit query")
            
            flash("Query submitted successfully!", "success")
            return redirect(url_for('employee_queries'))
            
        except Exception as e:
            print(f"❌ Query submission error: {e}")
            flash("Failed to submit query", "error")
    
    # GET request
    try:
        resp = requests.get(
            f"{EMPLOYEE_SERVICE_URL}/api/employee/queries/{employee_id}",
            timeout=20,
        )
        payload = resp.json() if resp.content else {}
        queries_data = payload.get("queries_data", []) if resp.status_code == 200 else []

        try:
            return render_template('employee-queries.html', queries_data=queries_data)
        except TemplateNotFound:
            # Keep this route usable even if the template is absent.
            return jsonify(payload if payload else {"success": True, "queries_data": queries_data})
        
    except Exception as e:
        print(f"❌ Queries error: {e}")
        try:
            return render_template('employee-queries.html', queries_data=[])
        except TemplateNotFound:
            return jsonify({'success': False, 'error': str(e), 'queries_data': []}), 500

# ==========================================
# EMPLOYEE APPRAISAL ROUTES - DIRECT DB
# ==========================================
@app.route("/api/appraisal/employee/<employee_id>")
def proxy_employee_appraisal(employee_id):
    try:
        url = f"{EMPLOYEE_SERVICE_URL}/api/appraisal/employee/{employee_id}"
        print(f"🔄 Proxying 'My Appraisal' to: {url}")
        
        # INCREASED TIMEOUT to 100 seconds
        resp = requests.get(url, timeout=100)
        
        return jsonify(resp.json()), resp.status_code

    except requests.exceptions.Timeout:
        print("❌ Microservice timed out (Snowflake is slow)")
        return jsonify({"success": False, "error": "Database is warming up. Please refresh in a moment."}), 504
    except Exception as e:
        print(f"❌ Proxy Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# 2. Team Appraisals
@app.route("/api/appraisal/viewable/<employee_id>")
def proxy_viewable_appraisals(employee_id):
    try:
        url = f"{EMPLOYEE_SERVICE_URL}/api/appraisal/viewable/{employee_id}"
        print(f"🔄 Proxying 'Team Appraisals' to: {url}")
        
        # INCREASED TIMEOUT to 100 seconds
        resp = requests.get(url, timeout=100)
        
        return jsonify(resp.json()), resp.status_code

    except requests.exceptions.Timeout:
        print("❌ Microservice timed out")
        return jsonify({"success": False, "error": "Service timed out"}), 504
    except Exception as e:
        print(f"❌ Proxy Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# 3. Hierarchy/Organization
@app.route("/api/appraisal/hierarchy/<employee_id>")
def proxy_hierarchy(employee_id):
    try:
        url = f"{EMPLOYEE_SERVICE_URL}/api/appraisal/hierarchy/{employee_id}"
        print(f"🔄 Proxying 'Hierarchy' to: {url}")
        
        # INCREASED TIMEOUT to 100 seconds
        resp = requests.get(url, timeout=100)
        
        return jsonify(resp.json()), resp.status_code

    except requests.exceptions.Timeout:
        print("❌ Microservice timed out")
        return jsonify({"success": False, "error": "Service timed out"}), 504
    except Exception as e:
        print(f"❌ Proxy Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/employee/appraisal')
def employee_appraisal():
    """Main appraisal portal page"""
    if session.get("login_type") != "employee":
        return redirect(url_for('home'))
    
    return render_template('employee-appraisal.html',employee_id=session.get("employee_id"),
    is_manager=session.get("is_manager", False))


@app.route('/employee-hierarchy')
def employee_hierarchy_page():
    """Standalone organization hierarchy page for employees."""
    if session.get("login_type") != "employee":
        return redirect(url_for('home'))

    return render_template(
        'employee-hierarchy.html',
        employee_id=session.get("employee_id"),
        employee_name=session.get("employee_name", "Employee")
    )

@app.route('/api/appraisal/goals', methods=['GET', 'POST'])
def appraisal_goals():
    """Proxy appraisal goals operations to employee_service."""
    if session.get("login_type") != "employee":
        return jsonify({'error': 'Not authenticated'}), 401
    
    employee_id = session.get('employee_id')
    try:
        if request.method == 'GET':
            resp = requests.get(
                f"{EMPLOYEE_SERVICE_URL}/api/appraisal/goals/{employee_id}",
                timeout=20,
            )
        else:
            resp = requests.post(
                f"{EMPLOYEE_SERVICE_URL}/api/appraisal/goals/{employee_id}",
                json=request.get_json(silent=True) or {},
                timeout=20,
            )

        return jsonify(resp.json()), resp.status_code
            
    except Exception as e:
        print(f"❌ Appraisal goals error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/appraisal/trainings', methods=['GET', 'POST'])
def appraisal_trainings():
    """Proxy appraisal trainings operations to employee_service."""
    if session.get("login_type") != "employee":
        return jsonify({'error': 'Not authenticated'}), 401
    
    employee_id = session.get('employee_id')
    try:
        if request.method == 'GET':
            resp = requests.get(
                f"{EMPLOYEE_SERVICE_URL}/api/appraisal/trainings/{employee_id}",
                timeout=20,
            )
        else:
            resp = requests.post(
                f"{EMPLOYEE_SERVICE_URL}/api/appraisal/trainings/{employee_id}",
                json=request.get_json(silent=True) or {},
                timeout=20,
            )

        return jsonify(resp.json()), resp.status_code
            
    except Exception as e:
        print(f"❌ Appraisal trainings error: {e}")
        return jsonify({'error': str(e)}), 500
        
@app.route('/api/appraisal/summary')
def appraisal_summary():
    if session.get("login_type") != "employee":
        return jsonify({'error': 'Not authenticated'}), 401

    employee_id = session.get('employee_id')
    try:
        resp = requests.get(
            f"{EMPLOYEE_SERVICE_URL}/api/appraisal/summary/{employee_id}",
            timeout=20,
        )
        return jsonify(resp.json()), resp.status_code

    except Exception as e:
        print("❌ Appraisal summary error:", e)
        return jsonify({'error': str(e)}), 500
# ==========================================
# EMPLOYEE LOGOUT
# ==========================================
@app.route('/Emp-logout')
def Emp_logout():
    """Employee logout - calls home_service to update IS_ACTIVE"""
    employee_id = session.get("employee_id")
    _sync_logout_state("employee", employee_id=employee_id, reason="manual-logout")
    
    session.clear()
    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('home'))


@app.route('/admin/active-users')
def admin_active_users():
    """Admin page to view currently logged-in users"""
    
    # Check if admin is logged in
    if not _has_employee_admin_access():
        flash('Admin access required', 'error')
        return redirect(url_for('home'))
    
    try:
        # Get active users from home_service
        response = requests.get(
            f"{HOME_SERVICE_URL}/home/active-users",
            timeout=10
        )
        
        if response.status_code == 200:
            try:
                data = response.json()
            except ValueError:
                data = {}
            
            return render_template(
                'admin_active_users.html',
                active_user_count=data.get('active_user_count', 0),
                active_employee_count=data.get('active_employee_count', 0),
                total_active=data.get('total_active', 0),
                active_users=data.get('active_users', [])
            )
        else:
            flash('Failed to fetch active users', 'error')
            return render_template(
                'admin_active_users.html',
                active_user_count=0,
                active_employee_count=0,
                total_active=0,
                active_users=[]
            )
            
    except Exception as e:
        print(f"❌ Admin active users error: {e}")
        flash(f'Error: {str(e)}', 'error')
        return render_template(
            'admin_active_users.html',
            active_user_count=0,
            active_employee_count=0,
            total_active=0,
            active_users=[]
        )


@app.route('/debug_login')
def debug_login():
    session['login_type'] = 'employee'
    session['employee_id'] = 'CH25006'
    session['user'] = 'support@chakorahub.com'
    session['email'] = 'support@chakorahub.com'
    session['admin_verified'] = True
    return redirect(url_for('admin_internship_page'))


@app.route('/admin/internship')
def admin_internship_page():
    """Admin placeholder page for internship selection workflows."""
    if not _has_employee_admin_access():
        flash('Admin access required', 'error')
        return redirect(url_for('home'))
    return render_template('admin_internship_dashboard.html')


@app.route('/feedback-scroll')
def feedback_scroll():
    feedbacks = []
    conn = get_db_connection()
    
    if conn:
        print("✅ DB connection established")
        cursor = conn.cursor()
        try:
            # 🔹 FIXED: Get feedback with user names from NRM_USERS table
            cursor.execute("""
                SELECT 
                    to_char(f.FEEDBACK_MESSAGE),
                    COALESCE(u.USERNAME, f.NAME, 'Anonymous') as username,
                    f.SUBMITTED_AT
                FROM NRM_FEEDBACK f
                LEFT JOIN NRM_USERS u ON f.STUDENT_ID = u.ID
                WHERE f.FEEDBACK_MESSAGE IS NOT NULL 
                  AND dbms_lob.getlength(f.FEEDBACK_MESSAGE) > 0
                ORDER BY f.SUBMITTED_AT DESC
            """)
            rows = cursor.fetchall()
            
            # Format the data properly
            for row in rows:
                feedbacks.append({
                    'feedback_message': row[0],
                    'username': row[1].strip() if row[1] else 'Anonymous'
                })
                
            print(f"📦 Feedbacks fetched: {len(feedbacks)}")
            if not feedbacks:
                print("⚠ No feedbacks found in database")
                
        except Exception as e:
            print("❌ SQL Error while fetching feedbacks:", e)
            import traceback
            traceback.print_exc()
        finally:
            cursor.close()
            conn.close()
            print("🔒 DB connection closed")
    else:
        print("❌ Could not establish DB connection")

    return render_template('feedback-dis.html', feedbacks=feedbacks)


# Solutions page route
@app.route('/solutions')
def solutions():
    """Solutions page - displays the acupressure guide"""
    try:
        return render_template(
            "solutions.html",
            current_year=datetime.now().year
        )
    except Exception as e:
        print("❌ Solutions Error:", e)
        import traceback
        traceback.print_exc()
        flash("Error loading solutions page. Please try again.", "error")
        return redirect(url_for('home'))
    
@app.route("/client")
def client():
    return render_template("client.html")


# GET /whoami -> session-backed identity for meeting frontend
@app.route('/whoami', methods=['GET'])
def meeting_whoami():
    username = (
        session.get('user')
        or session.get('username')
        or session.get('email')
        or session.get('employee_name')
        or ''
    )
    user_type = (
        session.get('usertype')
        or session.get('user_type')
        or session.get('login_type')
        or 'external'
    )
    role = session.get('role') or ('admin' if str(user_type).lower() == 'admin' else 'student')

    return jsonify({
        'username': username,
        'role': role,
        'user_type': user_type,
        'authenticated': bool(username),
    })


# # GET /meeting-slots -> meeting_service.py (/meeting-slots)
@app.route('/meeting-slots', methods=['GET'])
def proxy_meeting_slots():

    booking_date = request.args.get('date', '').strip()

    if not booking_date:
        return jsonify({
            'error': 'date parameter required'
        }), 400

    response = requests.get(
        f'{MEETING_SERVICE_URL}/meeting/slots',
        params={'date': booking_date},
        timeout=(2, 10)   # 2s connect, 10s read
    )

    return jsonify(response.json()), response.status_code


# POST /meeting/api/agentic-suggestions — AI slot/complexity/duration suggestions
@app.route('/meeting/api/agentic-suggestions', methods=['POST'])
def meeting_api_agentic_suggestions():
    data = request.get_json(silent=True) or {}
    try:
        resp = requests.post(
            f"{MEETING_SERVICE_URL}/meeting/agentic-suggestions",
            json={
                "email":          data.get("email", ""),
                "date":           data.get("date", ""),
                "booking_reason": data.get("booking_reason", ""),
            },
            timeout=20,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.Timeout:
        return jsonify({"success": False, "message": "Suggestion service timed out"}), 504
    except Exception as e:
        print(f"❌ agentic-suggestions proxy error: {e}")
        return jsonify({"success": False, "message": "Suggestion service unavailable"}), 502
    
# # GET /meeting/api/price-preview - Direct proxy to Meeting microservice (bypass API Gateway)
@app.route('/meeting/api/price-preview', methods=['GET'])
def proxy_meeting_price_preview():
    params = {
        'date': request.args.get('date', '').strip(),
        'start_time': request.args.get('start_time', '').strip(),
        'duration_minutes': request.args.get('duration_minutes', '').strip(),
        'complexity': request.args.get('complexity', 'Medium').strip(),
        'booking_type': request.args.get('booking_type', 'external').strip(),
        'email': request.args.get('email', '').strip(),
    }
    if not params['date']:
        return jsonify({'error': 'date required'}), 400
    if not params['start_time']:
        return jsonify({'error': 'start_time required'}), 400
    if not params['duration_minutes']:
        return jsonify({'error': 'duration_minutes required'}), 400
    response = requests.get(
        f'{MEETING_SERVICE_URL}/meeting-price-preview',
        params=params,
        timeout=20,
    )
    return jsonify(response.json()), response.status_code

# # POST /meeting/api/create-payment-order - Direct proxy to Meeting microservice (bypass API Gateway)
@app.route('/meeting/api/create-payment-order', methods=['POST'])
def proxy_meeting_create_payment_order():
    payload = request.get_json() or {}
    response = requests.post(
        f'{BILLING_SERVICE_URL}/payment/create-order',
        json=payload,
        timeout=20,
    )
    return jsonify(response.json()), response.status_code

# Register
def is_password_valid(password):
    if len(password) < 8:
        return False
    special_chars = re.findall(r'[\W_]', password)
    has_upper = re.search(r'[A-Z]', password)
    has_number = re.search(r'\d', password)
    return len(special_chars) == 1 and bool(has_upper) and bool(has_number)


@app.route('/validate_admin_registration', methods=['POST'])
def validate_admin_registration():
    data = request.get_json()
    email = data.get('email', '').strip()
    phone = data.get('phone', '').strip()

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(DICT_CURSOR)

        cur.execute("""
            SELECT ID
            FROM NRM_USERS
            WHERE EMAIL = %s
              AND PHONE = %s
        """, (email, phone))

        user = cur.fetchone()
        return jsonify({"valid": bool(user)})

    except Exception as e:
        print("Validation error:", e)
        return jsonify({"valid": False})

    finally:
        if cur: cur.close()
        if conn: conn.close()

# ─── Route: /api/teams/status ────────────────────────────────────
@app.route('/api/teams/status', methods=['GET'])
def teams_status():
    """Calendar checks this on load to see if Teams is connected."""
    try:
        get_teams_token()
        return jsonify({"synced": True})
    except Exception as e:
        return jsonify({"synced": False, "error": str(e)})


# ─── Route: /api/teams/auth ──────────────────────────────────────
@app.route('/api/teams/auth', methods=['POST'])
def teams_auth():
    """Calendar calls this when user clicks 'Sync with Teams'."""
    try:
        get_teams_token()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ─── Route: /api/teams/meetings ──────────────────────────────────
@app.route('/api/teams/meetings', methods=['GET'])
def teams_meetings():
    """Fetch all Teams meetings for the organizer — shown in calendar."""
    try:
        organizer_email = "support@chakorahub.com"
        try:
            month = int(request.args.get("month", datetime.now().month))
            year = int(request.args.get("year", datetime.now().year))
        except ValueError:
            month = datetime.now().month
            year = datetime.now().year

        month_start = datetime(year, month, 1)
        next_month_start = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
        start_iso = month_start.strftime("%Y-%m-%dT00:00:00Z")
        end_iso = next_month_start.strftime("%Y-%m-%dT00:00:00Z")

        response = requests.get(
            f"{MS365_SERVICE_URL}/teams/calendar/{organizer_email}",
            params={"start": start_iso, "end": end_iso},
            timeout=30,
        )

        payload = response.json() if response.headers.get("Content-Type", "").startswith("application/json") else {}
        if response.ok:
            return jsonify({"success": True, "meetings": payload.get("meetings", [])})

        return jsonify({"success": False, "meetings": [], "error": payload.get("detail") or payload.get("message") or response.text}), response.status_code

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500




# =========================
# INTERNAL REQUEST HELPER
# =========================
def proxy_ms365(method, endpoint):

    if not _is_admin_user(allow_db_fallback=True):
        return jsonify({
            "success": False,
            "message": "Admin access required"
        }), 403

    target_url = f"{MS365_SERVICE_URL}{endpoint}"

    try:
        response = requests.request(
            method=method,
            url=target_url,
            json=request.get_json(silent=True),
            timeout=300
        )

        return (
            response.content,
            response.status_code,
            {
                "Content-Type": response.headers.get(
                    "Content-Type",
                    "application/json"
                )
            }
        )

    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500


# ==========================================
# HEALTH
# FastAPI -> @router.get("/health")
# ==========================================
@app.route("/api/ms365/health")
def ms365_health():
    return proxy_ms365("GET", "/health")


# ==========================================
# TEAMS
# FastAPI -> @router.get("/teams")
# ==========================================
@app.route("/api/ms365/teams")
def ms365_teams():
    return proxy_ms365("GET", "/teams")


# ==========================================
# CHANNELS
# FastAPI -> @router.get("/teams/{team_id}/channels")
# ==========================================
@app.route("/api/ms365/teams/<team_id>/channels")
def ms365_channels(team_id):
    return proxy_ms365(
        "GET",
        f"/teams/{team_id}/channels"
    )


# ==========================================
# MESSAGES
# FastAPI ->
# @router.get("/teams/{team_id}/channels/{channel_id}/messages")
# ==========================================
@app.route(
    "/api/ms365/teams/<team_id>/channels/<channel_id>/messages"
)
def ms365_messages(team_id, channel_id):

    return proxy_ms365(
        "GET",
        f"/teams/{team_id}/channels/{channel_id}/messages"
    )


# ==========================================
# SYNC NOTES
# FastAPI ->
# @router.post("/sync/{team_id}/{channel_id}")
# ==========================================
@app.route(
    "/api/ms365/sync/<team_id>/<channel_id>",
    methods=["POST"]
)
def ms365_sync(team_id, channel_id):

    return proxy_ms365(
        "POST",
        f"/sync/{team_id}/{channel_id}"
    )


# ==========================================
# ASYNC SYNC
# FastAPI ->
# @router.post("/sync-async/{team_id}/{channel_id}")
# ==========================================
@app.route(
    "/api/ms365/sync-async/<team_id>/<channel_id>",
    methods=["POST"]
)
def ms365_sync_async(team_id, channel_id):

    return proxy_ms365(
        "POST",
        f"/sync-async/{team_id}/{channel_id}"
    )


# ==========================================
# MANUAL NOTE UPLOAD
# FastAPI ->
# @router.post("/upload-manual")
# ==========================================
@app.route(
    "/api/ms365/upload-manual",
    methods=["POST"]
)
def ms365_upload_manual():

    return proxy_ms365(
        "POST",
        "/upload-manual"
    )

# ==========================================
# ONBOARDING PROXY ROUTES
# ==========================================

@app.route('/api/onboarding/upload-resume', methods=['POST'])
def proxy_upload_resume():
    try:
        # Frontend sends JSON + base64 encoded resume
        data = request.get_json(silent=True)
        if not data:
            return jsonify({"message": "Invalid JSON body", "success": False}), 400

        name     = data.get('name', '').strip()
        email    = data.get('email', '').strip()
        filename = data.get('filename', '').strip()
        resume   = data.get('resume', '')   # base64 string

        if not all([name, email, filename, resume]):
            return jsonify({"message": "name, email, filename and resume are required", "success": False}), 400

        os.environ["NO_PROXY"] = ONBOARDING_SERVICE_URL
        os.environ["no_proxy"] = ONBOARDING_SERVICE_URL

        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies   = {"http": None, "https": None}
            response = internal_session.post(
                f"{ONBOARDING_SERVICE_URL}/upload-resume",
                json=data,          # forward the whole JSON body as-is
                timeout=60,
                allow_redirects=False,
            )

        try:
            payload = response.json()
        except ValueError:
            payload = {
                "message": "Onboarding service returned non-JSON response",
                "success": False,
                "upstream_status": response.status_code,
                "upstream_preview": (response.text or "")[:300]
            }

        return jsonify(payload), response.status_code

    except requests.exceptions.Timeout:
        return jsonify({"message": "Onboarding service timed out.", "success": False}), 504
    except requests.exceptions.ConnectionError:
        return jsonify({"message": "Onboarding service unavailable.", "success": False}), 503
    except Exception as e:
        print(f"❌ proxy_upload_resume error: {e}")
        return jsonify({"message": f"Error: {str(e)}", "success": False}), 500


@app.route('/api/onboarding/applications', methods=['GET'])
def proxy_list_applications():
    """
    Proxy for listing all applications from onboarding service.
    """
    try:
        os.environ["NO_PROXY"] = ONBOARDING_SERVICE_URL
        os.environ["no_proxy"] = ONBOARDING_SERVICE_URL
        
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.get(
                f"{ONBOARDING_SERVICE_URL}/applications",
                timeout=30,
                allow_redirects=False,
            )
        
        try:
            payload = response.json()
        except ValueError:
            payload = {
                "applications": [],
                "total_count": 0,
                "error": "Onboarding service returned non-JSON response"
            }
        
        return jsonify(payload), response.status_code
        
    except requests.exceptions.Timeout:
        return jsonify({
            "applications": [],
            "total_count": 0,
            "error": "Service timed out"
        }), 504
    except requests.exceptions.ConnectionError:
        return jsonify({
            "applications": [],
            "total_count": 0,
            "error": "Service unavailable"
        }), 503
    except Exception as e:
        print(f"❌ proxy_list_applications error: {e}")
        return jsonify({
            "applications": [],
            "total_count": 0,
            "error": str(e)
        }), 500


@app.route('/api/onboarding/application-status/<application_id>', methods=['GET'])
def proxy_application_status(application_id):
    """
    Proxy for getting application status from onboarding service.
    """
    if not application_id:
        return jsonify({
            "success": False,
            "applications": [],
            "total_applications": 0,
            "message": "Application ID is required"
        }), 400
    
    try:
        os.environ["NO_PROXY"] = ONBOARDING_SERVICE_URL
        os.environ["no_proxy"] = ONBOARDING_SERVICE_URL
        
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.get(
                f"{ONBOARDING_SERVICE_URL}/application-status/{application_id}",
                timeout=30,
                allow_redirects=False,
            )
        
        try:
            payload = response.json()
        except ValueError:
            payload = {
                "success": False,
                "applications": [],
                "total_applications": 0,
                "error": "Onboarding service returned non-JSON response"
            }
        
        return jsonify(payload), response.status_code
        
    except requests.exceptions.Timeout:
        return jsonify({
            "success": False,
            "applications": [],
            "total_applications": 0,
            "error": "Service timed out"
        }), 504
    except requests.exceptions.ConnectionError:
        return jsonify({
            "success": False,
            "applications": [],
            "total_applications": 0,
            "error": "Service unavailable"
        }), 503
    except Exception as e:
        print(f"❌ proxy_application_status error: {e}")
        return jsonify({
            "success": False,
            "applications": [],
            "total_applications": 0,
            "error": str(e)
        }), 500


@app.route('/api/onboarding/update-status', methods=['PUT'])
def proxy_update_status():
    """
    Proxy for updating application status to onboarding service.
    """
    try:
        data = request.get_json(silent=True)
        if not data:
            return jsonify({
                "message": "Invalid request body",
                "success": False
            }), 400
        
        # Validate required fields
        if not data.get('application_id'):
            return jsonify({
                "message": "Application ID is required",
                "success": False
            }), 400
        if not data.get('status'):
            return jsonify({
                "message": "Status is required",
                "success": False
            }), 400
        
        os.environ["NO_PROXY"] = ONBOARDING_SERVICE_URL
        os.environ["no_proxy"] = ONBOARDING_SERVICE_URL
        
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.put(
                f"{ONBOARDING_SERVICE_URL}/update-status",
                json=data,
                timeout=30,
                allow_redirects=False,
            )
        
        try:
            payload = response.json()
        except ValueError:
            payload = {
                "message": "Onboarding service returned non-JSON response",
                "success": False
            }
        
        return jsonify(payload), response.status_code
        
    except requests.exceptions.Timeout:
        return jsonify({
            "message": "Onboarding service timed out. Please try again.",
            "success": False
        }), 504
    except requests.exceptions.ConnectionError:
        return jsonify({
            "message": "Onboarding service is currently unavailable.",
            "success": False
        }), 503
    except Exception as e:
        print(f"❌ proxy_update_status error: {e}")
        return jsonify({
            "message": f"Error updating status: {str(e)}",
            "success": False
        }), 500


@app.route('/api/onboarding/delete-application', methods=['DELETE'])
def proxy_delete_application():
    """
    Proxy for deleting application from onboarding service.
    """
    try:
        data = request.get_json(silent=True)
        if not data:
            return jsonify({
                "message": "Invalid request body",
                "success": False
            }), 400
        
        if not data.get('application_id'):
            return jsonify({
                "message": "Application ID is required",
                "success": False
            }), 400
        
        os.environ["NO_PROXY"] = ONBOARDING_SERVICE_URL
        os.environ["no_proxy"] = ONBOARDING_SERVICE_URL
        
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.delete(
                f"{ONBOARDING_SERVICE_URL}/delete-application",
                json=data,
                timeout=30,
                allow_redirects=False,
            )
        
        try:
            payload = response.json()
        except ValueError:
            payload = {
                "message": "Onboarding service returned non-JSON response",
                "success": False
            }
        
        return jsonify(payload), response.status_code
        
    except requests.exceptions.Timeout:
        return jsonify({
            "message": "Onboarding service timed out. Please try again.",
            "success": False
        }), 504
    except requests.exceptions.ConnectionError:
        return jsonify({
            "message": "Onboarding service is currently unavailable.",
            "success": False
        }), 503
    except Exception as e:
        print(f"❌ proxy_delete_application error: {e}")
        return jsonify({
            "message": f"Error deleting application: {str(e)}",
            "success": False
        }), 500


@app.route('/api/onboarding/download-resume/<application_id>', methods=['GET'])
def proxy_download_resume(application_id):
    """
    Proxy for downloading resume from onboarding service.
    Returns a redirect to the presigned S3 URL.
    """
    if not application_id:
        return jsonify({
            "message": "Application ID is required",
            "success": False
        }), 400
    
    try:
        os.environ["NO_PROXY"] = ONBOARDING_SERVICE_URL
        os.environ["no_proxy"] = ONBOARDING_SERVICE_URL
        
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.get(
                f"{ONBOARDING_SERVICE_URL}/download-resume/{application_id}",
                timeout=30,
                allow_redirects=False,
            )
        
        # Check if the response is a redirect
        if response.status_code == 302 or response.status_code == 307:
            redirect_url = response.headers.get('Location')
            if redirect_url:
                return redirect(redirect_url, code=302)
        
        # If not a redirect, try to return JSON
        try:
            payload = response.json()
            return jsonify(payload), response.status_code
        except ValueError:
            # If not JSON and not redirect, return the response as-is
            return response.content, response.status_code, response.headers.items()
        
    except requests.exceptions.Timeout:
        return jsonify({
            "message": "Onboarding service timed out. Please try again.",
            "success": False
        }), 504
    except requests.exceptions.ConnectionError:
        return jsonify({
            "message": "Onboarding service is currently unavailable.",
            "success": False
        }), 503
    except Exception as e:
        print(f"❌ proxy_download_resume error: {e}")
        return jsonify({
            "message": f"Error downloading resume: {str(e)}",
            "success": False
        }), 500


@app.route('/api/onboarding/health', methods=['GET'])
def proxy_onboarding_health():
    """
    Health check for onboarding service.
    """
    try:
        os.environ["NO_PROXY"] = ONBOARDING_SERVICE_URL
        os.environ["no_proxy"] = ONBOARDING_SERVICE_URL
        
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.get(
                f"{ONBOARDING_SERVICE_URL}/health",
                timeout=5,
                allow_redirects=False,
            )
        
        try:
            payload = response.json()
        except ValueError:
            payload = {
                "status": "error",
                "service": "onboarding-service",
                "error": "Non-JSON response from health check"
            }
        
        return jsonify(payload), response.status_code
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "service": "onboarding-service",
            "error": str(e)
        }), 503

# ────────────────────────────────────────────────────────
#  ROUTE 1 — /register  (GET + POST combined)
#  EXACT same pattern as admin_register
# ────────────────────────────────────────────────────────
@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'GET':
        prefill = {}
        user_id = session.get("user_id")
        if user_id:
            status_code, prefill_data = _get_student_service_json(
                f"/api/student/resources-registration/prefill?user_id={user_id}",
                timeout=10,
            )
            if status_code == 200 and prefill_data.get("success"):
                prefill = prefill_data.get("prefill", {})

        return render_template("register-full.html", prefill=prefill)

    payload = {
        "user_id": session.get("user_id"),
        "usertype": request.form.get("usertype", ""),
        "username": request.form.get("username", ""),
        "email": request.form.get("email", ""),
        "phone": request.form.get("phone", ""),
        "location": request.form.get("location", ""),
        "gothram": request.form.get("gothram", ""),
        "employed": request.form.get("employed", ""),
        "experience": request.form.get("experience", ""),
        "skill_set": request.form.get("skill_set", ""),
        "password": request.form.get("password", ""),
        "confirm_password": request.form.get("confirm_password", ""),
    }

    status_code, response_data = _post_student_service_json(
        "/api/student/resources-registration",
        payload,
        timeout=15,
    )

    if status_code != 200 or not response_data.get("success"):
        flash(response_data.get("message") or response_data.get("detail") or "Registration failed.", "danger")
        return redirect(url_for('register'))

    flash(response_data.get("message") or "Registration successful. Please log in.", "success")
    return redirect(url_for('home'))


# ────────────────────────────────────────────────────────
#  ROUTE 2 — /create_razorpay_order
# ────────────────────────────────────────────────────────
@app.route("/create_razorpay_order", methods=["POST"])
@app.route("/create-payment-order", methods=["POST"])
def create_razorpay_order():
    data = request.get_json(silent=True) or {}
    amount = data.get("amount")
    currency = data.get("currency", "INR")
    course = data.get("course", "Registration")

    if not amount:
        return jsonify({"error": "amount required"}), 400

    try:
        amount_int = int(amount)
    except (TypeError, ValueError):
        return jsonify({"error": "amount must be an integer in paise"}), 400

    # Safe diagnostics: show key mode/length without exposing values.
    key_id_mode = "unknown"
    
    if key_id.startswith("rzp_live_"):
        key_id_mode = "live"
    elif key_id.startswith("rzp_test_"):
        key_id_mode = "test"

    key_id_has_leading_space = key_id != key_id.lstrip()
    key_id_has_trailing_space = key_id != key_id.rstrip()
    key_secret_has_leading_space = key_secret != key_secret.lstrip()
    key_secret_has_trailing_space = key_secret != key_secret.rstrip()

    print(f"[razorpay-order] key_id mode: {key_id_mode}, len: {len(key_id)}, leading_space: {key_id_has_leading_space}, trailing_space: {key_id_has_trailing_space}")
    print(f"[razorpay-order] key_secret len: {len(key_secret)}, leading_space: {key_secret_has_leading_space}, trailing_space: {key_secret_has_trailing_space}")

    if not key_id or not key_secret:
        existing_candidates = [str(p) for p in _ENV_PATH_CANDIDATES if p.exists()]
        print("❌ /create-payment-order: RZP keys missing at runtime")
        print(f"[diag] env candidates that exist: {existing_candidates}")
        print(f"[diag] process has RZP_KEY_ID: {'YES' if os.getenv('RZP_KEY_ID') else 'NO'}")
        print(f"[diag] process has RZP_KEY_SECRET: {'YES' if os.getenv('RZP_KEY_SECRET') else 'NO'}")
        return jsonify({"error": "Payment service is not configured. Please contact support."}), 500

    try:
        response = requests.post(
            RAZORPAY_ORDERS_URL,
            json={"amount": amount_int, "currency": currency, "notes": {"course": course}},
            auth=HTTPBasicAuth(key_id, key_secret),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if response.ok:
            order = response.json()
            if order.get("id"):
                print(f"✅ Razorpay order created: {order['id']}")
                return jsonify({"order_id": order["id"]}), 200
            return jsonify({"error": "Razorpay response missing order id"}), 502

        message = "Could not create order"
        try:
            error_payload = response.json() or {}
            if isinstance(error_payload, dict):
                err_obj = error_payload.get("error") if isinstance(error_payload.get("error"), dict) else {}
                detailed = err_obj.get("description") or err_obj.get("reason") or err_obj.get("code")
                if detailed:
                    message = f"Could not create order: {detailed}"
        except Exception:
            error_payload = {"raw": response.text}

        print(f"❌ Razorpay error: {response.status_code} — {error_payload}")
        return jsonify({"error": message}), 500
    except Exception as e:
        print(f"❌ Razorpay request error: {e}")
        return jsonify({"error": "Could not reach Razorpay"}), 500


@app.route("/diag/registration-service", methods=["GET"])
def diag_registration_service():
    """Quick connectivity probe for student registration dependencies."""
    def _probe(url):
        try:
            resp = requests.get(url, timeout=5)
            payload = None
            try:
                payload = resp.json()
            except Exception:
                payload = {"text": resp.text[:300]}

            return {
                "ok": resp.ok,
                "status_code": resp.status_code,
                "payload": payload,
            }
        except Exception as ex:
            return {
                "ok": False,
                "error": str(ex),
            }

    direct_base = STUDENT_SERVICE_URL.rstrip("/")
    gateway_base = STUDENT_SERVICE_URL.rstrip("/")

    direct_health = f"{direct_base}/health"
    direct_form_data = f"{direct_base}/api/student/registration/form-data"
    gateway_form_data = f"{gateway_base}/api/student/registration/form-data"

    result = {
        "STUDENT_SERVICE_URL": direct_base,
        "student_service_gateway_url": gateway_base,
        "probes": {
            "direct_health": {
                "url": direct_health,
                **_probe(direct_health),
            },
            "direct_registration_form_data": {
                "url": direct_form_data,
                **_probe(direct_form_data),
            },
            "gateway_registration_form_data": {
                "url": gateway_form_data,
                **_probe(gateway_form_data),
            },
        },
    }
    return jsonify(result), 200


#OFFERS PAGE:
@app.route('/offers_page', methods=['GET', 'POST'])
def offers_page():
    if request.method == 'POST' and 'user' not in session:
        return redirect(url_for('user_nrm_logins'))

    conn = get_db_connection()
    if not conn:
        return render_template("offers_new.html", courses=[], offers=[])

    cursor = conn.cursor(DICT_CURSOR)

    cursor.execute("SELECT id, course_name, course_fee FROM nrm_courses ORDER BY course_name ASC")
    # Snowflake DictCursor returns uppercase keys; normalise to lowercase for template consistency
    courses = [{k.lower(): v for k, v in row.items()} for row in cursor.fetchall()]

    cursor.execute("""
        SELECT o.id, o.discount_percentage, o.valid_from, o.valid_to, o.is_active,
               c.course_name, c.course_fee
        FROM nrm_offers o
        JOIN nrm_courses c ON o.course_id = c.id
        ORDER BY o.created_at DESC
    """)
    offers = [{k.lower(): v for k, v in row.items()} for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    # Apply display-name mapping
    for c in courses:
        db_name_norm = normalize(c["course_name"])
        c["display_name"] = COURSE_MAP.get(db_name_norm, c["course_name"])

    for o in offers:
        db_name_norm = normalize(o["course_name"])
        o["display_name"] = COURSE_MAP.get(db_name_norm, o["course_name"])

    return render_template("offers_new.html", courses=courses, offers=offers)

@app.route('/save_offer', methods=['POST'])
def save_offer():
    if 'user' not in session:
        return redirect(url_for('user_nrm_logins'))

    course_id = request.form.get('course_id')
    discount = request.form.get('discount')
    valid_from = request.form.get('valid_from') or None
    valid_to = request.form.get('valid_to') or None

    if valid_from:
        valid_from = datetime.strptime(valid_from, "%Y-%m-%d").date()
    if valid_to:
        valid_to = datetime.strptime(valid_to, "%Y-%m-%d").date()

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO nrm_offers
        (course_id, discount_percentage, valid_from, valid_to, is_active, created_at, updated_at)
        VALUES (%s, %s, %s, %s, 1, NOW(), NOW())
    """, (course_id, discount, valid_from, valid_to))
    conn.commit()
    cursor.close()
    conn.close()
    # Invalidate + pre-warm the global resources cache so students see the
    # new offer immediately — no stale data, no extra Snowflake calls.
    try:
        requests.post(f"{STUDENT_SERVICE_URL}/api/admin/invalidate-resources-cache", timeout=5)
    except Exception:
        pass
    return redirect(url_for('offers_page'))

@app.route('/delete_offer/<int:offer_id>', methods=['POST'])
def delete_offer(offer_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM nrm_offers WHERE id=%s", (offer_id,))
        conn.commit()
    finally:
        cursor.close()
        conn.close()
    # Invalidate + pre-warm global cache after deletion
    try:
        requests.post(f"{STUDENT_SERVICE_URL}/api/admin/invalidate-resources-cache", timeout=5)
    except Exception:
        pass
    return redirect(url_for('offers_page'))

# ---------- Video Sessions ----------
@app.route('/video-session/<tech>/<lang>')
def video_sessions(tech, lang):

    # Normalize the frontend tech name
    key = tech.lower().strip()

    # Map frontend → real DB course name
    real_course_name = COURSE_MAP.get(key)

    if not real_course_name:
        return f"No mapping found for course: {tech}"

    conn = get_db_connection()
    cur = conn.cursor(DICT_CURSOR)

    # Fetch course_id
    cur.execute("""
        SELECT ID 
        FROM nrm_courses 
        WHERE LOWER(COURSE_NAME) = LOWER(%s)
    """, (real_course_name,))
    course = cur.fetchone()

    if not course:
        conn.close()
        return f"No course found in DB for '{real_course_name}'"

    course_id = course['ID']

    # Fetch language_id
    cur.execute("""
        SELECT ID 
        FROM NRM_LANGUAGES
        WHERE LOWER(LANGUAGE) = LOWER(%s)
    """, (lang,))
    language = cur.fetchone()

    if not language:
        conn.close()
        return f"No language found for {lang}"

    language_id = language['ID']

    # Fetch videos
    cur.execute("""
        SELECT youtube_id, title, session_number
        FROM nrm_video_sessions
        WHERE course_id = %s AND language_id = %s
        ORDER BY session_number
    """, (course_id, language_id))

    sessions = cur.fetchall()
    conn.close()

    from flask import make_response
    html = render_template(
        'video-sessions.html',
        tech=real_course_name,
        lang=lang,
        sessions=sessions
    )

    resp = make_response(html)
    resp.headers['Cache-Control'] = 'public, max-age=3600'
    return resp


# --- Demo Videos Route ---
@app.route('/demo-videos')
def demo_videos():
    if 'profile_pic' not in session:
        session['profile_pic'] = 'profile_photo/defaultpicture.jpg'

    username = session.get('user')
    usertype = session.get('usertype')

    # Use same connector as video_sessions
    conn = get_db_connection()
    cur = conn.cursor(DICT_CURSOR)

    # Fetch demo videos from DB
    cur.execute("SELECT id, youtube_id, title FROM nrm_demo_videos ORDER BY id ASC")
    demos = cur.fetchall()

    conn.close()

    return render_template(
        'demo-videos.html',
        profile_pic=session['profile_pic'],
        username=username,
        usertype=usertype,
        demos=demos
    )

# ---------- Admin Report ----------
@app.route('/admin-report')
def admin_report():
    if not _has_employee_admin_access():
        flash("Access denied.", "error")
        return redirect(url_for("home"), code=303)

    conn = get_db_connection()
    cur = conn.cursor(DICT_CURSOR)

    # Get all courses
    cur.execute("SELECT ID, COURSE_NAME FROM NRM_COURSES")
    courses = cur.fetchall()

    report_data = []
    for course in courses:
        course_id = course['ID']            # ✅ FIX
        course_name = course['COURSE_NAME']

        folder_name = course_name.replace(" ", "_")

        # PPT count
        ppts_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'ppt', folder_name)
        ppt_count = len(os.listdir(ppts_dir)) if os.path.exists(ppts_dir) else 0

        # Video count from DB
        cur.execute("SELECT COUNT(*) as cnt FROM nrm_video_sessions WHERE course_id = %s", (course_id,))
        total_video_count = cur.fetchone()['cnt']

        # Interview Questions count
        iq_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'interview_questions', folder_name)
        iq_count = len(os.listdir(iq_dir)) if os.path.exists(iq_dir) else 0

        # Code count
        code_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'code', folder_name)
        code_count = len(os.listdir(code_dir)) if os.path.exists(code_dir) else 0

        report_data.append({
            'course': course_name,
            'ppt': ppt_count,
            'videos': total_video_count,
            'interview_questions': iq_count,
            'code': code_count
        })

    conn.close()
    return render_template('admin-report.html', report_data=report_data)

#student_report
@app.route('/generate-student-report', methods=['GET', 'POST'])
def generate_student_report():
    if not require_employee_login():
        session.pop("last_visited_path", None)
        session.modified = True
        flash("Please login as employee first", "error")
        return redirect(url_for("home"), code=303)

    reg_id = None
    student = None
    email_status = None
    students = []

    try:
        status_code, students_payload = _get_student_service_json(
            "/api/student/report/students",
            timeout=10,
        )
        if status_code == 200 and students_payload.get("success"):
            students = students_payload.get("data", []) or []
            # Defensive: Ensure every student dict has 'first_name' key
            for student in students:
                if 'first_name' not in student or student['first_name'] is None:
                    student['first_name'] = ''
        else:
            print(f"⚠️ student report students fetch non-200/non-success: status={status_code} payload={students_payload}")
    except Exception as e:
        print(f"❌ student report students fetch error: {e}")

    if request.method == 'POST':
        reg_id = (request.form.get('reg_id') or '').strip().upper()
        if reg_id:
            try:
                status_code, cert_payload = _post_student_service_json(
                    "/api/student/report/certificate",
                    {"reg_id": reg_id},
                    timeout=12,
                )
                if status_code == 200 and cert_payload.get("success"):
                    student = cert_payload.get("student")
                    email_status = cert_payload.get("email_status")
                elif status_code == 404:
                    email_status = "warning|⚠️ No student found for this registration ID."
                else:
                    print(f"⚠️ student report certificate fetch non-200/non-success: status={status_code} payload={cert_payload}")
                    email_status = "warning|⚠️ Unable to fetch certificate details right now."
            except Exception as e:
                print(f"❌ student report certificate fetch error: {e}")
                email_status = "warning|⚠️ Certificate service unavailable right now."

    return render_template(
        'student_report.html',
        students=students,
        reg_id=reg_id,
        student=student,
        email_status=email_status,
    )

@app.route("/student-report-view")
def student_report_view():
    if not require_employee_login():
        session.pop("last_visited_path", None)
        session.modified = True
        flash("Please login as employee first", "error")
        return redirect(url_for("home"), code=303)
    reg_id = request.args.get("reg_id")

    if not reg_id:
        return "Registration ID missing", 400

    try:
        status_code, payload = _get_student_service_json(
            f"/api/student/report/student-view?reg_id={requests.utils.quote(str(reg_id))}",
            timeout=10,
        )
        if status_code == 404:
            return "Student not found", 404
        if status_code != 200:
            return "Service unavailable", 500

        student = (payload or {}).get("student")

        if not student:
            return "Student not found", 404

        return render_template("report.html", student=student)

    except Exception as e:
        print("❌ ERROR in student_report_view:", e)
        return str(e), 500

# ==========================================
# CENTRALIZED FEEDBACK SERVICE ROUTES
# ==========================================
@app.route("/feedback", methods=["GET"])
def feedback_student_page():
    """Renders the public student feedback submission page."""
    token = (request.args.get("token") or "").rstrip("/\\").replace("%5C", "").strip()
    return render_template("student-feedback.html", token=token)



@app.route("/api/student/activities", methods=["GET"])
def api_student_activities_proxy():
    reg_id = request.args.get("reg_id", "")
    email = request.args.get("email", "")
    
    if email:
        status_code, payload = _get_feedback_service_json(f"/feedback/student/{requests.utils.quote(str(email))}")
        if status_code == 200 and payload.get("success") and payload.get("activities"):
            # Format activities list for modal
            acts = []
            for a in payload.get("activities", []):
                acts.append({
                    "id": a.get("ACTIVITY_ID") or a.get("id"),
                    "type": a.get("MODULE_NAME") or a.get("type"),
                    "label": f"{a.get('MODULE_NAME')}: {a.get('ACTIVITY_NAME')}",
                    "eligible": bool(a.get("ELIGIBLE_FOR_FEEDBACK", 1))
                })
            return jsonify({"success": True, "activities": acts}), 200

    # Fallback to student_service lookup
    status_code, payload = _get_student_service_json(f"/api/student/activities?reg_id={requests.utils.quote(str(reg_id))}")
    return jsonify(payload), status_code

@app.route("/api/student/feedback/generate", methods=["POST"])
def api_feedback_generate_proxy():
    data = request.get_json() or {}
    
    if "activities" in data and ("activity_ids" not in data or not data["activity_ids"]):
        data["activity_ids"] = [
            a.get("activity_id") or a.get("id")
            for a in data.get("activities", [])
            if isinstance(a, dict) and (a.get("activity_id") or a.get("id"))
        ]

    status_code, payload = _post_feedback_service_json("/feedback/generate", data)
    
    if isinstance(payload, dict) and payload.get("success"):
        if "requests" not in payload or not payload["requests"]:
            feedback_url = payload.get("feedback_url", "")
            feedback_id = payload.get("feedback_id", "")
            payload["requests"] = [{
                "feedback_url": feedback_url,
                "feedback_id": feedback_id
            }]

    # Automatically trigger send email if generated successfully
    if status_code == 200 and isinstance(payload, dict) and payload.get("success"):
        feedback_id = payload.get("feedback_id")
        student_email = data.get("student_email")
        student_name = data.get("student_name", "Student")
        expiry_hours = data.get("expiry_hours", 24)
        
        if feedback_id and student_email:
            _post_feedback_service_json("/feedback/send", {
                "feedback_id": feedback_id,
                "student_name": student_name,
                "student_email": student_email,
                "expiry_hours": expiry_hours
            })
            payload["email_sent"] = True

    return jsonify(payload), status_code


@app.route("/api/student/feedback/send", methods=["POST"])
def api_feedback_send_proxy():
    data = request.get_json() or {}
    status_code, payload = _post_feedback_service_json("/feedback/send", data)
    return jsonify(payload), status_code

@app.route("/api/student/feedback/token/<path:token>", methods=["GET"])
@app.route("/api/student/feedback/token/", methods=["GET"])
def api_feedback_token_proxy(token=""):
    if not token:
        token = request.args.get("token", "")
    token = token.rstrip("/\\").replace("%5C", "").strip()
    status_code, payload = _get_feedback_service_json(f"/feedback/{token}")
    return jsonify(payload), status_code


@app.route("/api/student/feedback/submit", methods=["POST"])
def api_feedback_submit_proxy():
    data = request.get_json() or {}
    status_code, payload = _post_feedback_service_json("/feedback/submit", data)
    return jsonify(payload), status_code


@app.route("/api/student/feedback/status", methods=["GET"])
def api_feedback_status_proxy():
    status_code, payload = _get_feedback_service_json("/feedback/status")
    return jsonify(payload), status_code

@app.route("/api/student/feedback/report", methods=["GET"])
def api_feedback_report_proxy():
    status_code, payload = _get_feedback_service_json("/feedback/report")
    return jsonify(payload), status_code

# ==========================================
# FETCH STUDENTS FOR REPORT PAGE
# ==========================================

@app.route("/students")
def fetch_students():
    try:
        status_code, payload = _get_student_service_json(
            "/api/student/report/students",
            timeout=10,
        )
        if status_code != 200:
            return jsonify({"status": "error", "message": "Service unavailable"})

        return jsonify({"status": "success", "data": payload.get("data", [])})

    except Exception as e:
        print("❌ ERROR in /students:", e)
        return jsonify({"status": "error", "message": str(e)})

@app.route("/api/student/report/all-activities")
def fetch_all_student_activities_proxy():
    try:
        status_code, payload = _get_student_service_json(
            "/api/student/report/all-activities",
            timeout=10,
        )
        if status_code != 200:
            return jsonify({"success": False, "data": {}}), status_code
        return jsonify(payload)
    except Exception as e:
        print("❌ ERROR in fetch_all_student_activities_proxy:", e)
        return jsonify({"success": False, "data": {}}), 500
        
@app.route("/student-report")
def student_report_page():
    return redirect(url_for("generate_student_report"), code=302)

@app.route('/delete_student/<registration_id>', methods=['POST'])
def delete_student(registration_id):
    try:
        resp = requests.delete(
            f"{STUDENT_SERVICE_URL}/api/student/report/student/{registration_id}",
            timeout=12,
        )
        if resp.status_code == 404:
            return jsonify({"success": False, "message": "Student not found."})
        if not resp.ok:
            return jsonify({"success": False, "message": "Service unavailable."})

        payload = resp.json() if resp.content else {}
        return jsonify({
            "success": bool(payload.get("success", True)),
            "message": payload.get("message", "Student deleted successfully."),
        })

    except Exception as e:
        print(f"❌ Delete Error: {e}")
        return jsonify({"success": False, "message": f"Error deleting student: {str(e)}"})

# Set session lifetime globally
# Keep the user logged in for 7 days unless they logout
app.permanent_session_lifetime = timedelta(days=7)

                 

@app.route("/aboutus")
def aboutus():
    return render_template("aboutus.html")

@app.route('/internships', methods=['GET'])
def internships():
    current_host = (request.headers.get("X-Forwarded-Host") or request.host or "").split(":")[0].strip().lower()
    if INTERNSHIP_PUBLIC_HOST and current_host != INTERNSHIP_PUBLIC_HOST:
        target_url = f"https://{INTERNSHIP_PUBLIC_HOST}{request.path}"
        if request.query_string:
            target_url = f"{target_url}?{request.query_string.decode('utf-8', errors='ignore')}"
        return redirect(target_url, code=308)

    return render_template('Internships.html')


# -------------------------------------------------------
# ADMIN REGISTER (GET + POST) — FINAL & CORRECT
# -------------------------------------------------------

@app.route('/admin_employee_login', methods=['POST'])
def admin_employee_login():
    """Verify employee credentials by proxying to student service."""
    data = request.json or {}
    username = data.get('username', '').strip()
    password = data.get('password', '').strip()

    if not username or not password:
        return jsonify({'success': False, 'message': 'Username and password are required'})

    try:
        status_code, response_data = _post_student_service_json(
            "/api/student/admin-employee-login",
            {"username": username, "password": password},
            timeout=10,
        )

        if status_code != 200:
            message = response_data.get('message') or response_data.get('detail') or 'Login verification failed'
            return jsonify({'success': False, 'message': message})

        if not response_data.get('success'):
            return jsonify({
                'success': False,
                'message': response_data.get('message', 'Login verification failed')
            })

        usertype = (response_data.get('usertype') or '').lower()
        verified_user_id = response_data.get('user_id')
        verified_email = response_data.get('email')

        session['admin_verified'] = True
        session['verified_user_id'] = verified_user_id
        session['verified_email'] = verified_email
        session['verified_at'] = datetime.now().isoformat()

        return jsonify({
            'success': True,
            'message': 'Login successful',
            'usertype': usertype
        })

    except Exception as e:
        print(f"Admin login error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'Login verification failed'})


ADMIN_EMAIL = (os.getenv("ADMIN_EMAIL") or "admin@chakorahub.com").strip()  # Replace with your actual admin email
  
# DEBUG: Print credential status (without revealing full keys)
print("\n" + "="*50)
print("AWS CREDENTIALS DEBUG INFO")
print("="*50)
print(f"AWS_ACCESS_KEY set: {'Yes' if AWS_ACCESS_KEY and AWS_ACCESS_KEY != 'YOUR_ACCESS_KEY_HERE' else 'No'}")
print(f"AWS_SECRET_KEY set: {'Yes' if AWS_SECRET_KEY and AWS_SECRET_KEY != 'YOUR_SECRET_KEY_HERE' else 'No'}")
print(f"AWS_REGION: {AWS_REGION}")
print(f"ADMIN_EMAIL: {ADMIN_EMAIL}")

if AWS_ACCESS_KEY and AWS_ACCESS_KEY != 'YOUR_ACCESS_KEY_HERE':
    print(f"Access Key (first 4 chars): {AWS_ACCESS_KEY[:4]}...")
if AWS_SECRET_KEY and AWS_SECRET_KEY != 'YOUR_SECRET_KEY_HERE':
    print(f"Secret Key (first 4 chars): {AWS_SECRET_KEY[:4]}...")
print("="*50 + "\n")

# Initialize AWS SES client with credentials
try:
    if AWS_ACCESS_KEY and AWS_SECRET_KEY and AWS_ACCESS_KEY != 'YOUR_ACCESS_KEY_HERE' and AWS_SECRET_KEY != 'YOUR_SECRET_KEY_HERE':
        ses = boto3.client(
            'ses',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        print("✅ AWS SES client initialized (lazy validation mode)")
    else:
        print("❌ AWS credentials are missing or still using placeholder values")
        ses = None
        
except Exception as e:
    print(f"❌ Failed to initialize AWS SES client: {str(e)}")
    ses = None

def send_registration_email(user_email, name, registration_id, user_type, registration_data=None):
    try:
        if ses is None:
            print("❌ SES email skipped: AWS SES client is not initialized. Check AWS credentials/region.")
            return False

        if registration_data is None:
            registration_data = {}

        if user_type == 'student':
            subject = f"✅ Student Registration Confirmed - {registration_id}"
            header_title = "🎓 Student Registration"
            id_label = "Student ID"
        else:
            subject = f"✅ Employee Registration Confirmed - {registration_id}"
            header_title = "👔 Employee Registration"
            id_label = "Employee ID"

        table_rows = ""
        for key, value in registration_data.items():
            if value and value != "Not provided":
                label = key.replace("_", " ").title()
                table_rows += f"""
                <tr>
                    <td style="background-color:#f8f9fa; font-weight:bold; width:40%;
                               color:#000000; padding:12px; border-bottom:1px solid #e0e0e0;">
                        {label}
                    </td>
                    <td style="background-color:#ffffff; width:60%;
                               color:#000000; padding:12px; border-bottom:1px solid #e0e0e0;">
                        {value}
                    </td>
                </tr>
                """

        html_content = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
</head>
<body style="margin:0; padding:0; font-family:Arial, sans-serif; color:#000000; background-color:#ffffff;">
<div style="max-width:600px; margin:0 auto; border:1px solid #e0e0e0; border-radius:8px; overflow:hidden;">

    <div style="background-color:#667eea; color:#ffffff; padding:20px; text-align:center;">
        <h2 style="margin:0; font-size:24px;">{header_title}</h2>
        <p style="margin:5px 0 0;">Registration Confirmation</p>
    </div>

    <div style="padding:20px;">
        <p>Dear <strong>{name}</strong>,</p>
        <p>Thank you for registering with <strong>ChakoraHub</strong>. Your registration has been successfully completed.</p>

        <div style="background-color:#f8f9fa; border-left:4px solid #667eea; padding:15px; margin:20px 0; font-size:16px;">
            <strong style="color:#000000;">📋 {id_label}: {registration_id}</strong><br>
            <small style="color:#555555;">Please keep this ID for future reference</small>
        </div>

        <div style="background-color:#e8f5e9; border:2px solid #4caf50; padding:15px; border-radius:6px; margin:20px 0;">
            <h4 style="margin-top:0; color:#2e7d32;">🔐 Login Credentials</h4>
            <div style="background:#ffffff; padding:8px; margin-bottom:6px; border:1px solid #c8e6c9; font-family:monospace; color:#000000;">
                <strong>Email:</strong> {user_email}
            </div>
            <div style="background:#ffffff; padding:8px; border:1px solid #c8e6c9; font-family:monospace; color:#000000;">
                <strong>Default Password:</strong> changeme123
            </div>
            <p style="margin-top:10px; color:#2e7d32; font-size:14px;">
                ⚠️ <strong>Please change your password after first login.</strong>
            </p>
        </div>

        <h3 style="color:#667eea;">Registration Details</h3>
        <table style="width:100%; border-collapse:collapse; border:1px solid #e0e0e0;">
            <tr>
                <th colspan="2" style="background-color:#667eea; color:#ffffff; padding:12px; text-align:left;">
                    Registration Information
                </th>
            </tr>
            {table_rows}
        </table>

        <div style="background-color:#fff3e0; border:1px solid #ffe0b2; padding:12px; margin-top:20px;">
            <strong>📌 Next Steps:</strong>
            <ul style="margin:8px 0 0 18px; padding:0;">
                <li>Visit: <a href="https://www.chakorahub.com/login" style="color:#667eea;">www.chakorahub.com/login</a></li>
                <li>Login with your email &amp; password</li>
                <li>Change your password</li>
                <li>Complete your profile</li>
            </ul>
        </div>

        <div style="margin-top:30px; border-top:1px solid #e0e0e0; padding-top:15px; text-align:center;">
            <p style="margin:0;">Regards,<br><strong>ChakoraHub Team</strong></p>
            <p style="font-size:12px; color:#777777; margin-top:10px;">This is an automated email. Please do not reply.</p>
        </div>
    </div>
</div>
</body>
</html>
"""

        text_content = f"""
Dear {name},

Your registration with ChakoraHub is successful.

{id_label}: {registration_id}

LOGIN DETAILS:
Email: {user_email}
Password: changeme123

Please change your password after login.

Regards,
ChakoraHub Team
"""

        ses.send_email(
            Source=ADMIN_EMAIL,
            Destination={
                "ToAddresses": [user_email],
                "CcAddresses": [ADMIN_EMAIL]
            },
            Message={
                "Subject": {"Data": subject},
                "Body": {
                    "Html": {"Data": html_content},
                    "Text": {"Data": text_content}
                }
            }
        )

        print(f"✅ Email sent successfully to {user_email}")
        return True

    except Exception as e:
        import traceback
        print("❌ Email sending failed:", e)
        print("❌ SES traceback:", traceback.format_exc())
        return False


def send_registration_whatsapp_message(recipient_number, name, registration_id, user_type):
    recipient_number = (recipient_number or "").strip()
    if not recipient_number:
        print("⚠️ WhatsApp notification skipped: no recipient number configured.")
        return False

    if not WABA_SERVICE_URL:
        print("⚠️ WhatsApp notification skipped: WABA service URL is missing.")
        return False

    message = (
        f"ChakoraHub registration completed for {name}. "
        f"{user_type.title()} ID: {registration_id}."
    )

    try:
        response = requests.post(
            f"{WABA_SERVICE_URL}/send-registration-message",
            json={
                "phone_number": recipient_number,
                "message": message,
            },
            timeout=20,
        )

        try:
            response_data = response.json()
        except ValueError:
            response_data = {"raw_response": response.text[:300]}

        if response.ok:
            print(f"✅ WhatsApp notification sent to {recipient_number}: {response_data}")
            return True

        print(
            "⚠️ WhatsApp notification failed: "
            f"status={response.status_code}, response={response_data}"
        )
        return False
    except Exception as exc:
        print(f"⚠️ WhatsApp notification error: {exc}")
        return False


@app.route("/registration", methods=["GET", "POST"])
def admin_register():
    # Student registration is owned by the student microservice.
    student_form_data = {
        "offerings": [],
        "languages": [],
        "payment_key_id": (os.getenv("RZP_KEY_ID") or "").strip(),
    }

    if request.method == "GET":
        conn = None
        cur = None
        try:
            conn = get_db_connection()
            cur = conn.cursor(DICT_CURSOR)
            cur.execute(
                """
                SELECT
                    o.offering_id,
                    o.course_id,
                    c.course_name,
                    c.course_code,
                    o.registration_category,
                    CAST(ROUND(COALESCE(o.course_fee, 0)) AS INT) AS course_fee
                FROM nrm_course_offerings o
                JOIN nrm_courses c ON c.id = o.course_id
                WHERE o.is_active = 'Y' OR o.is_active IS NULL
                ORDER BY o.registration_category, c.course_name
                """
            )
            student_form_data["offerings"] = cur.fetchall() or []
        except Exception as offerings_err:
            print(f"⚠️ /registration GET could not load offerings from DB: {offerings_err}")
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

        try:
            form_resp = requests.get(
                f"{STUDENT_SERVICE_URL}/api/student/registration/form-data",
                timeout=5,
            )
            if form_resp.ok:
                form_data = form_resp.json() or {}
                student_form_data["languages"] = form_data.get("languages", [])
                student_form_data["payment_key_id"] = form_data.get("payment_key_id") or (os.getenv("RZP_KEY_ID") or "").strip()
        except Exception as proxy_err:
            print(f"⚠️ /registration GET could not load student form data from student-service: {proxy_err}")

    offerings, languages = student_form_data["offerings"], student_form_data["languages"]

    if request.method == "GET":
        return render_template("registration.html",
                               offerings=offerings,
                               languages=languages,
                               payment_key_id=student_form_data["payment_key_id"])

    # ---------- POST REQUEST (JSON) ----------
    data = request.get_json()

    if not data:
        return jsonify({"success": False, "message": "No data received"}), 400

    registration_type = data.get("registration_type", "").strip()
    fname = data.get("first_name", "").strip()
    lname = data.get("last_name", "").strip()
    email = data.get("email", "").strip().lower()
    phone = data.get("phone", "").strip()
    location_val = data.get("location", "") or data.get("personal_location", "")
    location_val = location_val.strip()

    print(f"📝 Processing {registration_type} registration for: {email}")

    if registration_type not in ['student_co', 'student_pl', 'student_ws']:
        return jsonify({"success": False, "message": "Invalid registration type."}), 400

    if not fname or len(fname) < 2:
        return jsonify({"success": False, "message": "First name must be at least 2 characters."}), 400

    if not lname or len(lname) < 2:
        return jsonify({"success": False, "message": "Last name must be at least 2 characters."}), 400

    if not email:
        return jsonify({"success": False, "message": "Email is required."}), 400

    if not (email.endswith("@gmail.com") or email.endswith("@chakorahub.com")):
        return jsonify({"success": False, "message": "Email must end with @gmail.com or @chakorahub.com"}), 400

    if not phone.isdigit() or len(phone) != 10:
        return jsonify({"success": False, "message": "Phone number must be exactly 10 digits."}), 400

    if not location_val:
        return jsonify({"success": False, "message": "Location is required."}), 400

    if registration_type in ['student_co', 'student_pl', 'student_ws']:
        endpoint = "/api/student/registration"
        target_url = f"{STUDENT_SERVICE_URL}{endpoint}"
        attempt_results = []
        last_error = None

        _student_host = urllib.parse.urlparse(STUDENT_SERVICE_URL).hostname or ""
        _no_proxy_value = ",".join([h for h in [STUDENT_INTERNAL_NO_PROXY, _student_host] if h])
        os.environ["NO_PROXY"] = _no_proxy_value
        os.environ["no_proxy"] = _no_proxy_value

        print(
            "[registration-proxy] setup "
            f"student_service_url={STUDENT_SERVICE_URL} "
            f"target_url={target_url} "
            f"student_host={_student_host} "
            f"no_proxy={_no_proxy_value} "
            f"registration_type={registration_type} "
            f"email={email}"
        )

        for attempt in range(1, 3):
            try:
                print(f"📤 Proxying student registration to {target_url} (attempt {attempt}/2)")
                with requests.Session() as internal_session:
                    internal_session.trust_env = False
                    internal_session.proxies = {"http": None, "https": None}
                    svc_resp = internal_session.post(
                        target_url,
                        json=data,
                        timeout=45,
                        allow_redirects=False,
                    )

                print(f"✅ Student service responded with status {svc_resp.status_code} from {target_url}")
                if svc_resp.status_code >= 400:
                    print(
                        "[registration-proxy] non-2xx response "
                        f"status={svc_resp.status_code} "
                        f"content_type={svc_resp.headers.get('Content-Type')} "
                        f"preview={(svc_resp.text or '').replace(chr(10), ' ')[:400]}"
                    )
                attempt_results.append({
                    "url": target_url,
                    "attempt": attempt,
                    "status": svc_resp.status_code,
                })

                try:
                    result = svc_resp.json()
                except ValueError:
                    preview = (svc_resp.text or "").replace("\n", " ").strip()[:300]
                    result = {
                        "success": False,
                        "message": "Registration service returned non-JSON response",
                        "upstream_status": svc_resp.status_code,
                        "upstream_preview": preview,
                    }
                    print(
                        "[registration-proxy] JSON decode failed "
                        f"status={svc_resp.status_code} preview={preview}"
                    )

                if isinstance(result, dict) and "message" not in result and result.get("detail"):
                    result["message"] = str(result.get("detail"))
                if isinstance(result, dict) and "success" not in result and svc_resp.status_code >= 400:
                    result["success"] = False

                if isinstance(result, dict) and result.get("success"):
                    send_registration_whatsapp_message(
                        phone,
                        f"{fname} {lname}",
                        str(result.get("registration_id") or ""),
                        "student",
                    )

                return jsonify(result), svc_resp.status_code

            except requests.ConnectionError as conn_err:
                last_error = str(conn_err)
                attempt_results.append({
                    "url": target_url,
                    "attempt": attempt,
                    "error": str(conn_err),
                })
                print(
                    f"❌ Connection error to student service at {target_url} "
                    f"(attempt {attempt}/2): {conn_err} | no_proxy={_no_proxy_value}"
                )
                if attempt == 2:
                    break
                time.sleep(0.5)
                continue
            except requests.Timeout as timeout_err:
                last_error = str(timeout_err)
                attempt_results.append({
                    "url": target_url,
                    "attempt": attempt,
                    "error": f"timeout: {timeout_err}",
                })
                print(
                    f"❌ Timeout calling student service at {target_url} "
                    f"(attempt {attempt}/2): {timeout_err}"
                )
                if attempt == 2:
                    break
                time.sleep(0.5)
                continue
            except Exception as proxy_err:
                import traceback
                last_error = str(proxy_err)
                attempt_results.append({
                    "url": target_url,
                    "attempt": attempt,
                    "error": str(proxy_err),
                })
                print(
                    f"❌ Student service proxy error at {target_url} "
                    f"(attempt {attempt}/2): {proxy_err} "
                    f"| type={type(proxy_err).__name__}"
                )
                traceback.print_exc()
                break

        details = f" Last error: {last_error}" if last_error else ""
        print(
            "⚠️ Student registration proxy unavailable. "
            f"Tried: {target_url}.{details} attempts={attempt_results}"
        )
        return jsonify({
            "success": False,
            "message": "Student registration is currently unavailable. Please try again shortly.",
        }), 503

    return jsonify({
        "success": False,
        "message": "Invalid registration type."
    }), 400


@app.route("/admin-register", methods=["GET", "POST"])
def admin_register_legacy_route():
    """Backward-compatible route. Prefer /registration."""
    if request.method == "GET":
        return redirect(url_for("admin_register"), code=301)
    return admin_register()


@app.route('/registration/upload-resume', methods=['POST'])
def registration_upload_resume():
    file = request.files.get('resume')
    if not file or not file.filename:
        return jsonify({"success": False, "message": "Resume file is required."}), 400

    registration_type = (request.form.get('registration_type') or 'student_pl').strip().lower()

    try:
        _student_host = urllib.parse.urlparse(STUDENT_SERVICE_URL).hostname or ""
        _no_proxy_value = ",".join([h for h in [STUDENT_INTERNAL_NO_PROXY, _student_host] if h])
        os.environ["NO_PROXY"] = _no_proxy_value
        os.environ["no_proxy"] = _no_proxy_value

        files = {
            "resume": (file.filename, file.read(), file.mimetype or "application/octet-stream")
        }
        data = {
            "registration_type": registration_type
        }

        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            svc_resp = internal_session.post(
                f"{STUDENT_SERVICE_URL}/api/student/registration/upload-resume",
                files=files,
                data=data,
                timeout=45,
                allow_redirects=False,
            )

        try:
            payload = svc_resp.json()
        except ValueError:
            payload = {
                "success": False,
                "message": "Resume upload service returned non-JSON response"
            }

        return jsonify(payload), svc_resp.status_code

    except Exception as e:
        print(f"❌ Resume upload proxy error: {e}")
        return jsonify({"success": False, "message": "Resume upload failed. Please try again."}), 500


# ---------- DUPLICATE CHECKS ----------
@app.route('/check_admin_email', methods=['POST'])
def check_admin_email():
    email = request.json.get('email', '').strip().lower()
    if not email:
        return jsonify({'exists': False})
    exists = False
    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT 1 FROM nrm_users WHERE LOWER(email) = %s", (email,))
            exists = cursor.fetchone() is not None
        except Exception as e:
            print(f"Email check error: {e}")
        finally:
            cursor.close()
            connection.close()
    return jsonify({'exists': exists})


@app.route('/check_admin_phone', methods=['POST'])
def check_admin_phone():
    phone = request.json.get('phone', '').strip()
    if not phone:
        return jsonify({'exists': False})
    exists = False
    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT 1 FROM nrm_users WHERE phone = %s", (phone,))
            exists = cursor.fetchone() is not None
        except Exception as e:
            print(f"Phone check error: {e}")
        finally:
            cursor.close()
            connection.close()
    return jsonify({'exists': exists})


@app.route('/admin_registration_logout', methods=['POST'])
def admin_registration_logout():
    session.pop('admin_verified', None)
    session.pop('verified_user_id', None)
    session.pop('verified_email', None)
    session.pop('verified_at', None)
    return jsonify({'success': True, 'message': 'Logged out successfully'})


# ============================================================================
# ADDITIONAL HELPER ENDPOINTS FOR DROPDOWN DATA
# ============================================================================

@app.route('/api/get_departments', methods=['GET'])
def get_departments():
    connection = get_db_connection()
    departments = []
    if connection:
        try:
            cursor = connection.cursor(DICT_CURSOR)
            cursor.execute("SELECT DEPT_ID, DEPT_NAME FROM EMP_NRM_DEPARTMENTS ORDER BY DEPT_NAME")
            rows = cursor.fetchall()
            departments = [{"id": r["DEPT_ID"], "name": r["DEPT_NAME"]} for r in rows]
        except Exception as e:
            print(f"Error fetching departments: {e}")
        finally:
            cursor.close()
            connection.close()
    return jsonify({"departments": departments})


@app.route('/api/get_designations', methods=['GET'])
def get_designations():
    connection = get_db_connection()
    designations = []
    if connection:
        try:
            cursor = connection.cursor(DICT_CURSOR)
            cursor.execute("SELECT DESIGNATION_ID, TITLE FROM EMP_NRM_DESIGNATIONS ORDER BY TITLE")
            rows = cursor.fetchall()
            designations = [{"id": r["DESIGNATION_ID"], "title": r["TITLE"]} for r in rows]
        except Exception as e:
            print(f"Error fetching designations: {e}")
        finally:
            cursor.close()
            connection.close()
    return jsonify({"designations": designations})


@app.route('/api/get_managers', methods=['GET'])
def get_managers():
    connection = get_db_connection()
    managers = []
    if connection:
        try:
            cursor = connection.cursor(DICT_CURSOR)
            cursor.execute("SELECT MANAGER_ID, MANAGER_NAME FROM EMP_NRM_MANAGERS ORDER BY MANAGER_NAME")
            rows = cursor.fetchall()
            managers = [{"id": r["MANAGER_ID"], "name": r["MANAGER_NAME"]} for r in rows]
        except Exception as e:
            print(f"Error fetching managers: {e}")
        finally:
            cursor.close()
            connection.close()
    return jsonify({"managers": managers})


@app.route('/api/get_locations', methods=['GET'])
def get_locations():
    connection = get_db_connection()
    locations = []
    if connection:
        try:
            cursor = connection.cursor(DICT_CURSOR)
            cursor.execute("SELECT LOCATION_ID, BRANCH_NAME FROM EMP_NRM_LOCATIONS ORDER BY BRANCH_NAME")
            rows = cursor.fetchall()
            locations = [{"id": r["LOCATION_ID"], "name": r["BRANCH_NAME"]} for r in rows]
        except Exception as e:
            print(f"Error fetching locations: {e}")
        finally:
            cursor.close()
            connection.close()
    return jsonify({"locations": locations})

   

#nrm_enquiries
# Zapier Webhook URL
ZAPIER_WEBHOOK_URL = "https://hooks.zapier.com/hooks/catch/25218395/u8dirlt/"

# ==========================================
# ENQUIRY ROUTES (Proxy to Student Service)
# ==========================================


@app.route('/enquiry', methods=['GET'])
@app.route('/nrm_enquiries', methods=['GET'])
def nrm_enquiries():
    """Display the enquiry form page"""
    return render_template('enquiry.html')


@app.route('/submit_nrm_enquiries', methods=['POST'])
def submit_nrm_enquiries():
    """Handle enquiry submissions with proper error handling"""
    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
    
    # Get form data
    name = request.form.get('name', '').strip()
    email = request.form.get('email', '').strip()
    phone = request.form.get('phone', '').strip()
    enquiry_text = request.form.get('enquiry', '').strip()

    # Validate required fields
    if not all([name, email, phone, enquiry_text]):
        if is_ajax:
            return jsonify({'success': False, 'message': 'All fields are required.'}), 400
        else:
            flash("All fields are required.", "error")
            return redirect(url_for('home'))

    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        
        if not conn:
            error_msg = "Database connection failed. Please try again later."
            print("❌ DB Connection failed in submit_nrm_enquiries")
            if is_ajax:
                return jsonify({'success': False, 'message': error_msg}), 500
            else:
                flash(error_msg, "error")
                return redirect(url_for('home'))
        
        cursor = conn.cursor(DICT_CURSOR)
        
        user_id = None
        
        # Check if user is logged in
        if 'user' in session:
            user_email = session['user']
            
            # Get user ID from nrm_users table for logged-in users
            cursor.execute("SELECT * FROM nrm_users WHERE email = %s", (user_email,))
            user_row = cursor.fetchone()
            
            if user_row:
                # Try to get the ID - with multiple fallbacks
                if 'ID' in user_row:
                    user_id = user_row['ID']
                elif 'id' in user_row:
                    user_id = user_row['id']
                elif 'USER_ID' in user_row:
                    user_id = user_row['USER_ID']
                else:
                    # Try to get the first column value
                    user_id = list(user_row.values())[0]

        # 🔧 FIXED: Removed Python comment from SQL string
        cursor.execute("""
            INSERT INTO nrm_enquiries (student_id, name, email, phone, enquiry, created_at, is_guest_enquiry)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), %s)
        """, (user_id, name, email, phone, enquiry_text, user_id is None))
        
        conn.commit()
        print(f"✅ Enquiry saved to database for {name} ({email})")

        # Send enquiry to AWS Step Functions (non-critical)
        payload = {
            "name": name,
            "email": email,
            "phone": phone,
            "enquiry": enquiry_text,
            "source": "guest" if user_id is None else "logged_in_user",
            "user_id": user_id
        }

        try:
            sf_response = sf_client.start_execution(
                stateMachineArn="arn:aws:states:eu-north-1:196527705786:stateMachine:ChakoraHub-Enquiry",
                input=json.dumps(payload)
            )
            print(f"✅ Step Functions execution started: {sf_response.get('executionArn', 'N/A')}")
        except Exception as sf_err:
            print(f"⚠️ Step Functions execution failed (non-critical): {sf_err}")
            # Don't fail the whole request if Step Functions fails

        # Return success response
        success_message = "Your enquiry was submitted successfully! We'll get back to you soon."
        if is_ajax:
            return jsonify({'success': True, 'message': success_message}), 200
        else:
            flash(success_message, "success")
            return redirect(url_for('nrm_enquiries'))
            
    except Exception as e:
        print(f"❌ ERROR in submit_nrm_enquiries: {e}")
        import traceback
        print(f"❌ TRACEBACK: {traceback.format_exc()}")
        
        error_message = "Error submitting enquiry. Please try again later."
        if is_ajax:
            return jsonify({'success': False, 'message': error_message}), 500
        else:
            flash(error_message, "error")
            return redirect(url_for('nrm_enquiries'))
            
    finally:
        # 🔧 FIXED: Proper cleanup in finally block
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass




#contactus
@app.route('/contact')
def contact():
    return render_template('contactus.html')


#---------upload-------
# Upload Page - Only for Admin
def extract_text_from_pdf(pdf_path):
    try:
        import fitz  # PyMuPDF
    except ImportError:
        return "ERROR: PyMuPDF (fitz) not installed."

    html_content = ""
    try:
        doc = fitz.open(pdf_path)
        for page in doc:
            blocks = page.get_text("dict")["blocks"]
            for block in blocks:
                if "lines" in block:
                    for line in block["lines"]:
                        for span in line["spans"]:
                            text = span["text"].replace("\n", "<br>")
                            if span["flags"] & 2:
                                html_content += f"<b>{text}</b>"
                            else:
                                html_content += text
                        html_content += "<br>"
            html_content += "<br>"
        doc.close()
    except Exception as e:
        print("Error reading PDF:", e)
        return f"ERROR: {e}"

    return html_content

# --- Admin Upload Page ---
# Root upload directory

# Allowed extensions per category
BASE_UPLOADS = os.path.join(os.getcwd(), "uploads")
app.config['UPLOAD_FOLDER'] = BASE_UPLOADS
app.config['SYLLABUS_FOLDER'] = os.path.join(BASE_UPLOADS, "syllabus")

#------------------ Syllabus Routes ------------------#
# ========================= SYLLABUS MAIN PAGE =========================
@app.route('/syllabus', methods=["GET", "POST"])
def syllabus_page():
    conn = get_db_connection()
    cursor = conn.cursor(DICT_CURSOR)

    # ✅ Fetch courses with file_path
    cursor.execute("""
        SELECT ns.id, nc.course_name, ns.file_path
        FROM nrm_syllabus ns
        JOIN nrm_courses nc ON ns.course_id = nc.id
        ORDER BY nc.course_name ASC
    """)
    courses = cursor.fetchall()
    cursor.close()
    conn.close()

    # ✅ Get user profile image from session or DB
    user_image = 'default.png'
    if 'user' in session:
        username = session['user']
        conn = get_db_connection()
        cursor = conn.cursor(DICT_CURSOR)
        cursor.execute("SELECT profile_pic FROM nrm_users WHERE username = %s", (username,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        if user:
            user_image = user.get('PROFILE_PIC') or user.get('profile_pic') or user_image

    return render_template('syllabus.html', courses=courses, user_image=user_image)

# ========================= FETCH A SPECIFIC SYLLABUS =========================
@app.route('/syllabus/<int:course_id>')
def get_syllabus(course_id):
    conn = get_db_connection()
    cursor = conn.cursor(DICT_CURSOR)

    cursor.execute("""
        SELECT ns.id, nc.course_name, ns.file_path
        FROM nrm_syllabus ns
        JOIN nrm_courses nc ON ns.course_id = nc.id
        WHERE ns.id = %s
    """, (course_id,))
    course = cursor.fetchone()

    cursor.close()
    conn.close()

    if not course:
        return jsonify({})

    course_name = course.get('COURSE_NAME') or course.get('course_name')
    file_path = course.get('FILE_PATH') or course.get('file_path')

    return jsonify({
        'course_name': course_name,
        'pdf_url': file_path,
        'is_admin': session.get('role') == 'admin'
    })

# ========================= SERVE PDF FILES =========================
@app.route('/uploads/syllabus/<path:filename>')
def serve_syllabus_file(filename):
    return send_from_directory(app.config['SYLLABUS_FOLDER'], filename)

# ========================= DELETE A SYLLABUS =========================
@app.route('/syllabus/delete/<int:course_id>', methods=['POST'])
def delete_syllabus(course_id):
    if session.get('role') != 'admin':
        return jsonify(success=False), 403

    conn = get_db_connection()
    cursor = conn.cursor(DICT_CURSOR)
    cursor.execute("SELECT file_path FROM nrm_syllabus WHERE id = %s", (course_id,))
    result = cursor.fetchone()

    if result:
        try:
            file_path = result.get('FILE_PATH') or result.get('file_path')
            # Only try deleting local files; skip remote URLs (for S3-backed paths).
            if file_path and not str(file_path).startswith(('http://', 'https://')):
                abs_local = os.path.join(script_dir, str(file_path).lstrip('/'))
                if os.path.exists(abs_local):
                    os.remove(abs_local)

            cursor.execute("DELETE FROM nrm_syllabus WHERE id = %s", (course_id,))
            conn.commit()
            cursor.close()
            conn.close()
            return jsonify(success=True)
        except Exception as e:
            print("❌ Error deleting syllabus:", e)
            cursor.close()
            conn.close()
            return jsonify(success=False)
    else:
        cursor.close()
        conn.close()
        return jsonify(success=False)

# ================== Upload Page + Courses ================== #


@app.route('/api/courses')
def get_courses():
    try:
        resp = requests.get(
            f"{STUDENT_SERVICE_URL}/api/student/admin/courses",
            timeout=10,
        )
    except requests.RequestException as exc:
        return jsonify({"error": "Student service unavailable", "detail": str(exc)}), 502

    try:
        payload = resp.json()
    except ValueError:
        return jsonify({"error": "Student service returned non-JSON response"}), 502

    return jsonify(payload if isinstance(payload, list) else payload.get("courses", [])), resp.status_code


@app.route('/api/course-offerings')
def get_course_offerings():
    try:
        resp = requests.get(
            f"{STUDENT_SERVICE_URL}/api/student/admin/course-offerings",
            timeout=10,
        )
    except requests.RequestException as exc:
        return jsonify({"error": "Student service unavailable", "detail": str(exc)}), 502

    try:
        payload = resp.json()
    except ValueError:
        return jsonify({"error": "Student service returned non-JSON response"}), 502

    return jsonify(payload if isinstance(payload, list) else payload.get("offerings", [])), resp.status_code


def _resolve_session_usertype():
    """Resolve usertype from session only."""
    session_usertype = str(session.get('usertype') or '').strip().lower()
    if session_usertype:
        return session_usertype

    if str(session.get('role') or '').strip().lower() == 'admin':
        session['usertype'] = 'admin'
        return 'admin'

    return ''


def _is_admin_user(allow_db_fallback=False):
    """Fast admin guard with optional one-time DB fallback on cold cache.
    Uses DB fallback only when needed."""
    if 'user' not in session:
        return False

    # Trust explicit admin role already present in session.
    session_role = str(session.get('usertype') or session.get('role') or '').strip().lower()
    if session_role in ('admin', 'administrator'):
        session['role'] = 'admin'
        return True

    user_id = session.get('user_id')
    if user_id:
        usertype = _resolve_session_usertype()
        if usertype in ('admin', 'administrator'):
            session['role'] = 'admin'
            return True

    if not allow_db_fallback:
        return False

    try:
        conn = get_db_connection()
        cursor = conn.cursor(DICT_CURSOR)
        cursor.execute(
            "SELECT usertype FROM nrm_users WHERE email = %s",
            (session['user'],)
        )
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        resolved_usertype = str((user or {}).get('USERTYPE') or (user or {}).get('usertype') or '').strip().lower()
        if resolved_usertype in ('admin', 'administrator'):
            session['usertype'] = resolved_usertype
            session['role'] = 'admin'
            return True
    except Exception as e:
        print(f"⚠️ Admin role DB fallback failed: {e}")

    return False


@app.route('/admin/upload', methods=['GET'])
def upload_page():
    start = time.time()
    print("Session:", session)

    if not _has_employee_admin_access():
        return "<h3>Access denied</h3>", 403
    print("Auth OK:", time.time() - start)

    return render_template(
        'upload.html',
        username=session.get('user'),
        usertype=session.get('usertype', 'admin')
    )

# @app.route('/admin/upload')
# def upload_page():
#     return "UPLOAD OK"

# ================== Upload Handler ================== #
#@app.route('/upload_file/<file_type>', methods=['GET', 'POST'])

@app.route('/admin/testpost', methods=['POST'])
def testpost():
    return "POST OK"

@app.route('/admin/upload_file/<file_type>', methods=['GET', 'POST'])
def upload_file_handler(file_type):
    try:
        if request.method == 'GET':
            return redirect(url_for('upload_page'))

        if not _has_employee_admin_access():
            flash("Access denied.", "error")
            return redirect(url_for('upload_page'))

        if 'file' not in request.files:
            flash("No file selected.")
            return redirect(url_for('upload_page'))

        file = request.files['file']
        if file.filename == '':
            flash("No file selected.")
            return redirect(url_for('upload_page'))

        data = {}
        if file_type == 'syllabus':
            data['course_id'] = (request.form.get('course_id') or '').strip()
            if not data['course_id']:
                flash("❌ Please select a course for syllabus upload.")
                return redirect(url_for('upload_page'))
        else:
            data['category'] = (request.form.get('category') or '').strip()
            if not data['category']:
                flash("❌ Please select a technology/category.")
                return redirect(url_for('upload_page'))

        files = {
            'file': (file.filename, file.stream, file.mimetype or 'application/octet-stream')
        }
        target_url = f"{STUDENT_SERVICE_URL}/api/student/admin/upload-file/{file_type}"
        resp = requests.post(target_url, data=data, files=files, timeout=90)

        payload = resp.json() if resp.headers.get('Content-Type', '').startswith('application/json') else {}
        if resp.status_code == 200 and payload.get('success'):
            flash(f"✅ {payload.get('message', f'{file_type.upper()} uploaded successfully!')}")
        else:
            message = payload.get('detail') or payload.get('message') or 'Upload failed. Please try again.'
            flash(f"❌ {message}")

        return redirect(url_for('upload_page'))

    except requests.RequestException as e:
        print(f"❌ Upload proxy error: {e}")
        flash("❌ Upload service unavailable. Please try again.")
        return redirect(url_for('upload_page'))
    except Exception as e:
        print(f"❌ Upload error: {e}")
        flash("❌ Upload failed. Please try again.")
        return redirect(url_for('upload_page'))



ORG_COMPLIANCE_BUCKET  = "org-complaince-docs"
ORG_COMPLIANCE_REGION  = "eu-north-1"
ORG_COMPLIANCE_BASE_URL = f"https://{ORG_COMPLIANCE_BUCKET}.s3.{ORG_COMPLIANCE_REGION}.amazonaws.com"

ORG_FOLDER_MAP = {
    "hr":         "hr-policy",
    "legal":      "legal-compliance",
    "finance":    "finance-accounts",
    "operations": "internal-operations",
}

ORG_ALLOWED_EXTENSIONS = {
    'pdf', 'docx', 'doc', 'xlsx', 'xls', 'csv',
    'pptx', 'ppt', 'txt', 'png', 'jpg', 'jpeg'
}


def _upload_org_doc_impl(doc_type='general'):
    """Proxy org doc upload to student-service → s3://org-complaince-docs"""
    try:
        # ── Auth check ────────────────────────────────────────────────
        if not _is_admin_user():
            flash("❌ Access denied. Admins only.", "error")
            return redirect(url_for('upload_page'))

        # ── File presence check ───────────────────────────────────────
        if 'file' not in request.files:
            flash("❌ No file selected.", "warning")
            return redirect(url_for('upload_page'))

        file = request.files['file']
        if not file or file.filename == '':
            flash("❌ No file selected.", "warning")
            return redirect(url_for('upload_page'))

        if doc_type not in ORG_FOLDER_MAP:
            flash(f"❌ Invalid document type: '{doc_type}'.", "error")
            return redirect(url_for('upload_page'))

        # ── Proxy to student-service ──────────────────────────────────
        category    = (request.form.get('org_doc_category') or 'General').strip()
        uploaded_by = (request.form.get('uploaded_by') or session.get('username') or 'admin').strip()

        student_url = f"{STUDENT_SERVICE_URL}/api/admin/upload-org-doc/{doc_type}"

        resp = requests.post(
            student_url,
            files={"file": (file.filename, file.stream, file.content_type or 'application/octet-stream')},
            data={"org_doc_category": category, "uploaded_by": uploaded_by},
            timeout=60,
        )

        if resp.status_code == 200:
            data = resp.json()
            flash(f"✅ '{file.filename}' uploaded successfully under {category}.", "success")
        else:
            try:
                msg = resp.json().get("detail", resp.text)
            except Exception:
                msg = resp.text
            flash(f"❌ Upload failed: {msg}", "error")

        return redirect(url_for('upload_page'))

    except Exception as e:
        print(f"❌ Org doc proxy error: {e}")
        traceback.print_exc()
        flash(f"❌ Upload failed: {str(e)}", "error")
        return redirect(url_for('upload_page'))


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/upload/org/<doc_type>', methods=['POST'])
def upload_org_doc(doc_type):
    return _upload_org_doc_impl(doc_type=doc_type)


@app.route('/admin/upload-org-doc', methods=['POST'])
def upload_org_doc_handler():
    legacy_doc_type = request.form.get('folder') or request.form.get('doc_type') or 'general'
    return _upload_org_doc_impl(doc_type=legacy_doc_type)


# ==========================================
# DELETE ORG DOCUMENT FROM S3
# ==========================================

@app.route('/admin/delete-org-doc', methods=['POST'])
def delete_org_doc():
    try:
        # ── Auth check ────────────────────────────────────────────────
        if not _is_admin_user():
            flash("❌ Access denied. Admins only.", "error")
            return redirect(url_for('upload_page'))

        s3_key = (request.form.get('s3_key') or '').strip()
        if not s3_key:
            flash("❌ Missing document key — cannot delete.", "error")
            return redirect(url_for('upload_page'))

        # ── Build S3 client ───────────────────────────────────────────
        access_key = (os.getenv('AWS_ACCESS_KEY') or os.getenv('AWS_ACCESS_KEY_ID') or '').strip()
        secret_key = (os.getenv('AWS_SECRET_KEY') or os.getenv('AWS_SECRET_ACCESS_KEY') or '').strip()

        if access_key and secret_key:
            s3 = boto3.client(
                's3',
                region_name=ORG_COMPLIANCE_REGION,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
        else:
            s3 = boto3.client('s3', region_name=ORG_COMPLIANCE_REGION)

        # ── Delete from S3 ────────────────────────────────────────────
        s3.delete_object(Bucket=ORG_COMPLIANCE_BUCKET, Key=s3_key)

        print(f"🗑️ Org doc deleted → s3://{ORG_COMPLIANCE_BUCKET}/{s3_key}")
        flash("✅ Organisation document deleted successfully.", "success")
        return redirect(url_for('upload_page'))

    except Exception as e:
        print(f"❌ Org doc delete error: {e}")
        traceback.print_exc()
        flash(f"❌ Failed to delete document: {str(e)}", "error")
        return redirect(url_for('upload_page'))

# ================== Delete File ================== #
@app.route('/delete_file', methods=['POST'])
def delete_file():
    if not _is_admin_user():
        return "<h3>Access denied: Admins only</h3>"

    rel_path = request.form.get('file_path')
    if not rel_path:
        flash("No file path provided.", "warning")
        return redirect(url_for('upload_page'))

    parts = rel_path.split('/')
    file_type = parts[0]
    category = parts[1] if len(parts) > 1 else None

    # ✅ Resolve correct base folder
    if file_type in ["practice_test", "practice_tests"]:
        base_folder = app.config['UPLOAD_FOLDERS']['practice_tests']
    else:
        base_folder = os.path.join(app.config['UPLOAD_FOLDER'], file_type)

    abs_path = os.path.join(base_folder, *parts[1:])

    try:
        if os.path.exists(abs_path):
            os.remove(abs_path)

            if file_type == 'syllabus':
                public_path = '/' + '/'.join(['uploads'] + parts)
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute("DELETE FROM nrm_syllabus WHERE file_path = %s", (public_path,))
                conn.commit()
                cursor.close()
                conn.close()

            flash(f"✅ {rel_path} deleted successfully.", "success")
        else:
            flash("❌ File not found.", "danger")
    except Exception as e:
        flash(f"❌ Error deleting file: {e}", "danger")

    # ✅ Redirect back
    if file_type in ['ppt', 'code', 'interview_questions'] and category:
        if file_type == 'ppt':
            return redirect(url_for('view_category_files', tech=category, file_type='ppt'))
        elif file_type == 'code':
            return redirect(url_for('view_category_files', tech=category, file_type='code'))
        elif file_type == 'interview_questions':
            return redirect(url_for('view_category_files', tech=category, file_type='interview'))
    # practice_test or anything else → just stay in upload page
    return redirect(url_for('upload_page'))

@app.route('/uploads/<path:subpath>/<filename>')
def serve_uploaded_file(subpath, filename):
    base_dir = app.config['UPLOAD_FOLDER']
    directory = os.path.join(base_dir, subpath)
    return send_from_directory(directory, filename)
# ================== Category View ================== #
@app.route('/view/<tech>/<file_type>')
def view_category_files(tech, file_type):
    usertype = session.get('usertype', 'public')

    if usertype.lower() not in ['admin', 'administrator'] and not user_has_completed_resources_registration(session.get('user_id')):
        flash("Please complete ChakoraHub Register to access code, PPTs, videos, and interview questions.", "error")
        return redirect(url_for('register'))

    # ✅ Mapping file types to subfolders
    file_type_map = {
        'ppt': 'ppt',
        'code': 'code',
        'interview': 'interview_questions'
    }

    if file_type not in file_type_map:
        return "<h3>Invalid file type</h3>"

    base_folder = file_type_map[file_type]

    # Decode tech name from URL
    decoded_tech = urllib.parse.unquote(tech)

    rel_path = os.path.join(app.config['UPLOAD_FOLDER'], base_folder, decoded_tech)
    full_path = os.path.abspath(rel_path)

    # Prevent directory traversal
    upload_root = os.path.abspath(app.config['UPLOAD_FOLDER'])
    if not full_path.startswith(upload_root):
        return "<h3>Invalid path</h3>"

    # List files
    files = sorted(os.listdir(full_path)) if os.path.exists(full_path) else []

    return render_template(
        'category-view.html',
        tech=decoded_tech,
        file_type=file_type,
        files=files,
        usertype=usertype,
        file_type_map=file_type_map
    )

#meeting module
nrm_meeting_queue = []
meeting_start_time = None
meeting_end_time = None
queue_lock = Lock()

# Configuration
TOTAL_MEETING_TIME = timedelta(minutes=60)  # Total meeting duration
USER_TIME_LIMIT = timedelta(minutes=10)     # Max time per user

@app.route('/meeting')
def meeting_home():
   """Render the meeting join page."""
   return render_template('meeting.html')


@app.route("/meeting/identify", methods=["POST"])
def meeting_identify():
    try:
        data = request.get_json(silent=True) or {}
        identity = str(data.get("identity", "")).strip()
        if not identity:
            return jsonify({"success": False, "message": "Email or phone number is required"}), 400

        identity_type = None
        if validate_email(identity):
            identity_type = "email"
        elif validate_e164_phone(identity):
            identity_type = "phone"
        else:
            return jsonify({"success": False, "message": "Invalid email or E.164 phone number"}), 400

        # Direct lookup against meeting_service avoids stale cache-derived counts.
        lookup_response = requests.post(
            f"{MEETING_SERVICE_URL}/meeting/identify",
            json={"identity": identity},
            timeout=12,
        )
        lookup_payload = lookup_response.json() if lookup_response.headers.get("Content-Type", "").startswith("application/json") else {}
        if lookup_response.status_code != 200:
            return jsonify({
                "success": False,
                "message": lookup_payload.get("message") or lookup_payload.get("detail") or "Student lookup failed"
            }), lookup_response.status_code

        existing_student = bool(lookup_payload.get("exists", False))
        total_bookings = int(lookup_payload.get("total_bookings", 0) or 0)
        cache_hit = False
        student_data = {
            "exists": existing_student,
            "total_bookings": total_bookings,
            "student_email": identity if identity_type == "email" else "",
            "student_phone": identity if identity_type == "phone" else "",
            "student_name": "",
        }

        suggestions = None
        if existing_student:
        # ── KAFKA: publish meeting.suggestion.requested ──────────────
            suggestion_correlation_id = str(uuid.uuid4())
            kafka_publish("meeting.suggestion.requested", {
                "correlation_id":  suggestion_correlation_id,
                "student_email":   student_data.get("student_email"),
                "student_phone":   student_data.get("student_phone"),
                "student_name":    student_data.get("student_name"),
                "subject":         data.get("subject"),
                "booking_reason":  data.get("booking_reason"),
                "requested_duration": data.get("requested_duration"),
                "timestamp":       datetime.utcnow().isoformat()
            })

            suggestion_email = (
                (student_data.get("student_email") or "").strip().lower()
                if identity_type == "email"
                else ""
            ) or (identity.lower().strip() if identity_type == "email" else "")

            suggestion_response = requests.post(
                f"{MEETING_SERVICE_URL}/meeting/agentic-suggestions",
                json={
                    "email": suggestion_email,
                },
                timeout=20
            )
            if suggestion_response.status_code == 200:
                suggestions = suggestion_response.json().get("suggestions")

        return jsonify({
            "success": True,
            "exists": existing_student,
            "total_bookings": total_bookings,
            "cache_hit": cache_hit,
            "student": student_data if existing_student else {
                "identity": identity,
                "identity_type": identity_type
            },
            "suggestions": suggestions,

            # Backward compatibility for older clients
            "existing_student": existing_student,
        }), 200
    except Exception as e:
        print(f"❌ meeting_identify error: {e}")
        return jsonify({
            "success": False,
            "message": "Unable to process request"
        }), 500

@app.route("/meeting/slots", methods=["GET"])
def meeting_slots():
    try:
        response = requests.get(
            f"{MEETING_SERVICE_URL}/meeting/slots",
            params=request.args,
            timeout=15
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        print(f"❌ meeting_slots error: {e}")
        return jsonify({
            "success": False,
            "message": "Unable to fetch slots"
        }), 500

@app.route("/meeting/pricing", methods=["GET", "POST"])
def meeting_pricing():
    try:
        payload = request.get_json(silent=True) or {}
        payload.update(request.args.to_dict(flat=True))
        response = requests.get(
            f"{MEETING_SERVICE_URL}/meeting-price-preview",
            params={
                "date": payload.get("date", ""),
                "start_time": payload.get("start_time", ""),
                "duration_minutes": payload.get("duration_minutes", ""),
                "complexity": payload.get("complexity", "Medium"),
                "booking_type": payload.get("booking_type", "external"),
                "email": payload.get("email", ""),
            },
            timeout=20
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        print(f"❌ meeting_pricing error: {e}")
        return jsonify({
            "success": False,
            "message": "Pricing engine failed"
        }), 500


@app.route("/meeting/book", methods=["POST"])
def meeting_book():
    try:
        payload = request.get_json(silent=True) or {}
        response = requests.post(
            f"{MEETING_SERVICE_URL}/meeting/book",
            json=payload,
            timeout=30
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        print(f"❌ meeting_book error: {e}")
        return jsonify({
            "success": False,
            "message": "Booking failed"
        }), 500


@app.route("/meeting/update-purpose", methods=["POST"])
def meeting_update_purpose():
    try:
        payload = request.get_json(silent=True) or {}
        response = requests.post(
            f"{MEETING_SERVICE_URL}/meeting/update-purpose",
            json=payload,
            timeout=20,
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        print(f"❌ meeting_update_purpose error: {e}")
        return jsonify({
            "success": False,
            "message": "Could not update meeting purpose"
        }), 500


@app.route("/meeting/payment/create-order", methods=["POST"])
def create_meeting_payment_order():
    try:
        payload = request.get_json(silent=True) or {}
        response = requests.post(
            f"{BILLING_SERVICE_URL}/payment/create-order",
            json=payload,
            timeout=20
        )
        data = response.json()

        # ── KAFKA: publish payment.created (Flask side) ──────────
        if response.status_code == 200 and data.get("success"):
            kafka_publish("payment.created", {
                "correlation_id": str(uuid.uuid4()),
                "order_id":       data.get("order_id"),
                "amount":         data.get("amount"),
                "currency":       data.get("currency", "INR"),
                "source":         "flask_proxy",
                "timestamp":      datetime.utcnow().isoformat(),
            })

        return jsonify(response.json()), response.status_code
    except requests.exceptions.ConnectionError as e:
        print(f"❌ payment order connection error: {e} | BILLING_SERVICE_URL={BILLING_SERVICE_URL}")
        return jsonify({
            "success": False,
            "message": f"Unable to connect to billing service at {BILLING_SERVICE_URL}"
        }), 502
    except Exception as e:
        print(f"❌ payment order error: {e}")
        return jsonify({
            "success": False,
            "message": "Payment order creation failed"
        }), 500

@app.route("/meeting/payment/verify", methods=["POST"])
def verify_meeting_payment():
    try:
        payload = request.get_json(silent=True) or {}
        response = requests.post(
            f"{BILLING_SERVICE_URL}/payment/verify",
            json=payload,
            timeout=20
        )
        if response.status_code != 200:
            return jsonify(response.json()), response.status_code
        verification_data = response.json()

        # ── KAFKA: publish payment.completed ─────────────────────
        kafka_publish("payment.completed", {
            "correlation_id": str(uuid.uuid4()),
            "order_id":       payload.get("order_id"),
            "payment_id":     payload.get("payment_id"),
            "status":         "captured",
            "source":         "flask_proxy",
            "timestamp":      datetime.utcnow().isoformat(),
        })

        booking_id = verification_data.get("booking_id")
        teams_response = requests.post(
            f"{MS365_SERVICE_URL}/teams/create-meeting",
            json={
                "booking_id": booking_id
            },
            timeout=30
        )
        teams_data = {}
        if teams_response.status_code == 200:
            teams_data = teams_response.json()
        return jsonify({
            "success": True,
            "payment": verification_data,
            "teams": teams_data
        }), 200
    except Exception as e:
        print(f"❌ payment verification error: {e}")
        return jsonify({
            "success": False,
            "message": "Payment verification failed"
        }), 500

@app.route("/meeting/api/hold", methods=["POST"])
def proxy_meeting_hold():
    try:
        payload = request.get_json(silent=True) or {}
        response = requests.post(
            f"{MEETING_SERVICE_URL}/meeting-hold",
            json=payload,
            headers={"Authorization": request.headers.get("Authorization", "")},
            timeout=10,
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        print(f"❌ proxy_meeting_hold error: {e}")
        return jsonify({"success": False, "detail": "Hold service unavailable"}), 502

@app.route("/meeting/student-progress", methods=["GET"])
def meeting_student_progress():
    try:
        student_email = request.args.get("student_email", "").strip()
        response = requests.get(
            f"{RAG_SERVICE_URL}/student-progress",
            params={
                "student_email": student_email
            },
            timeout=45
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        print(f"❌ student progress error: {e}")
        return jsonify({
            "success": False,
            "message": "Unable to fetch student progress"
        }), 500

# SETTINGS PAGE
@app.route('/settings')
def settings():
    message = session.pop('message', None)
    return render_template('settings.html', message=message)

# UPLOAD PROFILE PHOTO
@app.route('/upload_photo', methods=['POST'])
def upload_photo():
    if 'user' not in session:
        flash("You must be logged in to upload a photo.")
        return redirect(url_for('home'))

    username = session['user']
    file = request.files.get('photo')

    if not file or file.filename == '':
        flash("No file selected.")
        return redirect(url_for('settings'))

    conn = get_db_connection()
    cursor = conn.cursor(DICT_CURSOR)

    cursor.execute("SELECT profile_pic FROM nrm_logins WHERE username = %s", (username,))
    user = cursor.fetchone()

    if not user:
        cursor.close()
        conn.close()
        flash("User not found.")
        return redirect(url_for('settings'))

    if allowed_file(file.filename):
        filename = secure_filename(file.filename)
        upload_folder = os.path.join('static', 'profile_photo')
        os.makedirs(upload_folder, exist_ok=True)
        save_path = os.path.join(upload_folder, filename)
        file.save(save_path)

        new_pic_path = f'profile_photo/{filename}'

        old_pic = user['profile_pic']
        if old_pic and old_pic != 'profile_photo/defaultpicture.jpg':
            old_path = os.path.join('static', old_pic)
            if os.path.exists(old_path):
                try:
                    os.remove(old_path)
                except Exception as e:
                    print("Error deleting old photo:", e)

        cursor.execute("UPDATE nrm_logins SET profile_pic = %s WHERE username = %s", (new_pic_path, username))
        conn.commit()

        session['profile_pic'] = new_pic_path

        cursor.close()
        conn.close()

        flash("✅ Profile photo uploaded successfully!")
        return redirect(url_for('settings'))
    else:
        flash("❌ Invalid file type. Only png, jpg, jpeg, gif allowed.")
        return redirect(url_for('settings'))

# REMOVE PROFILE PHOTO
@app.route('/remove_photo', methods=['POST'])
def remove_photo():
    if 'user' not in session:
        flash("Session expired. Please log in again.")
        return redirect(url_for('home'))

    username = session['user']
    current_pic = session.get('profile_pic')
    default_pic = 'profile_photo/defaultpicture.jpg'

    # Delete old photo from static folder if not default
    if current_pic and current_pic != default_pic:
        try:
            full_path = os.path.join('static', current_pic)
            if os.path.exists(full_path):
                os.remove(full_path)
        except Exception as e:
            print("Error deleting profile picture:", e)

    # Update DB to set default profile picture
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE nrm_logins SET profile_pic = %s WHERE username = %s", (default_pic, username))
    conn.commit()
    cursor.close()
    conn.close()

    session['profile_pic'] = default_pic
    flash("Profile photo removed.")
    return redirect(url_for('settings'))

@app.route('/change_name', methods=['POST'])
def change_name():
    if 'user' not in session or 'usertype' not in session:
        flash("You must be logged in.")
        return redirect(url_for('user_nrm_logins'))

    new_name = request.form.get('new_name', '').strip()
    current_user = session['user']
    usertype = session['usertype']

    if not new_name:
        flash("New name cannot be empty!")
        return redirect(url_for('settings'))

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if name already exists
        if usertype in ['admin', 'student']:
            cursor.execute("SELECT username FROM nrm_logins WHERE username = %s", (new_name,))
        elif usertype == 'public':
            cursor.execute("SELECT username FROM nrm_users WHERE username = %s", (new_name,))
        else:
            flash("Unrecognized user type!")
            cursor.close()
            conn.close()
            return redirect(url_for('settings'))

        if cursor.fetchone():
            flash("Username already taken.")
            cursor.close()
            conn.close()
            return redirect(url_for('settings'))

        # Update name
        if usertype in ['admin', 'student']:
            cursor.execute("UPDATE nrm_logins SET username = %s WHERE email = %s", (new_name, current_user))
        elif usertype == 'public':
            cursor.execute("UPDATE nrm_users SET username = %s WHERE email = %s", (new_name, current_user))

        conn.commit()
        cursor.close()
        conn.close()

        # Update session
        session['user'] = new_name
        session.modified = True
        flash("Name updated successfully!")
    except Exception as e:
        flash(f"An error occurred: {str(e)}")

    return redirect(url_for('settings'))

@app.route('/profile')
def profile():
    """Student profile page"""
    user_id = session.get('user_id')

    if not user_id:
        flash("Please login first.")
        return redirect(url_for('home'))

    # Always provide complete template context so profile page never crashes
    # when downstream service calls fail.
    student = {
        "ID": user_id,
        "USERNAME": (session.get('user') or "").strip(),
        "EMAIL": (session.get('email') or "").strip(),
        "PHONE": (session.get('phone') or "").strip(),
        "USERTYPE": ((session.get('usertype') or "student").strip().lower() or "student"),
        "PROFILE_PIC": (session.get('profile_pic') or "profile_photo/defaultpicture.jpg"),
        "GOTHRAM": "",
        "REGISTRATION_SOURCE": "",
        "CREATED_AT": None,
        "UPDATED_AT": None,
    }
    login = {
        "IS_ACTIVE": "Y",
        "LAST_LOGIN": None,
        "FAILED_LOGIN_ATTEMPTS": 0,
    }
    stu_rec = {
        "ID": None,
        "USER_ID": user_id,
        "FIRST_NAME": "",
        "LAST_NAME": "",
        "GOTHRAM": "",
        "EMPLOYED": "No",
        "EXPERIENCE": "",
        "LOCATION": "",
        "REGISTRATION_SOURCE": "",
        "ADDRESS": "",
        "SKILL_SET": "",
        "RESUME_FILE_PATH": "",
    }

    registration = {}
    registrations = []
    internships = []
    intern_docs = []
    enquiries = []
    feedbacks = []

    try:
        # Fetch base identity from resources endpoint (contains user block).
        resources_resp = requests.post(
            f"{STUDENT_SERVICE_URL}/api/student/resources",
            json={"user_id": user_id},
            timeout=10
        )
        if resources_resp.status_code == 200:
            resources_data = resources_resp.json() or {}
            user_payload = resources_data.get("user") or {}
            student.update({
                "USERNAME": user_payload.get("username") or student["USERNAME"],
                "EMAIL": user_payload.get("email") or student["EMAIL"],
                "PHONE": user_payload.get("phone") or student["PHONE"],
                "USERTYPE": (user_payload.get("usertype") or student["USERTYPE"]),
                "PROFILE_PIC": user_payload.get("profile_pic") or student["PROFILE_PIC"],
            })

        response = requests.post(
            f"{STUDENT_SERVICE_URL}/api/student/profile",
            json={"user_id": user_id},
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            address = data.get("address", "")
            stu_rec["ADDRESS"] = address

    except requests.RequestException as e:
        print(f"❌ Profile service error: {e}")

    return render_template(
        'profile.html',
        student=student,
        login=login,
        stu_rec=stu_rec,
        registration=registration,
        registrations=registrations,
        internships=internships,
        intern_docs=intern_docs,
        enquiries=enquiries,
        feedbacks=feedbacks,
        profile_pic=student.get("PROFILE_PIC") or "profile_photo/defaultpicture.jpg",
        address=stu_rec.get("ADDRESS") or "",
    )

@app.route('/save_address', methods=['POST'])
def save_address():
    """Save student address"""
    user_id = session.get('user_id')

    if not user_id:
        flash("Please login first.")
        return redirect(url_for('home'))

    # profile.html sends STU_ADDRESS; keep backward compatibility with address.
    address = (request.form.get('STU_ADDRESS') or request.form.get('address') or '').strip()

    try:
        response = requests.put(
            f"{STUDENT_SERVICE_URL}/api/student/profile",
            json={"user_id": user_id, "address": address},
            timeout=10
        )

        if response.status_code == 200:
            session['address'] = address
            flash("Address saved successfully!")
        else:
            flash("Failed to save address")

    except requests.RequestException as e:
        print(f"❌ Save address error: {e}")
        flash("Service unavailable")

    return redirect(url_for('profile'))


@app.route('/update_student_profile', methods=['POST'])
def update_student_profile():
    """Backward-compatible endpoint used by profile.html form action."""
    return save_address()

# ✅ Helper to fetch all nrm_festivals from DB as dictionary
def get_nrm_festivals():
    conn = get_db_connection()
    cursor = conn.cursor(DICT_CURSOR)
    cursor.execute("SELECT festival_date, festival_name FROM nrm_festivals")
    rows = cursor.fetchall()
    conn.close()
    # Convert to { 'YYYY-MM-DD': 'Festival Name', ... }
    return {row['festival_date'].strftime('%Y-%m-%d'): row['festival_name'] for row in rows}

# ================== 📅 Calendar Route ==================
@app.route('/calendar')
def calendar_page():
    # AJAX pattern: render shell only; data is fetched client-side via /api/calendar/data.
    try:
        month = int(request.args.get('month', datetime.now().month))
        year = int(request.args.get('year', datetime.now().year))
    except ValueError:
        month = datetime.now().month
        year = datetime.now().year

    return render_template(
        'calendar.html',
        month=month,
        year=year,
        session_role=session.get('usertype', ''),
        username=session.get('user', '').split('@')[0]
    )


@app.route('/api/calendar/data', methods=['GET'])
@app.route('/calendar/data', methods=['GET'])
def api_calendar_data():
    """JSON endpoint for /calendar (AJAX-loaded).
    TODO: migrate this DB work into a dedicated calendar/meeting microservice.
    """
    try:
        month = int(request.args.get('month', datetime.now().month))
        year = int(request.args.get('year', datetime.now().year))
    except ValueError:
        month = datetime.now().month
        year = datetime.now().year

    print(f"[calendar_data] request month={month} year={year} path={request.path}")

    conn = get_db_connection()
    if not conn:
        return jsonify({
            "success": False,
            "message": "Database unavailable",
            "month": month,
            "year": year,
            "calendar_data": {},
        }), 503

    cursor = conn.cursor(DICT_CURSOR)
    try:
        month_start = datetime(year, month, 1)
        next_month_start = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)

        # Step 1: Get festivals
        cursor.execute("""
            SELECT festival_date, festival_name
            FROM nrm_festivals
            WHERE festival_date >= %s AND festival_date < %s
        """, (month_start, next_month_start))
        festival_rows = cursor.fetchall()
        print(f"[calendar_data] festivals rows={len(festival_rows)}")

        month_nrm_festivals = {
            row['FESTIVAL_DATE'].strftime('%Y-%m-%d'): row['FESTIVAL_NAME']
            for row in festival_rows
        }

        # Step 2: Get all slots
        cursor.execute("SELECT id, slot_label FROM nrm_time_slots ORDER BY id")
        all_slots_rows = cursor.fetchall()
        all_slots = [row['SLOT_LABEL'] for row in all_slots_rows]
        print(f"[calendar_data] slots rows={len(all_slots_rows)}")

        # Step 3: Get bookings (includes meeting_link)
        cursor.execute("""
            SELECT
                s.session_date,
                t.slot_label,
                s.meeting_link
            FROM nrm_session_bookings s
            JOIN nrm_time_slots t ON s.time_slot_id = t.id
            WHERE s.session_date >= %s AND s.session_date < %s
            ORDER BY s.session_date, t.slot_label
        """, (month_start, next_month_start))
        booking_rows = cursor.fetchall()
        print(f"[calendar_data] bookings rows={len(booking_rows)}")

        # Step 4: Organize bookings + meeting links
        booked_slots_dict = {}
        meeting_links_dict = {}
        for b in booking_rows:
            key = b['SESSION_DATE'].strftime('%Y-%m-%d')
            booked_slots_dict.setdefault(key, set()).add(b['SLOT_LABEL'])
            if b.get('MEETING_LINK'):
                meeting_links_dict[f"{key}_{b['SLOT_LABEL']}"] = b['MEETING_LINK']
    finally:
        cursor.close()
        conn.close()

    teams_by_date = {}
    try:
        organizer_email = "support@chakorahub.com"
        start_iso = month_start.strftime("%Y-%m-%dT00:00:00Z")
        end_iso = next_month_start.strftime("%Y-%m-%dT00:00:00Z")
        print(f"[calendar_data] teams_fetch email={organizer_email} start={start_iso} end={end_iso}")
        teams_resp = requests.get(
            f"{MS365_SERVICE_URL}/teams/calendar/{organizer_email}",
            params={"start": start_iso, "end": end_iso},
            timeout=25,
        )
        print(f"[calendar_data] teams_fetch status={teams_resp.status_code}")
        teams_payload = teams_resp.json() if teams_resp.headers.get("Content-Type", "").startswith("application/json") else {}
        if teams_resp.ok:
            teams_meetings = teams_payload.get("meetings", [])
            print(f"[calendar_data] teams_fetch meetings={len(teams_meetings)}")
            for meeting in teams_payload.get("meetings", []):
                start_raw = (meeting or {}).get("start")
                if not start_raw:
                    continue
                try:
                    start_dt = datetime.fromisoformat(str(start_raw).replace("Z", "+00:00"))
                    date_key = start_dt.date().strftime("%Y-%m-%d")
                except Exception:
                    continue

                teams_by_date.setdefault(date_key, []).append({
                    "subject": (meeting or {}).get("subject") or "Teams Meeting",
                    "start": (meeting or {}).get("start") or "",
                    "end": (meeting or {}).get("end") or "",
                    "start_timezone": (meeting or {}).get("start_timezone") or "",
                    "end_timezone": (meeting or {}).get("end_timezone") or "",
                    "organizer": (meeting or {}).get("organizer") or "support@chakorahub.com",
                    "joinUrl": (meeting or {}).get("joinUrl") or (meeting or {}).get("webLink") or "",
                })
            print(f"[calendar_data] teams_merge days={len(teams_by_date)}")
        else:
            print(f"Teams calendar fetch failed for /api/calendar/data: status={teams_resp.status_code}")
    except Exception as teams_err:
        print(f"Teams calendar merge skipped for /api/calendar/data: {teams_err}")

    # Step 5: Build calendar data
    days_in_month = (datetime(year, month % 12 + 1, 1) - timedelta(days=1)).day
    calendar_data = {}

    for day in range(1, days_in_month + 1):
        date_str = f"{year}-{month:02}-{day:02}"
        festival = month_nrm_festivals.get(date_str)
        bookings_status = []

        for slot in all_slots:
            status = (
                "Booked"
                if slot in booked_slots_dict.get(date_str, set())
                else "Not booked"
            )
            bookings_status.append({
                "slot": slot,
                "status": status,
                "meeting_link": meeting_links_dict.get(f"{date_str}_{slot}", "")
            })

        calendar_data[date_str] = {
            "festival": festival,
            "bookings": bookings_status,
            "teams": teams_by_date.get(date_str, [])
        }

    result = {
        "success": True,
        "month": month,
        "year": year,
        "calendar_data": calendar_data,
    }
    total_team_events = sum(len(v) for v in teams_by_date.values())
    print(
        f"[calendar_data] response festival_days={len(month_nrm_festivals)} "
        f"booking_days={len(booked_slots_dict)} teams_days={len(teams_by_date)} teams_events={total_team_events}"
    )
    return jsonify(result)

# ================== 🎉 Festival Greeting Route ==================
@app.route('/nrm_festivals')
def nrm_festivals():
    if 'user' not in session:
        return redirect(url_for('nrm_logins'))

    # ✅ Get today's festival from DB
    all_nrm_festivals = get_nrm_festivals()
    today = datetime.today().strftime('%Y-%m-%d')
    festival_today = all_nrm_festivals.get(today)

    greeting = f"Happy {festival_today}" if festival_today else None

    return render_template(
        'resources.html',
        user=session['user'],
        useremail=session.get('email'),
        usertype=session.get('usertype', ''),
        username=session['user'],
        festival_today=festival_today,
        greeting=greeting
    )
        

# ---------- ROUTES ----------
# ✅ Global Upload Folder for Practice Tests
@app.route('/upload_practice_test', methods=['POST'])
def upload_practice_test():
    if not _is_admin_user(allow_db_fallback=True):
        flash("Access denied.", "error")
        return redirect(url_for('upload_page'))

    if 'file' not in request.files:
        flash("No file selected.")
        return redirect(url_for('upload_page'))

    file = request.files['file']
    subject = (request.form.get('subject') or '').strip()

    if file.filename == '':
        flash("No file selected.")
        return redirect(url_for('upload_page'))
    if not subject:
        flash("❌ Please select a subject.")
        return redirect(url_for('upload_page'))

    files = {
        'file': (file.filename, file.stream, file.mimetype or 'application/octet-stream')
    }
    data = {'subject': subject}

    try:
        resp = requests.post(
            f"{STUDENT_SERVICE_URL}/api/student/admin/upload-practice-test",
            data=data,
            files=files,
            timeout=90,
        )
        payload = resp.json() if resp.headers.get('Content-Type', '').startswith('application/json') else {}
        if resp.status_code == 200 and payload.get('success'):
            flash(f"✅ {payload.get('message', 'Practice test uploaded successfully!')}")
        else:
            message = payload.get('detail') or payload.get('message') or 'Failed to upload practice test.'
            flash(f"❌ {message}")
    except requests.RequestException as e:
        print(f"❌ upload_practice_test proxy error: {e}")
        flash('❌ Upload service unavailable. Please try again.')

    return redirect(url_for('upload_page'))

@app.route('/practice-test/<subject>')
def practice_test(subject):
    subject_folder = os.path.join(app.config['UPLOAD_FOLDERS']['practice_tests'], subject)
    files = os.listdir(subject_folder) if os.path.exists(subject_folder) else []

    file_urls = [
        {
            'name': f,
            'url': url_for('serve_practice_test', subject=subject, filename=f)
        } for f in files
    ]

    role = session.get('usertype', 'user')
    return render_template('practice-test.html', subject=subject, file_urls=file_urls, usertype=role)

@app.route('/uploads/practice-tests/<subject>/<filename>')
def serve_practice_test(subject, filename):
    return send_from_directory(
        os.path.join(app.config['UPLOAD_FOLDERS']['practice_tests'], subject),
        filename
    )

#certificate
print("\n" + "="*50)
print("AWS CREDENTIALS DEBUG INFO  [generate-certificate]")
print("="*50)
print(f"AWS_ACCESS_KEY set : {'Yes' if AWS_ACCESS_KEY and AWS_ACCESS_KEY != 'YOUR_ACCESS_KEY_HERE' else 'No'}")
print(f"AWS_SECRET_KEY set : {'Yes' if AWS_SECRET_KEY and AWS_SECRET_KEY != 'YOUR_SECRET_KEY_HERE' else 'No'}")
print(f"AWS_REGION         : {AWS_REGION}")
print(f"ADMIN_EMAIL        : {ADMIN_EMAIL}")
if AWS_ACCESS_KEY and AWS_ACCESS_KEY != 'YOUR_ACCESS_KEY_HERE':
    print(f"Access Key (first 4 chars): {AWS_ACCESS_KEY[:4]}...")
if AWS_SECRET_KEY and AWS_SECRET_KEY != 'YOUR_SECRET_KEY_HERE':
    print(f"Secret Key (first 4 chars): {AWS_SECRET_KEY[:4]}...")
print("="*50 + "\n")




# ─── Initialize SES client ────────────────────
try:
    if (AWS_ACCESS_KEY and AWS_SECRET_KEY
            and AWS_ACCESS_KEY != 'YOUR_ACCESS_KEY_HERE'
            and AWS_SECRET_KEY != 'YOUR_SECRET_KEY_HERE'):
        ses = boto3.client(
            'ses',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        print("✅ AWS SES client initialized (lazy validation mode)")
    else:
        print("❌ AWS credentials are missing or still using placeholder values")
        ses = None
except Exception as e:
    print(f"❌ Failed to initialize AWS SES client: {str(e)}")
    ses = None


# ─────────────────────────────────────────────
#  SEND CERTIFICATE EMAIL via SES
# ─────────────────────────────────────────────


# ─────────────────────────────────────────────
#  SEND CERTIFICATE EMAIL via SES
# ─────────────────────────────────────────────
def send_certificate_email(student_email, student_name, reg_id, course_name, completion_date):
    print("\n" + "="*50)
    print("📧 ATTEMPTING TO SEND CERTIFICATE EMAIL")
    print("="*50)
    print(f"📧 Timestamp       : {datetime.now()}")
    print(f"📧 To              : {student_email}")
    print(f"📧 Student Name    : {student_name}")
    print(f"📧 Registration ID : {reg_id}")
    print(f"📧 Course          : {course_name}")
    print(f"📧 Completion Date : {completion_date}")

    if ses is None:
        print("❌ SES client is None – not initialized properly")
        return False

    try:
        subject = f"🎓 Certificate of Completion – {course_name} | {reg_id}"

        html_content = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
</head>

<body style="margin:0; padding:0; font-family:Arial, sans-serif; color:#000000; background-color:#ffffff;">
<div style="max-width:600px; margin:0 auto; border:1px solid #e0e0e0; border-radius:8px; overflow:hidden;">

    <!-- Header -->
    <div style="background-color:#673ab7; color:#ffffff; padding:24px; text-align:center;">
        <h2 style="margin:0; font-size:26px; letter-spacing:1px;">🎓 Certificate of Completion</h2>
        <p style="margin:6px 0 0; font-size:14px;">ChakoraHub – Empowering Education &amp; Career Growth</p>
    </div>

    <!-- Body -->
    <div style="padding:20px; color:#000000;">

        <p>Dear <strong>{student_name}</strong>,</p>

        <p>
            Congratulations! Your <strong>Certificate of Completion</strong>
            has been successfully generated.
        </p>

        <!-- Congrats Box -->
        <div style="background-color:#e8f5e9; border:2px solid #4caf50;
                    padding:15px; border-radius:6px; margin:20px 0; text-align:center;">
            <h3 style="margin:0 0 6px; color:#2e7d32;">🏆 Well Done!</h3>
            <p style="margin:0;">
                You have successfully completed <strong>{course_name}</strong>.
            </p>
        </div>

        <!-- Certificate ID -->
        <div style="background-color:#f3e5f5; border-left:4px solid #673ab7;
                    padding:15px; margin:20px 0; font-size:16px;">
            <strong style="color:#000000;">📜 Certificate / Registration ID: {reg_id}</strong><br>
            <small style="color:#555555;">Please keep this ID for your records</small>
        </div>

        <!-- Details Table -->
        <h3 style="color:#673ab7; margin-top:25px;">Certificate Details</h3>

        <table style="width:100%; border-collapse:collapse; border:1px solid #e0e0e0;">
            <tr>
                <th colspan="2" style="background-color:#673ab7; color:#ffffff;
                                       padding:12px; text-align:left;">
                    Completion Summary
                </th>
            </tr>

            <tr>
                <td style="background-color:#f8f9fa; font-weight:bold; width:40%;
                           color:#000000; padding:12px; border-bottom:1px solid #e0e0e0;">
                    Student Name
                </td>
                <td style="background-color:#ffffff; width:60%;
                           color:#000000; padding:12px; border-bottom:1px solid #e0e0e0;">
                    {student_name}
                </td>
            </tr>

            <tr>
                <td style="background-color:#f8f9fa; font-weight:bold; width:40%;
                           color:#000000; padding:12px; border-bottom:1px solid #e0e0e0;">
                    Registration ID
                </td>
                <td style="background-color:#ffffff; width:60%;
                           color:#000000; padding:12px; border-bottom:1px solid #e0e0e0;">
                    {reg_id}
                </td>
            </tr>

            <tr>
                <td style="background-color:#f8f9fa; font-weight:bold; width:40%;
                           color:#000000; padding:12px; border-bottom:1px solid #e0e0e0;">
                    Course Completed
                </td>
                <td style="background-color:#ffffff; width:60%;
                           color:#000000; padding:12px; border-bottom:1px solid #e0e0e0;">
                    {course_name}
                </td>
            </tr>

            <tr>
                <td style="background-color:#f8f9fa; font-weight:bold; width:40%;
                           color:#000000; padding:12px; border-bottom:1px solid #e0e0e0;">
                    Completion Date
                </td>
                <td style="background-color:#ffffff; width:60%;
                           color:#000000; padding:12px; border-bottom:1px solid #e0e0e0;">
                    {completion_date}
                </td>
            </tr>

            <tr>
                <td style="background-color:#f8f9fa; font-weight:bold; width:40%;
                           color:#000000; padding:12px;">
                    Issued By
                </td>
                <td style="background-color:#ffffff; width:60%;
                           color:#000000; padding:12px;">
                    ChakoraHub
                </td>
            </tr>
        </table>

        <!-- How to Access -->
        <div style="background-color:#fff3e0; border:1px solid #ffe0b2;
                    padding:12px; margin-top:20px;">
            <strong>📌 How to access your certificate:</strong>
            <ul style="margin:8px 0 0 18px; padding:0;">
                <li>Login to <a href="https://www.chakorahub.com/login" style="color:#673ab7;">www.chakorahub.com</a></li>
                <li>Go to <strong>Certificate Generator</strong></li>
                <li>Enter Registration ID <strong>{reg_id}</strong></li>
            </ul>
        </div>

        <!-- Signature -->
        <div style="margin-top:25px; text-align:right; font-style:italic; color:#000000;">
            <strong>V. SUBHASH CHANDRA</strong><br>
            <small>ChakoraHub</small>
        </div>

        <!-- Footer -->
        <div style="margin-top:30px; border-top:1px solid #e0e0e0; padding-top:15px; text-align:center;">
            <p style="margin:0;">
                Regards,<br>
                <strong>ChakoraHub Team</strong>
            </p>
            <p style="font-size:12px; color:#777777; margin-top:10px;">
                This is an automated message. Please do not reply.
            </p>
        </div>

    </div>
</div>
</body>
</html>
"""

        # Plain text fallback
        text_content = f"""
DEAR {student_name},

Congratulations! Your Certificate of Completion has been generated.

REGISTRATION ID: {reg_id}

CERTIFICATE DETAILS:
Student Name    : {student_name}
Registration ID : {reg_id}
Course Completed: {course_name}
Completion Date : {completion_date}
Issued By       : ChakoraHub

HOW TO ACCESS:
- Visit www.chakorahub.com/login
- Go to Certificate Generator
- Enter Registration ID: {reg_id}

Regards,
V. SUBHASH CHANDRA
ChakoraHub Team
"""

        print(f"📧 Sending certificate email via SES to {student_email}...")

        response = ses.send_email(
            Source=ADMIN_EMAIL,
            Destination={
                "ToAddresses": [student_email],
                "CcAddresses": [ADMIN_EMAIL]
            },
            Message={
                "Subject": {"Data": subject},
                "Body": {
                    "Html": {"Data": html_content},
                    "Text": {"Data": text_content}
                }
            }
        )

        print("✅ CERTIFICATE EMAIL SENT SUCCESSFULLY!")
        print(f"📧 SES MessageId: {response.get('MessageId', 'N/A')}")
        return True

    except Exception as e:
        print("❌ CERTIFICATE EMAIL SENDING FAILED!")
        print(f"❌ Error type   : {type(e).__name__}")
        print(f"❌ Error message: {str(e)}")
        traceback.print_exc()
        if hasattr(e, 'response'):
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg  = e.response.get('Error', {}).get('Message', 'Unknown')
            print(f"❌ AWS Error Code   : {error_code}")
            print(f"❌ AWS Error Message: {error_msg}")
        return False


# ─────────────────────────────────────────────
#  FLASK ROUTE
# ─────────────────────────────────────────────
@app.route('/generate-certificate-legacy', methods=['GET', 'POST'])
def generate_certificate():
    return redirect(url_for('generate_student_report'), code=302)


# ---------- BLOGGER (Microservice Proxy) ----------
# The blogger UI is served by /blogger, and all data is loaded via AJAX from the
# blogger microservice (FastAPI) using the proxy routes defined near the end of
# this file.
#
# NOTE: The older file-upload based implementation (uploading to local disk) has
# been removed in favor of using the dedicated microservice.

# ------------------ Book Session + My Bookings (Combined) ------------------
@app.route('/book-session', methods=['GET', 'POST'])
def book_session():
    if 'user' not in session:
        return redirect(url_for('user_nrm_logins'))

    email = session['user']
    conn = get_db_connection()
    cursor = conn.cursor(DICT_CURSOR)

    # Fetch username
    cursor.execute("SELECT username FROM nrm_users WHERE email = %s", (email,))
    user_row = cursor.fetchone()
    if not user_row:
        flash("User not found in nrm_users table.")
        cursor.close()
        conn.close()
        return redirect(url_for('user_nrm_logins'))

    username = user_row['USERNAME']

    # ---------- Handle Booking ----------
    if request.method == 'POST':
        course_name = request.form['course']
        selected_date = request.form['selected_date']
        selected_time = request.form['selected_time']

        # Get course id
        cursor.execute("SELECT id FROM nrm_courses WHERE course_name = %s", (course_name,))
        course_row = cursor.fetchone()
        if not course_row:
            flash("Course not found.")
            cursor.close()
            conn.close()
            return redirect(url_for('book_session'))

        course_id = course_row['ID']

        # Get time slot id
        cursor.execute("SELECT id FROM nrm_time_slots WHERE slot_label = %s", (selected_time,))
        time_slot_row = cursor.fetchone()
        if not time_slot_row:
            flash("Time slot not found.")
            cursor.close()
            conn.close()
            return redirect(url_for('book_session'))

        time_slot_id = time_slot_row['ID']

        # Check if slot is already booked
        cursor.execute("""
            SELECT 1 FROM nrm_session_bookings
            WHERE session_date = %s AND time_slot_id = %s
        """, (selected_date, time_slot_id))
        if cursor.fetchone():
            flash("⚠️ This time slot is already booked for the selected date.")
            cursor.close()
            conn.close()
            return redirect(url_for('book_session'))

        # ─── Auto-create Teams meeting ───────────────────────────────
        meeting_link = ""
        try:
            graph_token = get_teams_token()
            graph_headers = {
                "Authorization": f"Bearer {graph_token}",
                "Content-Type": "application/json"
            }
            event_payload = {
                "subject": f"Session: {course_name} | {selected_time}",
                "start": {
                    "dateTime": f"{selected_date}T{selected_time.split('-')[0].strip()}:00",
                    "timeZone": "Asia/Kolkata"
                },
                "end": {
                    "dateTime": f"{selected_date}T{selected_time.split('-')[1].strip()}:00",
                    "timeZone": "Asia/Kolkata"
                },
                "attendees": [{
                    "emailAddress": {"address": email, "name": username},
                    "type": "required"
                }],
                "isOnlineMeeting": True,
                "onlineMeetingProvider": "teamsForBusiness"
            }
            organizer = "support@chakorahub.com"
            g_resp = requests.post(
                f"https://graph.microsoft.com/v1.0/users/{organizer}/events",
                headers=graph_headers,
                json=event_payload
            )
            g_data = g_resp.json()
            meeting_link = (g_data.get("onlineMeeting") or {}).get("joinUrl", "")
        except Exception as te:
            print(f"⚠️ Teams meeting creation failed: {te}")
            # Booking still proceeds even if Teams fails

        # Insert booking WITH meeting link
        cursor.execute("""
            INSERT INTO nrm_session_bookings (username, course_id, session_date, time_slot_id, meeting_link)
            VALUES (%s, %s, %s, %s, %s)
        """, (username, course_id, selected_date, time_slot_id, meeting_link))
        conn.commit()
        flash("✅ Session booked successfully!" + (" 🟣 Teams meeting created." if meeting_link else ""))

    # ---------- Fetch User's Bookings ----------
    query = """
        SELECT b.id, c.course_name, b.session_date, t.slot_label, b.meeting_link
        FROM nrm_session_bookings b
        JOIN nrm_courses c ON b.course_id = c.id
        JOIN nrm_time_slots t ON b.time_slot_id = t.id
        WHERE b.username = %s
        ORDER BY b.session_date, t.id
    """
    cursor.execute(query, (username,))
    bookings = cursor.fetchall()

    cursor.close()
    conn.close()

    return render_template(
        'book-session.html',
        bookings=bookings,
        profile_pic=session.get('profile_pic', 'profile_photo/defaultpicture.jpg')
    )
# ------------------ Cancel Booking ------------------
@app.route('/cancel-booking/<int:booking_id>', methods=['POST'])
def cancel_booking(booking_id):
    if 'user' not in session:
        return redirect(url_for('user_nrm_logins'))

    email = session['user']
    conn = get_db_connection()
    cursor = conn.cursor()

    # Validate user
    cursor.execute("SELECT username FROM nrm_users WHERE email = %s", (email,))
    user_row = cursor.fetchone()
    if not user_row:
        flash("User not found.")
        cursor.close()
        conn.close()
        return redirect(url_for('book_session'))

    username = user_row[0]

    # Delete only user's booking
    cursor.execute("DELETE FROM nrm_session_bookings WHERE id = %s AND username = %s", (booking_id, username))
    conn.commit()

    cursor.close()
    conn.close()
    flash("❌ Booking cancelled successfully!")
    return redirect(url_for('book_session'))
#------billing------

# ==========================================
# BILLING AUTH HELPER
# ==========================================
def get_auth():
    """Return HTTPBasicAuth for billing FastAPI backend, or None if no session."""
    username = session.get("user") or session.get("employee_id") or ""
    # The billing FastAPI uses Basic Auth; reuse username as both user and token.
    # Replace with proper credentials/token logic if your FastAPI requires it.
    if not username:
        return None
    return HTTPBasicAuth(str(username), str(username))

# ==========================================
# UPDATED BILLING ROUTE WITH S3
# ==========================================
@app.route('/billing', methods=['GET', 'POST'])
def billing():
    """
    Flask proxy route for billing and S3 file upload
    """
    
    # GET REQUEST: Render the billing form
    if request.method == 'GET':
        try:
            auth = get_auth()
            response = requests.get(
                f"{FASTAPI_BASE_URL}/courses",
                auth=auth,
                timeout=10
            )

            if response.status_code == 200:
                courses = response.json().get('courses', [])
            else:
                courses = []
                flash("⚠️ Could not load courses list", "warning")
                    
        except requests.exceptions.RequestException as e:
            courses = []
            flash(f"⚠️ Could not connect to backend: {str(e)}", "warning")
        
        return render_template("billing.html", courses=courses)
    
    # POST REQUEST: Forward to FastAPI with file
    if request.method == 'POST':
        receipt_file = request.files.get('receipt_file')
        try:
            # Extract form data
            billing_type = request.form.get('billing_type', '').strip()
            billing_category = request.form.get('billing_category', '').strip()
            payment_method = request.form.get('payment_mode', '').strip()
            phone = request.form.get('phone', '').strip()
            amount = request.form.get('amount', 0)
            upi_txn_id = request.form.get('upi_txn_id', '').strip() or None
            
            # Get receipt file
            
            # Validate required fields
            if not all([billing_type, billing_category, payment_method, phone, amount]):
                flash("❌ All required fields must be filled", "danger")
                return redirect(url_for('billing'))
             # Ensure name matches HTML
            # Convert amount to float
            try:
                amount = float(amount)
                if amount <= 0:
                    raise ValueError("Amount must be greater than 0")
            except (ValueError, TypeError) as e:
                flash(f"❌ Invalid amount: {str(e)}", "danger")
                return redirect(url_for('billing'))
            
            # Get authentication
            auth = get_auth()
            if not auth:
                flash("❌ Please login first", "danger")
                return redirect(url_for('login'))
            
            # Prepare multipart form data
            files = {}
            if receipt_file and receipt_file.filename != '':
        # You MUST pass a tuple with (filename, stream, content_type)
                files = {
                    'receipt_file': (
                        receipt_file.filename,
                        receipt_file.stream,  # Use .stream to pass the actual data
                        receipt_file.content_type
                    )
                }
            
            form_data = {
                'billing_type': billing_type,
                'billing_category': billing_category,
                'payment_method': payment_method,
                'amount': amount,
                'phone': phone,
                'currency': 'INR',
                'upi_txn_id': upi_txn_id
            }
            
            print(f"📤 Submitting billing with receipt: {receipt_file.filename if receipt_file else 'None'}")
            
            # Forward request to FastAPI
            response = requests.post(
                f"{FASTAPI_BASE_URL}/billing-create",
                data=form_data,
                files=files if files else None,
                auth=auth,
                timeout=30
            )
            
            # Handle response
            if response.status_code == 200:
                data = response.json()
                transaction_uuid = data.get('transaction_uuid', 'N/A')
                amount_paid = data.get('amount', 0)
                receipt_uploaded = data.get('receipt_uploaded', False)
                receipt_path = data.get('receipt_path')
                
                # Build success message
                receipt_msg = ""
                if receipt_uploaded and receipt_path:
                    receipt_msg = " Receipt uploaded successfully to S3!"
                elif receipt_uploaded:
                    receipt_msg = " Receipt uploaded!"
                
                flash(
                    f"✅ Payment of ₹{amount_paid} recorded successfully!{receipt_msg} "
                    f"Transaction ID: {transaction_uuid}",
                    "success"
                )
            elif response.status_code == 404:
                error_data = response.json()
                flash(f"❌ {error_data.get('detail', 'User not found')}", "danger")
            elif response.status_code == 401:
                flash("❌ Authentication failed. Please login again.", "danger")
                return redirect(url_for('login'))
            elif response.status_code == 500:
                error_data = response.json()
                flash(f"❌ Server error: {error_data.get('detail', 'Unknown error')}", "danger")
            else:
                flash(f"❌ Unexpected error: {response.status_code}", "danger")
                
        except requests.exceptions.Timeout:
            flash("❌ Request timeout. Please try again.", "danger")
        except requests.exceptions.ConnectionError:
            flash("❌ Could not connect to backend service", "danger")
        except requests.exceptions.RequestException as e:
            flash(f"❌ Network error: {str(e)}", "danger")
        except Exception as e:
            flash(f"❌ Error processing billing: {str(e)}", "danger")
        
        return redirect(url_for('billing'))

# ==========================================
# BILLING HISTORY
# ==========================================
@app.route('/billing-history')
def billing_history():
    """View billing history for logged-in user."""
    try:
        auth = get_auth()
        if not auth:
            flash("❌ Please login first", "danger")
            return redirect(url_for('login'))
        
        phone = session.get('phone')
        if not phone:
            flash("❌ Phone number not found in session", "danger")
            return redirect(url_for('billing'))
        
        response = requests.get(
            f"{FASTAPI_BASE_URL}/billing-history",
            params={"phone": phone},
            auth=auth,
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            billing_entries = data.get('entries', [])
        else:
            flash("❌ Could not fetch billing history", "danger")
            return redirect(url_for('billing'))
        
        return render_template("billing_history.html", entries=billing_entries)
            
    except Exception as e:
        flash(f"❌ Error: {str(e)}", "danger")
        return redirect(url_for('billing'))

# ==========================================
# CACHE STATS ENDPOINT (ADMIN)
# ==========================================
@app.route('/admin/cache-stats')
def cache_stats():
    """View local cache statistics (admin only)."""
    if session.get('usertype') != 'admin':
        flash("❌ Admin access required", "danger")
        return redirect(url_for('home'))
    
    try:
        now_ts = time.time()
        with _local_cache_lock:
            active_items = [k for k, (exp, _) in _local_cache_store.items() if exp > now_ts]
            expired_items = [k for k, (exp, _) in _local_cache_store.items() if exp <= now_ts]

        stats = {
            "connected": True,
            "backend": "local-memory",
            "total_keys": len(active_items),
            "expired_keys": len(expired_items),
        }
        return render_template('redis_stats.html', stats=stats)
    except Exception as e:
        return jsonify({
            "connected": False,
            "error": str(e)
        })

# ==========================================
# CLEAR CACHE ENDPOINT (ADMIN)
# ==========================================
@app.route('/admin/clear-cache', methods=['POST'])
def clear_cache():
    """Clear local in-process cache (admin only)."""
    if session.get('usertype') != 'admin':
        flash("❌ Admin access required", "danger")
        return redirect(url_for('home'))
    
    pattern = request.form.get('pattern')
    
    try:
        if pattern:
            cache_delete_pattern(pattern)
            flash(f"✅ Cleared cache matching: {pattern}", "success")
        else:
            cache_delete_pattern("")
            flash("✅ All cache cleared", "success")
    except Exception as e:
        flash(f"❌ Error clearing cache: {str(e)}", "danger")
    
    return redirect(url_for('cache_stats'))


# Employee_Report
@app.route('/employee-report')
def employee_report():
    if not _has_employee_admin_access():
        flash("Access denied.", "error")
        return redirect(url_for("home"), code=303)

    conn = get_db_connection()
    if conn is None:
        return "DB connection failed", 500

    cursor = conn.cursor(DICT_CURSOR)

    try:
        cursor.execute("""
            SELECT
                p.FIRST_NAME AS "First Name",
                p.LAST_NAME AS "Last Name",
                p.EMAIL AS "Email",
                p.PHONE AS "Phone Number",
                jw.CREATED_AT AS "Hire Date",
                e.APPLICATION_ID AS "Reg ID",
                e.STATUS AS "Status",
                d.TITLE AS "Job Title",
                k.DOC_NUMBER AS "Aadhaar",
                a.ASSET_TAG AS "Laptop",
                s.OS AS "OS",
                s.SSD AS "SSD",
                s.RAM AS "RAM",
                app.RESUME_S3_PATH AS "Resume"
            FROM EMP_NRM_PERSONAL p

            LEFT JOIN EMP_NRM_EMPLOYEES e
                ON p.EMPLOYEE_ID = e.EMPLOYEE_ID

            LEFT JOIN EMP_NRM_JOB_WORK jw
                ON p.EMPLOYEE_ID = jw.EMPLOYEE_ID

            LEFT JOIN EMP_NRM_DESIGNATIONS d
                ON jw.DESIGNATION_ID = d.DESIGNATION_ID

            LEFT JOIN EMP_NRM_KYC k
                ON p.EMPLOYEE_ID = k.EMPLOYEE_ID
                AND k.DOC_TYPE = 'AADHAAR'

            LEFT JOIN EMP_NRM_ASSET_ALLOCATION a
                ON p.EMPLOYEE_ID = a.EMPLOYEE_ID

            LEFT JOIN ASSETS s
                ON a.ASSET_TAG = s.SERIAL_ID   -- adjust if needed

            LEFT JOIN NRM_APPLICATIONS app
                ON e.APPLICATION_ID = app.APPLICATION_ID
        """)

        rows = cursor.fetchall()

        # Replace None values with "-"
        employee_data = []
        for row in rows:
            cleaned_row = {}
            for key, value in row.items():
                cleaned_row[key] = value if value is not None else "-"
            employee_data.append(cleaned_row)

    except Exception as e:
        print("Query Error:", e)
        employee_data = []

    finally:
        cursor.close()
        conn.close()

    return render_template('employee-report.html')
@app.route('/admin/employee-report-data', methods=['POST'])
def employee_report_data():
    """AJAX endpoint — called by employee-report.html via fetch().
    Uses the same get_db_connection() + ASSETS / EMP_NRM_* / NRM_APPLICATIONS
    tables that already exist in VSRSUBHASH$CHAKORA_DB.CHAKORA.
    TODO: migrate to employee-microservice when ready.
    """
    if not _has_employee_admin_access():
        return jsonify({"success": False, "message": "Unauthorized"}), 401
    # --- Read optional filters from JSON body ---
    body     = request.get_json(silent=True) or {}
    status_f = str(body.get("status",     "")).strip()   # → e.STATUS
    title_f  = str(body.get("job_title",  "")).strip()   # → d.TITLE ILIKE
    date_f   = str(body.get("hire_after", "")).strip()   # → jw.CREATED_AT >=
    conn = get_db_connection()      # APP_READONLY_WH · VSRSUBHASH$CHAKORA_DB · CHAKORA
    if conn is None:
        return jsonify({"success": False, "message": "DB connection failed"}), 500
    cursor = conn.cursor(DICT_CURSOR)
    try:
        # Same SQL + same tables as the existing employee_report() above
        sql = """
    -- NEW: PRIMARY source — EMPLOYEE_REGISTRATIONS (one table, all data)

SELECT

    er.FIRST_NAME           AS "First Name",

    er.LAST_NAME            AS "Last Name",

    er.EMAIL                AS "Email",

    er.PHONE                AS "Phone Number",

    er.JOINING_DATE         AS "Hire Date",

    er.APPLICATION_ID       AS "Reg ID",

    er.STATUS               AS "Status",

    er.DESIGNATION_TITLE    AS "Job Title",

    er.DEPARTMENT_NAME      AS "Department",

    -- KYC: LEFT JOIN for Aadhaar

    k.DOC_NUMBER            AS "Aadhaar",

    -- Asset Information

    al.ASSET_TAG            AS "Laptop",

    ast.OS                  AS "OS",

    ast.SSD                 AS "SSD",

    ast.RAM                 AS "RAM",

    app.RESUME_S3_PATH      AS "Resume"

FROM EMPLOYEE_REGISTRATIONS er

LEFT JOIN EMP_NRM_KYC k
    ON er.EMPLOYEE_ID = k.EMPLOYEE_ID
    AND k.DOC_TYPE = 'AADHAAR'

LEFT JOIN EMP_NRM_ASSET_ALLOCATION al
    ON er.EMPLOYEE_ID = al.EMPLOYEE_ID
    AND al.STATUS = 'Active'

LEFT JOIN ASSETS ast
    ON al.ASSET_TAG = ast.SERIAL_ID

LEFT JOIN NRM_APPLICATIONS app
    ON er.APPLICATION_ID = app.APPLICATION_ID

WHERE 1=1
"""
        params = []
        if status_f:
            sql += f" AND er.STATUS = '{status_f}'"
        if title_f:
            sql += f" AND er.DESIGNATION_TITLE ILIKE '%{title_f}%'"
        if date_f:
            sql += f" AND er.JOINING_DATE >= '{date_f}'"
        
        sql += " ORDER BY er.JOINING_DATE DESC"
        
        print("========== FINAL SQL ==========")
        print(sql)
        print("================================")
        
        cursor.execute(sql)
        rows = cursor.fetchall() or []
        employees = []
        for row in rows:
            clean = {}
            for k, v in row.items():
                if isinstance(v, (datetime, date)):
                    clean[k] = v.isoformat()          # JSON-safe dates
                else:
                    clean[k] = v if v is not None else "-"
            employees.append(clean)
        return jsonify({
            "success":      True,
            "employees":    employees,
            "record_count": len(employees)
        }), 200
    except Exception as e:
        print(f"[employee_report_data] Error: {e}")
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/employee/profile')
def emp_profile():
    return render_template('emp_profile.html')


@app.route('/employee/settings')
def emp_settings():
    return redirect(url_for('settings'))
# 360 DASHBOARD ROUTES
# ==========================================================

@app.route('/dashboard')
def dashboard():
    """
    Render the main 360 Dashboard Portal page.
    """
    # ✅ FIX 1: Proper session check
    if session.get('login_type') != 'employee':
        flash("Please login as employee to access dashboard", "error")
        return redirect(url_for('home'))
    
    # ✅ FIX 2: Verify employee_id exists in session
    if not session.get('employee_id'):
        flash("Employee ID not found in session. Please re-login", "error")
        return redirect(url_for('employee_home'))
    
    # Get current module from query parameters
    current_module = request.args.get('module', 'welcome')
    
    # For Infra360, fetch data if needed
    infra_data = None
    if current_module == 'infra360':
        infra_data = get_infra360_data()
        if not infra_data:
            flash("Error loading Infra360 data", "error")
    
    return render_template('dashboard.html', 
                         current_module=current_module,
                         infra_data=infra_data,
                         employee_id=session.get('employee_id'))

def get_infra360_data():
    """
    Fetch data for Infra360 module display.
    """
    # ✅ FIX 3: Consistent session check
    if session.get('login_type') != 'employee':
        print("❌ Not logged in as employee")
        return None
    
    employee_id = session.get('employee_id')
    
    # ✅ FIX 4: Validate employee_id
    if not employee_id:
        print("❌ Employee ID not found in session")
        return None
    
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        
        if not conn:
            print("❌ Database connection failed")
            return None
        
        cursor = conn.cursor(DICT_CURSOR)
        
        # Get employee personal data
        personal_query = """
        SELECT FIRST_NAME, LAST_NAME, PROFILE_PIC
        FROM EMP_NRM_PERSONAL
        WHERE EMPLOYEE_ID = %s
        """
        cursor.execute(personal_query, (employee_id,))
        personal_data = cursor.fetchone()
        
        # Get employee work data
        employee_query = """
        SELECT e.EMPLOYEE_ID, e.EMPLOYEE_NAME, e.DEPARTMENT,
               d.DEPT_NAME, des.TITLE as DESIGNATION
        FROM EMP_NRM_EMPLOYEES e
        LEFT JOIN EMP_NRM_DEPARTMENTS d ON e.DEPARTMENT = d.DEPT_ID
        LEFT JOIN EMP_NRM_JOB_WORK jw ON e.EMPLOYEE_ID = jw.EMPLOYEE_ID
        LEFT JOIN EMP_NRM_DESIGNATIONS des ON jw.DESIGNATION_ID = des.DESIGNATION_ID
        WHERE e.EMPLOYEE_ID = %s
        """
        cursor.execute(employee_query, (employee_id,))
        employee_data = cursor.fetchone()
        
        # Get asset allocation data
        asset_query = """
        SELECT ASSET_TYPE, ASSET_TAG, ALLOCATED_DATE, STATUS
        FROM EMP_NRM_ASSET_ALLOCATION
        WHERE EMPLOYEE_ID = %s AND STATUS = 'Active'
        ORDER BY ALLOCATED_DATE DESC
        LIMIT 1
        """
        cursor.execute(asset_query, (employee_id,))
        asset_data = cursor.fetchone()
        
        # Get enquiries data
        enquiries_query = """
        SELECT QUERY_TEXT as ENQUIRY, STATUS
        FROM EMP_NRM_QUERIES
        WHERE EMPLOYEE_ID = %s
        ORDER BY CREATED_AT DESC
        LIMIT 5
        """
        cursor.execute(enquiries_query, (employee_id,))
        enquiries_data = cursor.fetchall()
        
        # ✅ FIX 5: Safe data extraction with proper defaults
        emp_first_name = personal_data.get('FIRST_NAME', '') if personal_data else ''
        emp_last_name = personal_data.get('LAST_NAME', '') if personal_data else ''
        emp_full_name = f"{emp_first_name} {emp_last_name}".strip()
        
        emp_db_name = employee_data.get('EMPLOYEE_NAME', 'Employee') if employee_data else 'Employee'
        emp_name = emp_full_name if emp_full_name else emp_db_name
        
        # ✅ FIX 6: Handle None allocation_date safely
        allocation_date_str = 'N/A'
        if asset_data and asset_data.get('ALLOCATED_DATE'):
            try:
                allocation_date_str = asset_data['ALLOCATED_DATE'].strftime('%Y-%m-%d')
            except AttributeError:
                allocation_date_str = str(asset_data['ALLOCATED_DATE'])
        
        # Format the data with safe access
        infra_data = {
            'emp_name': emp_name,
            'emp_id': employee_data.get('EMPLOYEE_ID') if employee_data else employee_id,
            'department': employee_data.get('DEPT_NAME') or employee_data.get('DEPARTMENT', 'N/A') if employee_data else 'N/A',
            'designation': employee_data.get('DESIGNATION', 'N/A') if employee_data else 'N/A',
            'profile_pic': personal_data.get('PROFILE_PIC') if personal_data else None,
            'asset_name': asset_data.get('ASSET_TYPE', 'No Asset Assigned') if asset_data else 'No Asset Assigned',
            'asset_type': asset_data.get('ASSET_TYPE', 'N/A') if asset_data else 'N/A',
            'serial_number': asset_data.get('ASSET_TAG', 'N/A') if asset_data else 'N/A',
            'allocation_date': allocation_date_str,
            'status': asset_data.get('STATUS', 'N/A') if asset_data else 'N/A',
            'enquiries': [
                {
                    'ENQUIRY': row.get('ENQUIRY', 'No description'),
                    'STATUS': row.get('STATUS', 'Unknown')
                } for row in enquiries_data
            ] if enquiries_data else []
        }
        
        print(f"✅ Successfully loaded Infra360 data for employee {employee_id}")
        return infra_data
        
    except Exception as e:
        print(f"❌ Error fetching Infra360 data: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        # ✅ FIX 7: Proper cleanup in finally block
        try:
            if cursor:
                cursor.close()
        except Exception as e:
            print(f"Error closing cursor: {e}")
        
        try:
            if conn:
                conn.close()
        except Exception as e:
            print(f"Error closing connection: {e}")

# ==========================================================
# ELEARN360 PAGE
# ==========================================================

@app.route('/elearn360')
def elearn360_home():
    """
    Serves the Elearn 360 static dashboard page.
    """
    # ✅ FIX 8: Consistent session check
    if session.get('login_type') != 'employee':
        flash("Please login as employee", "error")
        return redirect(url_for('employee_home'))
    
    return render_template('elearn360.html',
                         employee_id=session.get('employee_id'),
                         employee_name=session.get('employee_name', 'Employee'))

# ==========================================================
# INFRA 360 EMPLOYEE PORTAL PAGE
# ==========================================================
@app.route('/infra360')
def infra_360():
    """
    Render the infra_360 Employee Portal page.
    """
    # ✅ FIX 9: Consistent session check
    if session.get('login_type') != 'employee':
        flash("Please login as employee", "error")
        return redirect(url_for('employee_home'))
    
    # ✅ FIX 10: Fetch and pass data to template
    infra_data = get_infra360_data()
    
    if not infra_data:
        # Return with default empty data instead of failing
        infra_data = {
            'emp_name': session.get('employee_name', 'Employee'),
            'emp_id': session.get('employee_id', 'N/A'),
            'department': 'N/A',
            'designation': 'N/A',
            'profile_pic': None,
            'asset_name': 'No Asset Assigned',
            'asset_type': 'N/A',
            'serial_number': 'N/A',
            'allocation_date': 'N/A',
            'status': 'N/A',
            'enquiries': []
        }
    
    return render_template('infra-360.html', **infra_data)

# ==========================================================
# personal_360 EMPLOYEE PORTAL PAGE
# ==========================================================

@app.route('/personal360')
def personal_360():
    """
    Render the personal_360 Employee Portal page.
    """
    # ✅ FIX 11: Consistent session check
    if session.get('login_type') != 'employee':
        flash("Please login as employee", "error")
        return redirect(url_for('employee_home'))
    
    return render_template('personal360.html',
                         employee_id=session.get('employee_id'),
                         employee_name=session.get('employee_name', 'Employee'))

# ==========================================================
# Apply Open Positions
# ==========================================================

@app.route("/apply")
def resume_upload_page():
    """Serves the main application submission form at /apply."""
    return render_template('resume-upload.html')


# 2. New Route for the Applicant's Application Status Tracker
@app.route("/track-application")
def track_application():
    """Serves the application status tracking page."""
    # The application ID is handled by JavaScript in the HTML from the query string (e.g., ?id=APP_XXXX)
    return render_template('track-application.html')


# 3. New Route for the Admin Dashboard
@app.route("/admin-dashboard")
def admin_dashboard_page():
    """Serves the admin dashboard page."""
    if not _has_employee_admin_access():
        flash("Access denied.", "error")
        return redirect(url_for("home"), code=303)

    return render_template('admin-dashboard.html')

# ==========================================================
# Finance360 EMPLOYEE PORTAL PAGE
# ==========================================================
'''
@app.route('/finance360')
def finance360():
    """
    Render the Finance360 Employee Portal page.
    """
    if not session.get('logged_in'):
        return redirect(url_for('employee_home'))
    return render_template('finance360.html')

        UPDATE nrm_positions 
        SET title = %s, department = %s, location = %s, 
            description = %s, status = %s, hiring_manager = %s,
            salary_range = %s, employment_type = %s, updated_at = CURRENT_TIMESTAMP()
        WHERE id = %s
        """
        cursor.execute(query, (
            data['title'],
            data['department'],
            data['location'],
            data.get('description', ''),
            data.get('status', 'pending'),
            data.get('hiring_manager', ''),
            data.get('salary_range', ''),
            data.get('employment_type', 'Full-time'),
            position_id
        ))
        conn.commit()
        
        # Return the updated position
        cursor = conn.cursor(DICT_CURSOR)
        cursor.execute("SELECT * FROM nrm_positions WHERE id = %s", (position_id,))
        row = cursor.fetchone()
        
        if row:
            updated_position = {
                'id': row['ID'],
                'title': row['TITLE'],
                'department': row['DEPARTMENT'],
                'location': row['LOCATION'],
                'description': row['DESCRIPTION'],
                'status': row['STATUS'],
                'created': row['CREATED_DATE'].strftime('%Y-%m-%d') if row['CREATED_DATE'] else None,
                'applicant_count': row['APPLICANT_COUNT'],
                'hiring_manager': row['HIRING_MANAGER'],
                'salary_range': row['SALARY_RANGE'],
                'employment_type': row['EMPLOYMENT_TYPE'],
                'updated_at': row['UPDATED_AT'].strftime('%Y-%m-%d %H:%M:%S') if row['UPDATED_AT'] else None
            }
            
            # Trigger Zapier webhook
            if old_status and old_status != data.get('status', 'pending'):
                trigger_zapier_webhook('status_changed', {
                    'old_status': old_status,
                    'new_status': data.get('status', 'pending'),
                    'position': updated_position
                })
            else:
                trigger_zapier_webhook('updated', updated_position)
                
            return jsonify(updated_position)
        else:
            return jsonify({'error': 'Position not found'}), 404
            
    except Exception as e:
        print(f"❌ Error updating position: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/positions/<int:position_id>', methods=['DELETE'])
def delete_position(position_id):
    """Delete a position from Snowflake"""
    conn = get_positions_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        # Get position data before deleting for Zapier webhook
        cursor = conn.cursor(DICT_CURSOR)
        cursor.execute("SELECT * FROM nrm_positions WHERE id = %s", (position_id,))
        row = cursor.fetchone()
        deleted_position = None
        
        if row:
            deleted_position = {
                'id': row['ID'],
                'title': row['TITLE'],
                'department': row['DEPARTMENT'],
                'location': row['LOCATION'],
                'description': row['DESCRIPTION'],
                'status': row['STATUS']
            }
        
        # Delete the position
        cursor = conn.cursor()
        query = "DELETE FROM nrm_positions WHERE id = %s"
        cursor.execute(query, (position_id,))
        conn.commit()
        
        # Trigger Zapier webhook for deletion
        if deleted_position:
            trigger_zapier_webhook('deleted', deleted_position)
        
        return jsonify({'message': 'Position deleted successfully'})
    except Exception as e:
        print(f"❌ Error deleting position: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/positions/<int:position_id>/status', methods=['PATCH'])
def update_position_status(position_id):
    """Update only the status of a position"""
    data = request.json
    conn = get_positions_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        # Get current status
        cursor.execute("SELECT status FROM nrm_positions WHERE id = %s", (position_id,))
        current_row = cursor.fetchone()
        if not current_row:
            return jsonify({'error': 'Position not found'}), 404
        
        old_status = current_row[0]
        new_status = data.get('status', 'pending')
        
        query = "UPDATE nrm_positions SET status = %s, updated_at = CURRENT_TIMESTAMP() WHERE id = %s"
        cursor.execute(query, (new_status, position_id))
        conn.commit()
        
        # Get updated position
        cursor = conn.cursor(DICT_CURSOR)
        cursor.execute("SELECT * FROM nrm_positions WHERE id = %s", (position_id,))
        row = cursor.fetchone()
        
        if row:
            updated_position = {
                'id': row['ID'],
                'title': row['TITLE'],
                'department': row['DEPARTMENT'],
                'location': row['LOCATION'],
                'description': row['DESCRIPTION'],
                'status': row['STATUS'],
                'created': row['CREATED_DATE'].strftime('%Y-%m-%d') if row['CREATED_DATE'] else None,
                'applicant_count': row['APPLICANT_COUNT'],
                'hiring_manager': row['HIRING_MANAGER'],
                'salary_range': row['SALARY_RANGE'],
                'employment_type': row['EMPLOYMENT_TYPE'],
                'updated_at': row['UPDATED_AT'].strftime('%Y-%m-%d %H:%M:%S') if row['UPDATED_AT'] else None
            }
            
            # Trigger Zapier webhook for status change
            trigger_zapier_webhook('status_changed', {
                'old_status': old_status,
                'new_status': new_status,
                'position': updated_position
            })
            
            return jsonify(updated_position)
        else:
            return jsonify({'error': 'Position not found after update'}), 404
            
    except Exception as e:
        print(f"❌ Error updating position status: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/zapier/webhook', methods=['POST'])
def zapier_webhook():
    """Endpoint for Zapier to receive position updates (two-way communication)"""
    data = request.json
    
    # Log the webhook data
    print("🔔 Zapier webhook received:")
    print(json.dumps(data, indent=2))
    
    # Process different types of webhook payloads
    action = data.get('action')
    position_data = data.get('position', {})
    
    if action == 'applicant_applied':
        # Update applicant count in Snowflake
        conn = get_positions_db_connection()
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE nrm_positions SET applicant_count = applicant_count + 1 WHERE id = %s",
                    (position_data.get('position_id'),)
                )
                conn.commit()
                print(f"✅ Updated applicant count for position {position_data.get('position_id')}")
            except Exception as e:
                print(f"❌ Error updating applicant count: {e}")
            finally:
                cursor.close()
                conn.close()
    
    return jsonify({
        'status': 'success', 
        'message': 'Webhook processed successfully',
        'processed_at': datetime.now().isoformat()
    })

@app.route('/api/positions/health', methods=['GET'])
def positions_health_check():
    """Health check endpoint for positions API"""
    conn = get_positions_db_connection()
    db_status = 'connected' if conn else 'disconnected'
    if conn:
        conn.close()
    
    return jsonify({
        'status': 'healthy',
        'database': db_status,
        'timestamp': datetime.now().isoformat()
    })

# Initialize positions table when app starts
@app.before_first_request
def setup_positions():
    initialize_positions_table()
'''
# ==========================================================
# END OF OPEN POSITIONS MANAGEMENT API ROUTES
# ==========================================================


# ==========================================================
# FEEDBACK MANAGEMENT
# ==========================================================

# ==========================================
# FEEDBACK ROUTES (Proxy to Student Service)
# ==========================================
@app.route('/feedback', methods=['GET', 'POST'])
def feedback_form():
    request_id = request.args.get('request_id') or request.args.get('meeting_id') or request.form.get('request_id') or request.form.get('meeting_id')

    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        email = request.form.get('email', '').strip()
        phone = request.form.get('phone', '').strip()
        feedback_text = request.form.get('feedback', '').strip()
        rating_raw = request.form.get('rating', '').strip()

        print(f"📝 Feedback POST received in Flask: email={email} phone={phone} rating={rating_raw} request_id={request_id}")

        if not all([name, email, phone, feedback_text, rating_raw]):
            session['feedback_error'] = "Please fill in all fields."
            return redirect(url_for('feedback_form', request_id=request_id))

        try:
            rating = int(rating_raw)
            if rating < 1 or rating > 5:
                raise ValueError("Rating out of range")

            target_url = f"{STUDENT_SERVICE_URL.rstrip('/')}/api/student/feedback"
            os.environ["NO_PROXY"] = STUDENT_INTERNAL_NO_PROXY
            os.environ["no_proxy"] = STUDENT_INTERNAL_NO_PROXY
            with requests.Session() as internal_session:
                internal_session.trust_env = False
                internal_session.proxies = {"http": None, "https": None}
                upstream_response = internal_session.post(
                    target_url,
                    json={
                        "name": name,
                        "email": email,
                        "phone": phone,
                        "feedback_text": feedback_text,
                        "rating": rating,
                        "request_id": request_id,
                        "meeting_id": request_id,
                    },
                    timeout=10,
                    allow_redirects=False,
                )

            status_code = upstream_response.status_code
            try:
                response_data = upstream_response.json()
            except ValueError:
                response_data = {"success": False, "message": "Non-JSON response from student service"}

            print(
                "📝 Feedback upstream response:",
                {
                    "status_code": status_code,
                    "response": response_data,
                },
            )

            if status_code != 200:
                err_message = (response_data or {}).get("detail") or (response_data or {}).get("message")
                session['feedback_error'] = err_message or "Error submitting feedback."
                return redirect(url_for('feedback_form', request_id=request_id))

            data = response_data or {}
            if not data.get("success", True):
                session['feedback_error'] = data.get("message", "Error submitting feedback.")
                return redirect(url_for('feedback_form', request_id=request_id))

            session['feedback_submitted'] = True
            session['feedback_name'] = name

        except ValueError:
            session['feedback_error'] = "Please provide a valid star rating (1 to 5)."
        except requests.RequestException as e:
            print("Feedback proxy error:", e)
            session['feedback_error'] = "Feedback service unavailable. Please try again."
        except Exception as e:
            print("Feedback error:", e)
            traceback.print_exc()
            session['feedback_error'] = "Error submitting feedback."

        return redirect(url_for('feedback_form', request_id=request_id))

    # ---------------- GET ----------------
    if request_id:
        try:
            target_url = f"{STUDENT_SERVICE_URL.rstrip('/')}/api/student/feedback/form?request_id={request_id}&meeting_id={request_id}"
            os.environ["NO_PROXY"] = STUDENT_INTERNAL_NO_PROXY
            os.environ["no_proxy"] = STUDENT_INTERNAL_NO_PROXY
            with requests.Session() as internal_session:
                internal_session.trust_env = False
                internal_session.proxies = {"http": None, "https": None}
                upstream_response = internal_session.get(
                    target_url,
                    timeout=8,
                    allow_redirects=False,
                )

            if upstream_response.status_code == 200:
                return upstream_response.text
            else:
                return upstream_response.text, upstream_response.status_code
        except Exception as e:
            print(f"❌ Feedback form proxy error: {e}")
            return "Feedback service temporarily unavailable", 503

    user_data = None
    logged_in = False

    feedback_submitted = session.pop('feedback_submitted', False)
    feedback_name = session.pop('feedback_name', None)
    feedback_error = session.pop('feedback_error', None)

    if session.get('user_id'):
        try:
            target_url = f"{STUDENT_SERVICE_URL.rstrip('/')}/api/student/feedback/prefill?user_id={session['user_id']}"
            os.environ["NO_PROXY"] = STUDENT_INTERNAL_NO_PROXY
            os.environ["no_proxy"] = STUDENT_INTERNAL_NO_PROXY
            with requests.Session() as internal_session:
                internal_session.trust_env = False
                internal_session.proxies = {"http": None, "https": None}
                upstream_response = internal_session.get(
                    target_url,
                    timeout=8,
                    allow_redirects=False,
                )

            status_code = upstream_response.status_code
            payload = upstream_response.json() if upstream_response.content else {}
            if status_code == 200:
                prefill = payload.get("prefill") or {}
                user_data = {
                    'username': prefill.get('username', ''),
                    'email': prefill.get('email', ''),
                    'phone': prefill.get('phone', ''),
                }
                logged_in = True
        except requests.RequestException as e:
            print(f"❌ Feedback prefill service error: {e}")

    return render_template(
        'student-feedback.html',
        user=user_data,
        logged_in=logged_in,
        feedback_submitted=feedback_submitted,
        feedback_name=feedback_name,
        feedback_error=feedback_error
    )

@app.route('/api/feedback', methods=['POST'])
def api_feedback_submit():
    """JSON feedback submission endpoint — accepts application/json from the browser."""
    try:
        data = request.get_json(force=True) or {}
        name          = str(data.get('name', '')).strip()
        email         = str(data.get('email', '')).strip()
        phone         = str(data.get('phone', '')).strip()
        feedback_text = str(data.get('feedback_text', '')).strip()
        rating_raw    = data.get('rating')

        if not all([name, email, phone, feedback_text, rating_raw]):
            return jsonify({"success": False, "message": "Please fill in all fields."}), 400

        try:
            rating = int(rating_raw)
            if rating < 1 or rating > 5:
                raise ValueError
        except (TypeError, ValueError):
            return jsonify({"success": False, "message": "Rating must be between 1 and 5."}), 400

        target_url = f"{STUDENT_SERVICE_URL.rstrip('/')}/api/student/feedback"
        os.environ["NO_PROXY"] = STUDENT_INTERNAL_NO_PROXY
        os.environ["no_proxy"] = STUDENT_INTERNAL_NO_PROXY
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            upstream_response = internal_session.post(
                target_url,
                json={
                    "name": name,
                    "email": email,
                    "phone": phone,
                    "feedback_text": feedback_text,
                    "rating": rating,
                },
                timeout=10,
                allow_redirects=False,
            )

        status_code = upstream_response.status_code
        try:
            response_data = upstream_response.json()
        except ValueError:
            response_data = {"success": False, "message": "Non-JSON response from student service"}

        print(
            "📝 /api/feedback upstream response:",
            {
                "status_code": status_code,
                "response": response_data,
            },
        )

        if status_code == 200 and (response_data or {}).get("success"):
            return jsonify({"success": True, "message": "Feedback submitted successfully"})

        err = (response_data or {}).get("detail") or (response_data or {}).get("message") or "Error submitting feedback."
        upstream_status = status_code if isinstance(status_code, int) and 400 <= status_code < 600 else 500
        return jsonify({"success": False, "message": err}), upstream_status

    except Exception as e:
        print("❌ /api/feedback error:", e)
        traceback.print_exc()
        return jsonify({"success": False, "message": "Feedback service unavailable."}), 500


@app.route('/api/feedbacks')
def api_feedbacks():
    """Get all feedbacks"""
    try:
        target_url = f"{STUDENT_SERVICE_URL.rstrip('/')}/api/student/feedbacks"
        os.environ["NO_PROXY"] = STUDENT_INTERNAL_NO_PROXY
        os.environ["no_proxy"] = STUDENT_INTERNAL_NO_PROXY
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            upstream_response = internal_session.get(
                target_url,
                timeout=10,
                allow_redirects=False,
            )

        status_code = upstream_response.status_code
        data = upstream_response.json() if upstream_response.content else {}

        if status_code == 200:
            return jsonify(data.get("feedbacks", []))
        else:
            return jsonify([])

    except requests.RequestException as e:
        print(f"❌ Feedbacks service error: {e}")
        return jsonify([])


# -----------------------------------------------
# BRS FORM ROUTE (INSERTS INTO SNOWFLAKE)
# -----------------------------------------------
# --------------------------------------------------
# BRS FORM ROUTE (INSERTS INTO SNOWFLAKE)
# --------------------------------------------------
@app.route("/brs-form", methods=["GET", "POST"])
def brs_form():
    if request.method == "POST":
        project_title = request.form.get("project_title", "").strip()
        prepared_by = request.form.get("prepared_by", "").strip()
        department = request.form.get("department", "")
        submission_date = request.form.get("submission_date", "")
        client_company = request.form.get("client_company", "")
        purpose = request.form.get("purpose", "")
        scope_in = request.form.get("scope_in", "")
        scope_out = request.form.get("scope_out", "")
        business_requirements = request.form.get("business_requirements", "")
        functional_requirements = request.form.get("functional_requirements", "")
        non_functional_requirements = request.form.get("non_functional_requirements", "")
        assumptions = request.form.get("assumptions", "")
        dependencies = request.form.get("dependencies", "")

        if not project_title or not prepared_by:
            flash("Project Title and Prepared By are required!", "error")
            return redirect(url_for("brs_form"))

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO CHAKORA.BRS_FORMS (
                    PROJECT_TITLE, PREPARED_BY, DEPARTMENT, SUBMISSION_DATE,
                    CLIENT_COMPANY, PURPOSE, SCOPE_IN, SCOPE_OUT,
                    BUSINESS_REQUIREMENTS, FUNCTIONAL_REQUIREMENTS,
                    NON_FUNCTIONAL_REQUIREMENTS, ASSUMPTIONS, DEPENDENCIES
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                project_title, prepared_by, department, submission_date,
                client_company, purpose, scope_in, scope_out,
                business_requirements, functional_requirements,
                non_functional_requirements, assumptions, dependencies
            ))
            conn.commit()
            flash("BRS Form Submitted Successfully!", "success")
        except Exception as e:
            print("❌ BRS Error:", e)
            flash("Error submitting form.", "error")
        finally:
            cursor.close()
            conn.close()

        return redirect(url_for("brs_form"))

    return render_template("brs_form.html")

# -----------------------------------------------
# VIEW ALL BRS SUBMISSIONS (ADMIN/USER READ ONLY)
# -----------------------------------------------
@app.route("/brs-list")
def brs_list():
    rows = []
    try:
        conn = get_db_connection()
        cur = conn.cursor(DICT_CURSOR)

        cur.execute("""
            SELECT 
                ID, PROJECT_TITLE, PREPARED_BY, SUBMISSION_DATE, 
                CLIENT_COMPANY, CREATED_AT
            FROM CHAKORA.BRS_FORMS
            ORDER BY CREATED_AT DESC
        """)

        rows = cur.fetchall()

    except Exception as e:
        print("❌ BRS Fetch Error:", e)

    finally:
        try: cur.close()
        except: pass
        try: conn.close()
        except: pass

    return render_template("brs_list.html", submissions=rows)


# ------------- PAGE ROUTE (HTML) -------------
@app.route("/asset-module")
def asset_module():
    return render_template("assetmodule.html")


# ------------- MASTER DATA APIs -------------
@app.route("/api/types")
def api_types():
    conn = get_db_connection()
    if not conn:
        return jsonify([])
    cur = conn.cursor(DICT_CURSOR)
    cur.execute("SELECT ID, TYPE_NAME FROM ASSET_TYPES ORDER BY TYPE_NAME")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)


@app.route("/api/models/<type_id>")
def api_models(type_id):
    conn = get_db_connection()
    if not conn:
        return jsonify([])
    cur = conn.cursor(DICT_CURSOR)
    cur.execute("SELECT ID, MODEL_NAME FROM ASSET_MODELS WHERE TYPE_ID=%s ORDER BY MODEL_NAME", (type_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)


@app.route("/api/serials/<model_id>")
def api_serials(model_id):
    conn = get_db_connection()
    if not conn:
        return jsonify([])
    cur = conn.cursor(DICT_CURSOR)
    cur.execute("SELECT ID, SERIAL_NO FROM ASSET_SERIALS WHERE MODEL_ID=%s ORDER BY SERIAL_NO", (model_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)


@app.route("/api/vendors/<type_id>")
def api_vendors(type_id):
    conn = get_db_connection()
    if not conn:
        return jsonify([])
    cur = conn.cursor(DICT_CURSOR)
    cur.execute("SELECT ID, VENDOR_NAME FROM ASSET_VENDORS WHERE TYPE_ID=%s ORDER BY VENDOR_NAME", (type_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)


# ------------- SAVE ASSET -------------
@app.route("/save_asset", methods=["POST"])
def save_asset():
    try:
        asset_id        = request.form.get("asset_id")
        type_id         = request.form.get("type_id")
        model_id        = request.form.get("model_id")
        serial_id       = request.form.get("serial_id")
        vendor_id       = request.form.get("vendor_id")

        os_val          = request.form.get("os")
        ram             = request.form.get("ram")
        ssd             = request.form.get("ssd")

        owned_by        = request.form.get("owned_by")
        purchase_date   = request.form.get("purchase_date")
        warranty_expiry = request.form.get("warranty_expiry")
        location        = request.form.get("location")
        condition       = request.form.get("condition")
        status          = request.form.get("status")
        price           = request.form.get("price")

        conn = get_db_connection()
        if not conn:
            return jsonify({"success": False, "message": "❌ DB connection failed"})

        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ASSETS
            (ASSET_ID, TYPE_ID, MODEL_ID, SERIAL_ID, VENDOR_ID,
             OS, RAM, SSD,
             OWNED_BY, PURCHASE_DATE, WARRANTY_EXPIRY,
             LOCATION, CONDITION, STATUS, PRICE)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            asset_id, type_id, model_id, serial_id, vendor_id,
            os_val, ram, ssd,
            owned_by, purchase_date, warranty_expiry,
            location, condition, status, price
        ))

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"success": True, "message": "✔ Asset saved successfully!"})

    except Exception as e:
        print("❌ Save Asset Error:", e)
        return jsonify({"success": False, "message": "❌ " + str(e)})


# ------------- GET REPORT -------------
@app.route("/get_assets")
def get_assets():
    conn = get_db_connection()
    if not conn:
        return jsonify([])
    cur = conn.cursor(DICT_CURSOR)

    cur.execute("""
        SELECT
            a.ASSET_ID,
            t.TYPE_NAME          AS ASSET_TYPE,
            m.MODEL_NAME         AS MODEL_NAME,
            s.SERIAL_NO          AS SERIAL_NO,
            v.VENDOR_NAME        AS VENDOR_NAME,
            a.OS                 AS OS,
            a.RAM                AS RAM,
            a.SSD                AS SSD,
            a.OWNED_BY           AS OWNED_BY,
            a.PURCHASE_DATE      AS PURCHASE_DATE,
            a.WARRANTY_EXPIRY    AS WARRANTY_EXPIRY,
            a.LOCATION           AS LOCATION,
            a.CONDITION          AS ASSET_CONDITION,
            a.STATUS             AS ASSET_STATUS,
            a.PRICE              AS PRICE
        FROM ASSETS a
        LEFT JOIN ASSET_TYPES   t ON a.TYPE_ID   = t.ID
        LEFT JOIN ASSET_MODELS  m ON a.MODEL_ID  = m.ID
        LEFT JOIN ASSET_SERIALS s ON a.SERIAL_ID = s.ID
        LEFT JOIN ASSET_VENDORS v ON a.VENDOR_ID = v.ID
        ORDER BY a.PURCHASE_DATE DESC, a.ASSET_ID;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)


# ------------- DELETE ASSET -------------
@app.route("/delete_asset/<asset_id>", methods=["DELETE"])
def delete_asset(asset_id):
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"success": False, "message": "❌ DB connection failed"})
        cur = conn.cursor()
        cur.execute("DELETE FROM ASSETS WHERE ASSET_ID=%s", (asset_id,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"success": True, "message": "✔ Asset deleted successfully!"})
    except Exception as e:
        print("❌ Delete Asset Error:", e)
        return jsonify({"success": False, "message": "❌ " + str(e)})

  

 
# ==================================================
# STEP 1: BRS SUBMIT  →  proxy to brs_service
# ==================================================
@app.route("/industry/brs", methods=["GET", "POST"])
def industry_brs():
 
    if request.method == "GET":
        return render_template("submit_brs.html")
 
    # ── POST: collect form + file, forward to microservice ──────
    try:
        form = request.form
        file = request.files.get("file")
 
        if not file:
            flash("❌ Please upload a BRS document.", "error")
            return redirect(url_for("industry_brs"))
 
        # Encode file to base64 so it travels as JSON
        file_bytes   = file.read()
        encoded_file = base64.b64encode(file_bytes).decode("utf-8")
 
        payload = {
            "project_id":          form.get("project_id"),
            "project_name":        form.get("project_name"),
            "project_description": form.get("project_description"),
            "client_name":         form.get("client_name"),
            "department":          form.get("department"),
            "requirement_type":    form.get("requirement_type"),
            "priority":            form.get("priority"),
            "business_objective":  form.get("business_objective"),
            "scope":               form.get("scope"),
            "start_date":          form.get("start_date"),
            "end_date":            form.get("end_date"),
            "contact_email":       form.get("contact_email"),
            "contact_phone":       form.get("contact_phone"),
            "filename":            file.filename,
            "filedata":            encoded_file,   # microservice ignores S3 part
            "amount":              form.get("amount"),
        }
 
        # ── HTTP POST → BRS Microservice ────────────────────────
        resp   = requests.post(
            f"{APPLICATION_SERVICE_URL}/brs/submit",
            json    = payload,
            timeout = 30,
        )
        result = resp.json()
 
        if resp.status_code == 200:
            # Store flash msg in session; show it AFTER alignment submit
            session["brs_flash_msg"] = (
                f"BRS submitted successfully! "
                f"Your BRS ID is {result.get('brs_id')}"
            )
            session["brs_id"] = result.get("brs_id")   # ← keep for alignment step

            # ── Best-effort BRS submission emails ─────────────────
            try:
                send_brs_admin_email(result.get("brs_id"), payload)
            except Exception as email_err:
                print(f"⚠️ BRS admin email failed: {email_err}")

            try:
                send_brs_client_email(result.get("brs_id"), payload)
            except Exception as email_err:
                print(f"⚠️ BRS client email failed: {email_err}")

            # ── STEP 1.5: Client billing ─────────────────────────
            # Same pattern as student /registration: collect the
            # amount + contact details now, charge via Razorpay next,
            # then continue on to the Alignment Charter only once the
            # payment is verified.
            session["brs_amount"]      = form.get("amount")
            session["brs_client_name"] = form.get("client_name")
            session["brs_email"]       = form.get("contact_email")
            session["brs_phone"]       = form.get("contact_phone")
            return redirect(url_for("brs_payment"))
 
        else:
            flash(f"❌ {result.get('error', 'Submission failed')}", "error")
            return redirect(url_for("industry_brs"))
 
    except requests.exceptions.ConnectionError:
        flash("❌ BRS service is unreachable. Please try again later.", "error")
        return redirect(url_for("industry_brs"))
 
    except Exception:
        current_app.logger.exception("BRS submit proxy error")
        flash("❌ Internal server error. Please try again.", "error")
        return redirect(url_for("industry_brs"))
 
 
# ==================================================
# STEP 1.5: CLIENT BILLING  (mirrors the student /registration
# Razorpay flow — reuses /create_razorpay_order for order creation
# and _verify_razorpay_signature for verification. Result is written
# to NRM_PAYMENTS via collaboration_service's /brs/payment/record.)
# ==================================================
@app.route("/brs/payment", methods=["GET"])
def brs_payment():
    brs_id = session.get("brs_id")
    if not brs_id:
        flash("❌ No pending BRS payment found. Please submit a BRS first.", "error")
        return redirect(url_for("industry_brs"))

    return render_template(
        "brs_payment.html",
        brs_id=brs_id,
        amount=session.get("brs_amount"),
        client_name=session.get("brs_client_name"),
        email=session.get("brs_email"),
        phone=session.get("brs_phone"),
        payment_key_id=(os.getenv("RZP_KEY_ID") or "").strip(),
    )


@app.route("/brs/payment/verify", methods=["POST"])
def brs_payment_verify():
    payload = request.get_json(silent=True) or {}

    brs_id         = payload.get("brs_id") or session.get("brs_id")
    amount         = payload.get("amount") or session.get("brs_amount")
    rzp_order_id   = str(payload.get("razorpay_order_id") or "").strip()
    rzp_payment_id = str(payload.get("razorpay_payment_id") or "").strip()
    rzp_signature  = str(payload.get("razorpay_signature") or "").strip()

    if not (brs_id and rzp_order_id and rzp_payment_id and rzp_signature):
        return jsonify({"success": False, "message": "Missing payment details."}), 400

    # Same signature check used by /api/shop/payment/webhook and
    # /create_razorpay_order's counterpart — RZP_KEY_SECRET based HMAC.
    signature_ok   = _verify_razorpay_signature(rzp_order_id, rzp_payment_id, rzp_signature)
    payment_status = "SUCCESS" if signature_ok else "FAILED"

    record_payload = {
        "brs_id":               brs_id,
        "client_name":          payload.get("client_name") or session.get("brs_client_name"),
        "email":                payload.get("email") or session.get("brs_email"),
        "phone":                payload.get("phone") or session.get("brs_phone"),
        "amount":               amount,
        "currency":             payload.get("currency", "INR"),
        "razorpay_order_id":    rzp_order_id,
        "razorpay_payment_id":  rzp_payment_id,
        "razorpay_signature":   rzp_signature,
        "payment_status":       payment_status,
        "payment_method":       "Razorpay",
    }

    payment_id = None
    try:
        resp = requests.post(
            f"{APPLICATION_SERVICE_URL}/brs/payment/record",
            json=record_payload,
            timeout=20,
        )
        record_result = resp.json()
        payment_id = record_result.get("payment_id")
    except Exception as exc:
        print(f"❌ /brs/payment/verify: could not reach collaboration service: {exc}")

    # ── KAFKA: publish payment.completed (same event name/shape as
    # /meeting/payment/verify, tagged so consumers can tell BRS
    # client payments apart from meeting/shop payments) ────────────
    kafka_publish("payment.completed", {
        "correlation_id":  str(uuid.uuid4()),
        "reference_id":    brs_id,
        "reference_type":  "BRS",
        "order_id":        rzp_order_id,
        "payment_id":      rzp_payment_id,
        "status":          payment_status.lower(),
        "amount":          amount,
        "source":          "flask_proxy",
        "timestamp":       datetime.utcnow().isoformat(),
    })

    if not signature_ok:
        print(f"❌ /brs/payment/verify: signature verification failed for BRS {brs_id}")
        return jsonify({"success": False, "message": "Payment signature verification failed."}), 400

    # Clear the staged billing details now that payment is recorded.
    session.pop("brs_amount", None)
    session.pop("brs_client_name", None)
    session.pop("brs_email", None)
    session.pop("brs_phone", None)

    return jsonify({
        "success": True,
        "message": "Payment verified successfully",
        "payment_id": payment_id,
        "redirect_url": url_for("alignment_charter"),
    }), 200

# ==================================================
# STEP 2: ALIGNMENT CHARTER PAGE  (display only)
# ==================================================
@app.route("/alignment-charter")
def alignment_charter():
    return render_template("alignment_charter.html")
 
 
# ==================================================
# STEP 3: ALIGNMENT SUBMIT  →  proxy to brs_service
# ==================================================
@app.route("/alignment-submit", methods=["POST"])
def alignment_submit():
 
    brs_id = session.pop("brs_id", None)
 
    # ── Notify microservice that alignment is confirmed ──────────
    try:
        requests.post(
            f"{APPLICATION_SERVICE_URL}/brs/alignment-confirm",
            json    = {"brs_id": brs_id},
            timeout = 10,
        )
    except Exception as exc:
        print(f"⚠️ Alignment confirm call failed (non-blocking): {exc}")
 
    # ── Show success flash to user ───────────────────────────────
    msg = session.pop("brs_flash_msg", None)
    if msg:
        flash(msg, "success")
 
    return redirect(url_for("alignment_charter"))

# --------------------------------------------------------------
# RENDER APPLICATION FORM PAGE
# --------------------------------------------------------------
# --------------------------------------------------------------
# RENDER APPLICATION FORM PAGE
# --------------------------------------------------------------
import uuid

APPLICATION_SERVICE_URL = os.getenv("https://mobqdeus63.execute-api.eu-north-1.amazonaws.com/Prod", "http://13.62.242.164:8020")
        
@app.route("/applicationform")
def applicationform():
    return render_template("applicationform.html")

def send_application_email(application_id, application_data):
    """
    Admin notification email - FULL DARK MODE + OUTLOOK SAFE
    """
    try:
        if not ses:
            print("❌ SES not initialized")
            return False

        html_content = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
</head>

<body style="margin:0; padding:0; background-color:#f4f4f4 !important;
             font-family:Arial, sans-serif; color:#000000 !important;">

<table width="100%" cellpadding="0" cellspacing="0"
       style="background-color:#f4f4f4 !important; padding:20px;">
<tr>
<td align="center">

<table width="600" cellpadding="0" cellspacing="0"
       style="background-color:#ffffff !important;
              border:1px solid #d1d5db;">

<!-- HEADER -->
<tr>
<td style="background-color:#2563eb !important;
           padding:18px;
           text-align:center;
           font-size:20px;
           font-weight:bold;">

<span style="color:#ffffff !important;">
📩 New Application Submitted
</span>

<br>

<span style="color:#ffffff !important; font-size:14px;">
Chakora Hub
</span>

</td>
</tr>

<!-- BODY -->
<tr>
<td style="padding:20px;
           font-size:14px;
           color:#000000 !important;">

<p style="color:#000000 !important;">
<strong>New industry collaboration application submitted.</strong>
</p>

<!-- APPLICATION ID BOX -->
<table width="100%" cellpadding="10" cellspacing="0"
       style="background-color:#eef2ff !important;
              border-left:4px solid #2563eb;
              margin:15px 0;">
<tr>
<td style="font-size:15px; color:#000000 !important;">
<strong>Application ID:</strong> {application_id}
</td>
</tr>
</table>

<!-- DETAILS TABLE -->
<table width="100%" cellpadding="8" cellspacing="0"
       style="border-collapse:collapse;
              border:1px solid #d1d5db;">

<tr>
<th colspan="2"
    style="background-color:#2563eb !important;
           color:#ffffff !important;
           text-align:left;
           padding:10px;">
Application Details
</th>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">
Name
</td>
<td style="color:#000000 !important;">
{application_data.get('full_name','')}
</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">
Email
</td>
<td style="color:#000000 !important;">
{application_data.get('email','')}
</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">
Phone
</td>
<td style="color:#000000 !important;">
{application_data.get('phone','')}
</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">
Organisation
</td>
<td style="color:#000000 !important;">
{application_data.get('organisation','')}
</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">
Type
</td>
<td style="color:#000000 !important;">
{application_data.get('collaboration_type','')}
</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">
Project Title
</td>
<td style="color:#000000 !important;">
{application_data.get('project_title','')}
</td>
</tr>

</table>

<!-- BUTTON -->
<table align="center" cellpadding="0" cellspacing="0" style="margin:25px auto;">
<tr>
<td align="center">

<!--[if mso]>
<v:roundrect xmlns:v="urn:schemas-microsoft-com:vml"
href="https://www.chakorahub.com/admin/applications"
style="height:40px;v-text-anchor:middle;width:220px;"
arcsize="15%"
stroke="f"
fillcolor="#0ea5e9">
<w:anchorlock/>
<center style="color:#ffffff;font-family:Arial;font-size:14px;font-weight:bold;">
VIEW APPLICATION
</center>
</v:roundrect>
<![endif]-->

<!--[if !mso]><!-- -->
<a href="https://www.chakorahub.com/admin/applications"
style="background-color:#0ea5e9 !important;
       color:#ffffff !important;
       display:inline-block;
       padding:12px 22px;
       text-decoration:none;
       font-size:14px;
       font-weight:bold;
       border-radius:18px;">
VIEW APPLICATION
</a>
<!--<![endif]-->

</td>
</tr>
</table>

<p style="font-size:12px; color:#374151 !important;">
Please log in to the admin panel to review this application.
</p>

</td>
</tr>

<!-- FOOTER -->
<tr>
<td style="background-color:#f3f4f6 !important;
           padding:12px;
           text-align:center;
           font-size:12px;
           color:#374151 !important;">

<strong style="color:#000000 !important;">
ChakoraHub Admin Notification
</strong>

</td>
</tr>

</table>

</td>
</tr>
</table>

</body>
</html>
"""

        ses.send_email(
            Source=ADMIN_EMAIL,
            Destination={"ToAddresses": [ADMIN_EMAIL]},
            Message={
                "Subject": {"Data": f"📩 New Application Received - {application_id}"},
                "Body": {"Html": {"Data": html_content}}
            }
        )

        print("✅ Admin application email sent successfully")
        return True

    except Exception as e:
        print("❌ Admin email failed:", e)
        return False
        

def send_applicant_confirmation_email(application_id, application_data):
    """
    Confirmation email sent to the person who submitted the application.
    """
    try:
        if not ses:
            print("❌ SES not initialized")
            return False

        applicant_email = (application_data.get("email") or "").strip()
        if not applicant_email:
            print("⚠️ Applicant confirmation email skipped: no email provided")
            return False

        applicant_name = application_data.get("full_name", "")

        html_content = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
</head>

<body style="margin:0; padding:0; background-color:#f4f4f4 !important;
             font-family:Arial, sans-serif; color:#000000 !important;">

<table width="100%" cellpadding="0" cellspacing="0"
       style="background-color:#f4f4f4 !important; padding:20px;">
<tr>
<td align="center">

<table width="600" cellpadding="0" cellspacing="0"
       style="background-color:#ffffff !important;
              border:1px solid #d1d5db;">

<!-- HEADER -->
<tr>
<td style="background-color:#2563eb !important;
           padding:18px;
           text-align:center;
           font-size:20px;
           font-weight:bold;">

<span style="color:#ffffff !important;">
✅ Application Received
</span>

<br>

<span style="color:#ffffff !important; font-size:14px;">
Chakora Hub
</span>

</td>
</tr>

<!-- BODY -->
<tr>
<td style="padding:20px;
           font-size:14px;
           color:#000000 !important;">

<p style="color:#000000 !important;">
Hi {applicant_name or 'there'},<br><br>
Thank you for submitting your industry collaboration application. We've received it and our team will review it shortly.
</p>

<!-- APPLICATION ID BOX -->
<table width="100%" cellpadding="10" cellspacing="0"
       style="background-color:#eef2ff !important;
              border-left:4px solid #2563eb;
              margin:15px 0;">
<tr>
<td style="font-size:15px; color:#000000 !important;">
<strong>Application ID:</strong> {application_id}
</td>
</tr>
</table>

<!-- DETAILS TABLE -->
<table width="100%" cellpadding="8" cellspacing="0"
       style="border-collapse:collapse;
              border:1px solid #d1d5db;">

<tr>
<th colspan="2"
    style="background-color:#2563eb !important;
           color:#ffffff !important;
           text-align:left;
           padding:10px;">
Application Details
</th>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">
Name
</td>
<td style="color:#000000 !important;">
{application_data.get('full_name','')}
</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">
Organisation
</td>
<td style="color:#000000 !important;">
{application_data.get('organisation','')}
</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">
Type
</td>
<td style="color:#000000 !important;">
{application_data.get('collaboration_type','')}
</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">
Project Title
</td>
<td style="color:#000000 !important;">
{application_data.get('project_title','')}
</td>
</tr>

</table>

<p style="font-size:12px; color:#374151 !important; margin-top:20px;">
If you have any questions, just reply to this email or write to
<a href="mailto:support@chakorahub.com">support@chakorahub.com</a>.
</p>

</td>
</tr>

<!-- FOOTER -->
<tr>
<td style="background-color:#f3f4f6 !important;
           padding:12px;
           text-align:center;
           font-size:12px;
           color:#374151 !important;">

<strong style="color:#000000 !important;">
ChakoraHub
</strong>

</td>
</tr>

</table>

</td>
</tr>
</table>

</body>
</html>
"""

        ses.send_email(
            Source=ADMIN_EMAIL,
            Destination={"ToAddresses": [applicant_email]},
            Message={
                "Subject": {"Data": f"✅ We've received your application - {application_id}"},
                "Body": {"Html": {"Data": html_content}}
            }
        )

        print(f"✅ Applicant confirmation email sent to {applicant_email}")
        return True

    except Exception as e:
        print("❌ Applicant confirmation email failed:", e)
        return False


# ══════════════════════════════════════════════════════════════════
#  BRS SUBMISSION EMAILS  (STEP 1 of the BRS flow)
#  Same house style as send_application_email /
#  send_applicant_confirmation_email above — sent right after
#  /brs/submit succeeds, using the form data already posted by
#  submit_brs.html.
# ══════════════════════════════════════════════════════════════════
def send_brs_admin_email(brs_id, data):
    """Admin notification email for a newly submitted BRS."""
    try:
        if not ses:
            print("❌ SES not initialized")
            return False

        html_content = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
</head>

<body style="margin:0; padding:0; background-color:#f4f4f4 !important;
             font-family:Arial, sans-serif; color:#000000 !important;">

<table width="100%" cellpadding="0" cellspacing="0"
       style="background-color:#f4f4f4 !important; padding:20px;">
<tr>
<td align="center">

<table width="600" cellpadding="0" cellspacing="0"
       style="background-color:#ffffff !important;
              border:1px solid #d1d5db;">

<!-- HEADER -->
<tr>
<td style="background-color:#4F46E5 !important;
           padding:18px;
           text-align:center;
           font-size:20px;
           font-weight:bold;">

<span style="color:#ffffff !important;">
📩 New BRS Submitted
</span>

<br>

<span style="color:#ffffff !important; font-size:14px;">
Chakora Hub — Collaboration Portal
</span>

</td>
</tr>

<!-- BODY -->
<tr>
<td style="padding:20px;
           font-size:14px;
           color:#000000 !important;">

<p style="color:#000000 !important;">
<strong>A new Business Requirement Specification (BRS) has been submitted.</strong>
</p>

<!-- BRS ID BOX -->
<table width="100%" cellpadding="10" cellspacing="0"
       style="background-color:#eef2ff !important;
              border-left:4px solid #4F46E5;
              margin:15px 0;">
<tr>
<td style="font-size:15px; color:#000000 !important;">
<strong>BRS ID:</strong> {brs_id}
</td>
</tr>
</table>

<!-- DETAILS TABLE -->
<table width="100%" cellpadding="8" cellspacing="0"
       style="border-collapse:collapse;
              border:1px solid #d1d5db;">

<tr>
<th colspan="2"
    style="background-color:#4F46E5 !important;
           color:#ffffff !important;
           text-align:left;
           padding:10px;">
Project Details
</th>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000; width:35%;">Project Name</td>
<td style="color:#000000 !important;">{data.get('project_name','')}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Client Name</td>
<td style="color:#000000 !important;">{data.get('client_name','')}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Department</td>
<td style="color:#000000 !important;">{data.get('department','') or '—'}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Requirement Type</td>
<td style="color:#000000 !important;">{data.get('requirement_type','')}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Priority</td>
<td style="color:#000000 !important;">{data.get('priority','')}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Contact Email</td>
<td style="color:#000000 !important;">{data.get('contact_email','')}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Contact Phone</td>
<td style="color:#000000 !important;">{data.get('contact_phone','') or '—'}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Project Amount</td>
<td style="color:#000000 !important;">₹{data.get('amount','') or '0.00'}</td>
</tr>

</table>

<p style="font-size:12px; color:#374151 !important; margin-top:20px;">
Please log in to the Project Dashboard to review this submission.
</p>

</td>
</tr>

<!-- FOOTER -->
<tr>
<td style="background-color:#f3f4f6 !important;
           padding:12px;
           text-align:center;
           font-size:12px;
           color:#374151 !important;">

<strong style="color:#000000 !important;">
ChakoraHub Admin Notification
</strong>

</td>
</tr>

</table>

</td>
</tr>
</table>

</body>
</html>
"""

        ses.send_email(
            Source=ADMIN_EMAIL,
            Destination={"ToAddresses": [ADMIN_EMAIL]},
            Message={
                "Subject": {"Data": f"📩 New BRS Submitted - {brs_id}"},
                "Body": {"Html": {"Data": html_content}}
            }
        )

        print("✅ Admin BRS email sent successfully")
        return True

    except Exception as e:
        print("❌ Admin BRS email failed:", e)
        return False


def send_brs_client_email(brs_id, data):
    """Confirmation email sent to the client who submitted the BRS."""
    try:
        if not ses:
            print("❌ SES not initialized")
            return False

        client_email = (data.get("contact_email") or "").strip()
        if not client_email:
            print("⚠️ BRS client confirmation email skipped: no contact_email provided")
            return False

        client_name = data.get("client_name", "")

        html_content = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
</head>

<body style="margin:0; padding:0; background-color:#f4f4f4 !important;
             font-family:Arial, sans-serif; color:#000000 !important;">

<table width="100%" cellpadding="0" cellspacing="0"
       style="background-color:#f4f4f4 !important; padding:20px;">
<tr>
<td align="center">

<table width="600" cellpadding="0" cellspacing="0"
       style="background-color:#ffffff !important;
              border:1px solid #d1d5db;">

<!-- HEADER -->
<tr>
<td style="background-color:#4F46E5 !important;
           padding:18px;
           text-align:center;
           font-size:20px;
           font-weight:bold;">

<span style="color:#ffffff !important;">
✅ BRS Submitted
</span>

<br>

<span style="color:#ffffff !important; font-size:14px;">
Chakora Hub — Collaboration Portal
</span>

</td>
</tr>

<!-- BODY -->
<tr>
<td style="padding:20px;
           font-size:14px;
           color:#000000 !important;">

<p style="color:#000000 !important;">
Hi {client_name or 'there'},<br><br>
Thank you for submitting your Business Requirement Specification (BRS). We've received it and our
team will begin reviewing your project shortly.
</p>

<!-- BRS ID BOX -->
<table width="100%" cellpadding="10" cellspacing="0"
       style="background-color:#eef2ff !important;
              border-left:4px solid #4F46E5;
              margin:15px 0;">
<tr>
<td style="font-size:15px; color:#000000 !important;">
<strong>BRS ID:</strong> {brs_id}
</td>
</tr>
</table>

<!-- DETAILS TABLE -->
<table width="100%" cellpadding="8" cellspacing="0"
       style="border-collapse:collapse;
              border:1px solid #d1d5db;">

<tr>
<th colspan="2"
    style="background-color:#4F46E5 !important;
           color:#ffffff !important;
           text-align:left;
           padding:10px;">
Project Details
</th>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000; width:35%;">Project Name</td>
<td style="color:#000000 !important;">{data.get('project_name','')}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Requirement Type</td>
<td style="color:#000000 !important;">{data.get('requirement_type','')}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Priority</td>
<td style="color:#000000 !important;">{data.get('priority','')}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Project Amount</td>
<td style="color:#000000 !important;">₹{data.get('amount','') or '0.00'}</td>
</tr>

</table>

<p style="font-size:13px; color:#000000 !important; margin-top:20px;">
<strong>Next steps:</strong> complete the project payment to proceed, then review and accept the
Alignment Charter to finalize your submission.
</p>

<p style="font-size:12px; color:#374151 !important; margin-top:16px;">
If you have any questions, just reply to this email or write to
<a href="mailto:support@chakorahub.com">support@chakorahub.com</a>.
</p>

</td>
</tr>

<!-- FOOTER -->
<tr>
<td style="background-color:#f3f4f6 !important;
           padding:12px;
           text-align:center;
           font-size:12px;
           color:#374151 !important;">

<strong style="color:#000000 !important;">
ChakoraHub
</strong>

</td>
</tr>

</table>

</td>
</tr>
</table>

</body>
</html>
"""

        ses.send_email(
            Source=ADMIN_EMAIL,
            Destination={"ToAddresses": [client_email]},
            Message={
                "Subject": {"Data": f"✅ We've received your BRS - {brs_id}"},
                "Body": {"Html": {"Data": html_content}}
            }
        )

        print(f"✅ BRS client confirmation email sent to {client_email}")
        return True

    except Exception as e:
        print("❌ BRS client confirmation email failed:", e)
        return False


# ══════════════════════════════════════════════════════════════════
#  ORGANIZATION SIGN-OFF EMAILS
#  Sent right after /org/signoff/submit succeeds. The sign-off form
#  itself doesn't collect an email address, so the client copy goes
#  to the CONTACT_EMAIL on file for the referenced BRS (looked up via
#  /brs/lookup/<brs_id> on the collaboration microservice).
# ══════════════════════════════════════════════════════════════════
def send_orgsignoff_admin_email(signoff_id, payload):
    """Admin notification email for a new organization sign-off."""
    try:
        if not ses:
            print("❌ SES not initialized")
            return False

        html_content = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
</head>

<body style="margin:0; padding:0; background-color:#f4f4f4 !important;
             font-family:Arial, sans-serif; color:#000000 !important;">

<table width="100%" cellpadding="0" cellspacing="0"
       style="background-color:#f4f4f4 !important; padding:20px;">
<tr>
<td align="center">

<table width="600" cellpadding="0" cellspacing="0"
       style="background-color:#ffffff !important;
              border:1px solid #d1d5db;">

<!-- HEADER -->
<tr>
<td style="background-color:#4F46E5 !important;
           padding:18px;
           text-align:center;
           font-size:20px;
           font-weight:bold;">

<span style="color:#ffffff !important;">
📩 New Organization Sign-Off
</span>

<br>

<span style="color:#ffffff !important; font-size:14px;">
Chakora Hub — Collaboration Portal
</span>

</td>
</tr>

<!-- BODY -->
<tr>
<td style="padding:20px;
           font-size:14px;
           color:#000000 !important;">

<p style="color:#000000 !important;">
<strong>A new Organization Sign-Off has been submitted.</strong>
</p>

<!-- SIGNOFF ID BOX -->
<table width="100%" cellpadding="10" cellspacing="0"
       style="background-color:#eef2ff !important;
              border-left:4px solid #4F46E5;
              margin:15px 0;">
<tr>
<td style="font-size:15px; color:#000000 !important;">
<strong>Reference ID:</strong> {signoff_id}
</td>
</tr>
</table>

<!-- DETAILS TABLE -->
<table width="100%" cellpadding="8" cellspacing="0"
       style="border-collapse:collapse;
              border:1px solid #d1d5db;">

<tr>
<th colspan="2"
    style="background-color:#4F46E5 !important;
           color:#ffffff !important;
           text-align:left;
           padding:10px;">
Sign-Off Details
</th>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000; width:35%;">Organization Name</td>
<td style="color:#000000 !important;">{payload.get('org_name','')}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Authorized Person</td>
<td style="color:#000000 !important;">{payload.get('authorized_person','')}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">BRS ID</td>
<td style="color:#000000 !important;">{payload.get('brs_id','')}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Approval Status</td>
<td style="color:#000000 !important;">{payload.get('approval_status','')}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Approval Notes</td>
<td style="color:#000000 !important;">{payload.get('approval_notes','') or '—'}</td>
</tr>

</table>

<p style="font-size:12px; color:#374151 !important; margin-top:20px;">
Please log in to the admin panel to review this sign-off.
</p>

</td>
</tr>

<!-- FOOTER -->
<tr>
<td style="background-color:#f3f4f6 !important;
           padding:12px;
           text-align:center;
           font-size:12px;
           color:#374151 !important;">

<strong style="color:#000000 !important;">
ChakoraHub Admin Notification
</strong>

</td>
</tr>

</table>

</td>
</tr>
</table>

</body>
</html>
"""

        ses.send_email(
            Source=ADMIN_EMAIL,
            Destination={"ToAddresses": [ADMIN_EMAIL]},
            Message={
                "Subject": {"Data": f"📩 New Organization Sign-Off - {signoff_id}"},
                "Body": {"Html": {"Data": html_content}}
            }
        )

        print("✅ Admin org sign-off email sent successfully")
        return True

    except Exception as e:
        print("❌ Admin org sign-off email failed:", e)
        return False


def send_orgsignoff_client_email(signoff_id, payload, brs_info=None):
    """Confirmation email sent to the client (BRS contact) once their
    project's organization sign-off has been recorded."""
    try:
        if not ses:
            print("❌ SES not initialized")
            return False

        brs_info = brs_info or {}
        client_email = (brs_info.get("contact_email") or "").strip()
        if not client_email:
            print("⚠️ Org sign-off client email skipped: no contact_email on file for this BRS")
            return False

        client_name = brs_info.get("client_name", "")
        project_name = brs_info.get("project_name", "")

        html_content = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
</head>

<body style="margin:0; padding:0; background-color:#f4f4f4 !important;
             font-family:Arial, sans-serif; color:#000000 !important;">

<table width="100%" cellpadding="0" cellspacing="0"
       style="background-color:#f4f4f4 !important; padding:20px;">
<tr>
<td align="center">

<table width="600" cellpadding="0" cellspacing="0"
       style="background-color:#ffffff !important;
              border:1px solid #d1d5db;">

<!-- HEADER -->
<tr>
<td style="background-color:#4F46E5 !important;
           padding:18px;
           text-align:center;
           font-size:20px;
           font-weight:bold;">

<span style="color:#ffffff !important;">
✅ Organization Sign-Off Confirmed
</span>

<br>

<span style="color:#ffffff !important; font-size:14px;">
Chakora Hub — Collaboration Portal
</span>

</td>
</tr>

<!-- BODY -->
<tr>
<td style="padding:20px;
           font-size:14px;
           color:#000000 !important;">

<p style="color:#000000 !important;">
Hi {client_name or 'there'},<br><br>
This is a confirmation that the organization sign-off for your project
{('<strong>' + project_name + '</strong>') if project_name else ''} has been recorded.
</p>

<!-- REFERENCE BOX -->
<table width="100%" cellpadding="10" cellspacing="0"
       style="background-color:#eef2ff !important;
              border-left:4px solid #4F46E5;
              margin:15px 0;">
<tr>
<td style="font-size:15px; color:#000000 !important;">
<strong>Reference ID:</strong> {signoff_id}<br>
<strong>BRS ID:</strong> {payload.get('brs_id','')}
</td>
</tr>
</table>

<table width="100%" cellpadding="8" cellspacing="0"
       style="border-collapse:collapse;
              border:1px solid #d1d5db;">

<tr>
<th colspan="2"
    style="background-color:#4F46E5 !important;
           color:#ffffff !important;
           text-align:left;
           padding:10px;">
Sign-Off Details
</th>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000; width:35%;">Organization Name</td>
<td style="color:#000000 !important;">{payload.get('org_name','')}</td>
</tr>

<tr>
<td style="background-color:#f3f4f6; font-weight:bold; color:#000000;">Approval Status</td>
<td style="color:#000000 !important;">{payload.get('approval_status','')}</td>
</tr>

</table>

<p style="font-size:12px; color:#374151 !important; margin-top:20px;">
If you have any questions, just reply to this email or write to
<a href="mailto:support@chakorahub.com">support@chakorahub.com</a>.
</p>

</td>
</tr>

<!-- FOOTER -->
<tr>
<td style="background-color:#f3f4f6 !important;
           padding:12px;
           text-align:center;
           font-size:12px;
           color:#374151 !important;">

<strong style="color:#000000 !important;">
ChakoraHub
</strong>

</td>
</tr>

</table>

</td>
</tr>
</table>

</body>
</html>
"""

        ses.send_email(
            Source=ADMIN_EMAIL,
            Destination={"ToAddresses": [client_email]},
            Message={
                "Subject": {"Data": f"✅ Organization Sign-Off Confirmed - {signoff_id}"},
                "Body": {"Html": {"Data": html_content}}
            }
        )

        print(f"✅ Org sign-off client email sent to {client_email}")
        return True

    except Exception as e:
        print("❌ Org sign-off client email failed:", e)
        return False

@app.route("/submit-application", methods=["POST"])
def submit_application():

    data = request.form.to_dict()

    try:
        response = requests.post(
            f"{APPLICATION_SERVICE_URL}/application/submit",
            json=data,
            timeout=10
        )

        if response.status_code == 200:
            res = response.json()
            application_id = res.get("application_id")

            # Best-effort admin email notification (proxy owns SES, so this stays here)
            try:
                send_application_email(application_id, data)
            except Exception as email_err:
                print(f"⚠️ Application email failed: {email_err}")

            # Best-effort confirmation email to the applicant themselves
            try:
                send_applicant_confirmation_email(application_id, data)
            except Exception as email_err:
                print(f"⚠️ Applicant confirmation email failed: {email_err}")

            flash(f"✅ Application Submitted! ID: {application_id}", "success")
        else:
            err_msg = "Submission failed"
            try:
                err_msg = response.json().get("error", err_msg)
            except Exception:
                pass
            flash(f"❌ {err_msg}", "error")

    except Exception as e:
        print("Proxy Error:", e)
        flash("⚠️ Service unavailable", "error")

    return redirect(url_for("applicationform"))
#-------------singoff---------------#
@app.route("/signoff-form")
def signoff_form():
    # Always require a fresh login when landing here (e.g. from the
    # Home page) — clear any existing org session so checkOrgSession()
    # on the page always finds logged_in=False and shows the popup
    # first. Log in via the popup to unlock the form underneath.
    session.pop("org_logged_in", None)
    session.pop("org_username", None)
    return render_template("organizationsingoff.html")
# ======================================
# ORGANIZATION SIGN-OFF (SNOWFLAKE INSERT)
# ======================================
@app.route("/org-signoff", methods=["GET", "POST"])
def org_signoff():

    if request.method != "POST":
        # Same as /signoff-form: render the page and let the JS
        # overlay prompt for login if the session isn't there.
        return render_template("organizationsingoff.html")

    if not session.get("org_logged_in"):
        # Session expired/missing at submit time — send back to the
        # sign-off page (popup) rather than the old full-page login.
        return redirect("/signoff-form")

    # ── POST: collect form + file, forward to clientcollabration
    #          microservice over plain HTTP (no API Gateway, no DB
    #          access here — app.py stays a pure proxy) ────────────
    try:
        form = request.form
        file = request.files.get("consent_form")

        if not file:
            return render_template(
                "organizationsingoff.html",
                message="❌ Please upload the signed consent form.",
                message_type="error",
            )

        # Encode file to base64 so it travels as JSON, matching the
        # BRS upload flow. The microservice only persists the
        # filename as metadata (no S3 wiring yet).
        file_bytes   = file.read()
        encoded_file = base64.b64encode(file_bytes).decode("utf-8")

        payload = {
            "org_name":           form.get("org_name"),
            "authorized_person":  form.get("authorized_person"),
            "brs_id":             form.get("brs_id"),
            "approval_notes":     form.get("approval_notes"),
            "approval_status":    form.get("approval_status"),
            "filename":           file.filename,
            "filedata":           encoded_file,   # microservice ignores S3 part
        }

        # ── HTTP POST → Client Collaboration Microservice ────────
        resp = requests.post(
            f"{APPLICATION_SERVICE_URL}/org/signoff/submit",
            json=payload,
            timeout=30,
        )
        result = resp.json()

        if resp.status_code == 200:
            signoff_id = result.get("signoff_id")

            # ── Best-effort org sign-off emails ────────────────────
            try:
                send_orgsignoff_admin_email(signoff_id, payload)
            except Exception as email_err:
                print(f"⚠️ Org sign-off admin email failed: {email_err}")

            try:
                brs_info = None
                brs_id = payload.get("brs_id")
                if brs_id:
                    lookup_resp = requests.get(
                        f"{APPLICATION_SERVICE_URL}/brs/lookup/{brs_id}",
                        timeout=10,
                    )
                    if lookup_resp.ok:
                        brs_info = lookup_resp.json()
                send_orgsignoff_client_email(signoff_id, payload, brs_info)
            except Exception as email_err:
                print(f"⚠️ Org sign-off client email failed: {email_err}")

            return render_template(
                "organizationsingoff.html",
                message=f"✔ Organization Sign-Off submitted successfully! "
                        f"Reference ID: {signoff_id}",
                message_type="success",
            )

        return render_template(
            "organizationsingoff.html",
            message=f"❌ {result.get('error', 'Submission failed')}",
            message_type="error",
        )

    except requests.exceptions.ConnectionError:
        return render_template(
            "organizationsingoff.html",
            message="❌ Sign-off service is unreachable. Please try again later.",
            message_type="error",
        )

    except Exception:
        current_app.logger.exception("Org sign-off proxy error")
        return render_template(
            "organizationsingoff.html",
            message="❌ Internal server error. Please try again.",
            message_type="error",
        )

# =====================================================
# PROJECT STATUS  (PURE HTTP PROXY – NO LAMBDA, NO DB HERE)
# ---------------------------------------------------------------
# The Track Project Status page used to call an AWS Lambda /
# API Gateway URL directly from the browser. That's gone now —
# the page calls this same-origin route instead, and app.py just
# forwards the request over plain HTTP to the Client Collaboration
# microservice (clientcollabration.py), which is the only place
# that opens a DB connection for this data. Same proxy pattern as
# /track-login and /org-signoff.
# =====================================================
@app.route('/api/project-status/<project_id>', methods=['GET'])
def track_project_status(project_id):
    if not session.get("track_logged_in"):
        return jsonify({'error': 'Not logged in'}), 401

    try:
        resp = requests.get(
            f"{APPLICATION_SERVICE_URL}/project/status/{project_id}",
            timeout=15,
        )
        return jsonify(resp.json()), resp.status_code

    except requests.exceptions.RequestException:
        return jsonify({'error': 'Project status service is unreachable. Please try again later.'}), 503

    except Exception as e:
        print("❌ Project status proxy error:", e)
        traceback.print_exc()
        return jsonify({'error': 'Internal server error'}), 500


# =====================================================
# PROJECT DASHBOARD PAGE
# ---------------------------------------------------------------
# Same treatment as /track-project and /signoff-form: force a
# fresh login every time the page loads, and let the JS overlay
# prompt for it. The table/stat data is fetched client-side from
# /api/project-dashboard/data (pure HTTP proxy below).
# =====================================================
@app.route("/project-dashboard")
def project_dashboard():
    session.pop("dashboard_logged_in", None)
    return render_template("project-dashboard.html")


@app.route("/dashboard-session-check")
def dashboard_session_check():
    return jsonify({"logged_in": bool(session.get("dashboard_logged_in"))})


@app.route("/api/project-dashboard/data", methods=["GET"])
def project_dashboard_data():
    if not session.get("dashboard_logged_in"):
        return jsonify({'error': 'Not logged in'}), 401

    try:
        resp = requests.get(
            f"{APPLICATION_SERVICE_URL}/project/dashboard/summary",
            timeout=15,
        )
        return jsonify(resp.json()), resp.status_code

    except requests.exceptions.RequestException:
        return jsonify({'error': 'Dashboard service is unreachable. Please try again later.'}), 503

    except Exception as e:
        print("❌ Project dashboard proxy error:", e)
        traceback.print_exc()
        return jsonify({'error': 'Internal server error'}), 500

# =====================================================
# TRACK PROJECT PAGE
# =====================================================
@app.route("/track-project")
def track_project():
    session.pop("track_logged_in", None)
    return render_template("track-project-status.html")

@app.route("/track-session-check")
def track_session_check():
    return jsonify({"logged_in": bool(session.get("track_logged_in"))})

# =====================================================
# ORGANIZATION SIGN-OFF PAGE
# =====================================================
@app.route("/organization-signoff")
def organization_signoff():
    session.pop("org_logged_in", None)
    return render_template("organizationsingoff.html")

@app.route("/org-session-check")
def org_session_check():
    return jsonify({
        "logged_in": bool(session.get("org_logged_in"))
    })
# =====================================================
# COMMON LOGIN (PROXY – USED BY BOTH PAGES)
# ---------------------------------------------------------------
# Pure HTTP proxy in front of the Client Collaboration microservice
# (clientcollabration.py, POST /auth/login). app.py never opens a
# DB connection here — it only forwards the request and relays the
# response, same pattern as /org-signoff.
#
#   GET  /track-login  -> renders the login page (track-login.html)
#   POST /track-login  -> forwards credentials, sets the session,
#                          and either redirects (success) or
#                          re-renders the page with an error
# =====================================================
#   POST body carries a "context" field ("track", "org", or
#   "dashboard") telling this shared handler which page's popup
#   is calling it, so it sets the matching session flag. Each
#   page's session-check route (/track-session-check,
#   /org-session-check, /dashboard-session-check) only looks at
#   its own flag, so logging into one popup never unlocks another.
_LOGIN_SESSION_FLAGS = {
    "track":     ("track_logged_in",     "track_username",     "/track-project"),
    "org":       ("org_logged_in",       "org_username",       "/signoff-form"),
    "dashboard": ("dashboard_logged_in", "dashboard_username", "/project-dashboard"),
}


@app.route("/track-login", methods=["GET", "POST"])
def track_login():

    if request.method == "GET":
        return render_template("org-track-login.html")

    username = request.form.get("username")
    password = request.form.get("password")
    context = request.form.get("context", "track")

    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    try:
        response = requests.post(
            f"{APPLICATION_SERVICE_URL}/auth/login",
            json={
                "username": username,
                "password": password,
                "context": context,
            },
            timeout=15,
        )
        result = response.json()
    except requests.exceptions.RequestException:
        if is_ajax:
            return jsonify({"success": False, "error": "Login service is unreachable. Please try again later."}), 503
        return render_template(
            "org-track-login.html",
            error="Login service is unreachable. Please try again later."
        )

    if result.get("success"):
        session_flag, username_flag, redirect_to = _LOGIN_SESSION_FLAGS.get(
            result.get("context", context), _LOGIN_SESSION_FLAGS["track"]
        )
        session[session_flag] = True
        session[username_flag] = username

        if is_ajax:
            return jsonify({"success": True})
        return redirect(redirect_to)

    error_msg = result.get("error", "Invalid Username or Password")

    if is_ajax:
        return jsonify({"success": False, "error": error_msg})

    return render_template(
        "org-track-login.html",
        error=error_msg
    )
@app.route('/add_course', methods=['GET', 'POST'])
def add_course():
    if 'user' not in session:
        flash("Please login first.", "error")
        return redirect(url_for('home'))

    if request.method == 'GET':
        return redirect(url_for('upload_page'))

    if not _is_admin_user(allow_db_fallback=True):
        flash("Admin access required.", "danger")
        return redirect(url_for('upload_page'))

    payload = {
        "course_name": str(request.form.get('course_name') or '').strip(),
        "course_code": str(request.form.get('course_code') or '').strip().upper(),
        "registration_category": str(request.form.get('registration_category') or '').strip(),
        "course_fee": request.form.get('course_fee'),
        "is_active": 'Y' if str(request.form.get('is_active') or '').strip().upper() == 'Y' else 'N',
    }

    try:
        resp = requests.post(
            f"{STUDENT_SERVICE_URL}/api/student/admin/courses",
            json=payload,
            timeout=15,
        )
        body = resp.json()
    except requests.RequestException as exc:
        flash(f"Student service unavailable: {exc}", "error")
        return redirect(url_for('upload_page'))
    except ValueError:
        flash("Student service returned non-JSON response.", "error")
        return redirect(url_for('upload_page'))

    success = bool(resp.ok and body.get('success', False))
    message = body.get('message') or body.get('detail') or ('Course saved successfully.' if success else 'Unable to save course.')
    flash(message, 'success' if success else 'error')
    return redirect(url_for('upload_page'))


@app.route('/save_course', methods=['POST'])
def save_course():
    if 'user' not in session:
        flash("Please login first.", "error")
        return redirect(url_for('home'))

    if not _is_admin_user(allow_db_fallback=True):
        flash("Admin access required.", "danger")
        return redirect(url_for('upload_page'))

    return add_course()


@app.route('/delete_course/<int:course_id>', methods=['POST'])
def delete_course(course_id):
    if 'user' not in session:
        flash("Please login first.", "error")
        return redirect(url_for('home'))

    if not _is_admin_user(allow_db_fallback=True):
        flash("Admin access required.", "danger")
        return redirect(url_for('upload_page'))

    try:
        resp = requests.delete(
            f"{STUDENT_SERVICE_URL}/api/student/admin/courses/{course_id}",
            timeout=15,
        )
        body = resp.json()
    except requests.RequestException as exc:
        flash(f"Student service unavailable: {exc}", "error")
        return redirect(url_for('upload_page'))
    except ValueError:
        flash("Student service returned non-JSON response.", "error")
        return redirect(url_for('upload_page'))

    success = bool(resp.ok and body.get('success', False))
    message = body.get('message') or body.get('detail') or ('Course deleted successfully.' if success else 'Unable to delete course.')

    if success and isinstance(message, str) and message.strip().lower().endswith(': none'):
        message = f"Course deleted successfully: ID {course_id}"

    flash(message, 'success' if success else 'error')

    return redirect(url_for('upload_page'))


# ================== 📅 Employee Calendar Route ==================
@app.route('/employee-calender')
def employee_calender():
    # ✅ FIXED: correct session key
    if session.get('login_type') != 'employee':
        flash("Please login as employee to access the calendar")
        return redirect(url_for('home'))

    try:
        month = int(request.args.get('month', datetime.now().month))
        year = int(request.args.get('year', datetime.now().year))
    except ValueError:
        month = datetime.now().month
        year = datetime.now().year

    conn = get_db_connection()
    cursor = conn.cursor(DICT_CURSOR)

    month_start = datetime(year, month, 1)
    next_month_start = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)

    # Step 1: Get festivals
    cursor.execute("""
        SELECT festival_date, festival_name
        FROM nrm_festivals
        WHERE festival_date >= %s AND festival_date < %s
    """, (month_start, next_month_start))
    festival_rows = cursor.fetchall()

    month_nrm_festivals = {
        row['FESTIVAL_DATE'].strftime('%Y-%m-%d'): row['FESTIVAL_NAME']
        for row in festival_rows
    }

    # Step 2: Get all slots
    cursor.execute("SELECT id, slot_label FROM nrm_time_slots ORDER BY id")
    all_slots_rows = cursor.fetchall()
    all_slots = [row['SLOT_LABEL'] for row in all_slots_rows]

    # Step 3: Get bookings
    cursor.execute("""
        SELECT
            s.session_date,
            t.slot_label
        FROM nrm_session_bookings s
        JOIN nrm_time_slots t ON s.time_slot_id = t.id
        WHERE s.session_date >= %s AND s.session_date < %s
        ORDER BY s.session_date, t.slot_label
    """, (month_start, next_month_start))
    booking_rows = cursor.fetchall()

    # Step 4: Organize bookings
    booked_slots_dict = {}
    for b in booking_rows:
        key = b['SESSION_DATE'].strftime('%Y-%m-%d')
        booked_slots_dict.setdefault(key, set()).add(b['SLOT_LABEL'])

    conn.close()

    # Step 5: Build calendar data
    days_in_month = (datetime(year, month % 12 + 1, 1) - timedelta(days=1)).day
    calendar_data = {}

    for day in range(1, days_in_month + 1):
        date_str = f"{year}-{month:02}-{day:02}"
        festival = month_nrm_festivals.get(date_str)
        bookings_status = []

        for slot in all_slots:
            status = "Booked" if slot in booked_slots_dict.get(date_str, set()) else "Not booked"
            bookings_status.append({
                "slot": slot,
                "status": status
            })

        calendar_data[date_str] = {
            "festival": festival,
            "bookings": bookings_status
        }

    return render_template(
        'employee-calender.html',
        month=month,
        year=year,
        calendar_data=calendar_data,
        session_role=session.get('login_type', ''),
        username=session.get('user', '').split('@')[0]
    )






#payslip#

#payslip#

COMPANY_NAME    = "Chakora Hub"
COMPANY_ADDRESS = "kondapur, Hyderabad, Telangana - 500084"
COMPANY_EMAIL   = "admin@chakorahub.com"
COMPANY_PHONE   = "+91 77991 01166"

# ── Image paths ──────────────────────────────────────────────
# Both images must be in your Flask 'static' folder.
import os
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
LOGO_PATH   = os.path.join(BASE_DIR, 'static', 'logo.png')


def fetch_payslip_data(employee_id):
    """
    Fetches all data needed for the pay slip.
    For old employees: reads from EMP_NRM_PERSONAL, EMP_NRM_JOB_WORK, EMP_NRM_SALARY.
    For new employees (registered via admin form): falls back to EMPLOYEE_REGISTRATIONS.
    """
    try:
        conn = get_db_connection()
        if not conn:
            print("[PaySlip] DB connection failed")
            return None

        cursor = conn.cursor(DICT_CURSOR)

        # 1. Core employee info from EMP_NRM_EMPLOYEES
        cursor.execute("""
            SELECT EMPLOYEE_ID, EMPLOYEE_NAME, EMAIL, APPLIED_DATE
            FROM EMP_NRM_EMPLOYEES
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        emp_row = cursor.fetchone()
        if not emp_row:
            print(f"[PaySlip] No row found in EMP_NRM_EMPLOYEES for {employee_id}")
            return None

        # 2. Personal details — try EMP_NRM_PERSONAL first (old employees)
        cursor.execute("""
            SELECT FIRST_NAME, LAST_NAME, PHONE, EMAIL
            FROM EMP_NRM_PERSONAL
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        personal_row = cursor.fetchone() or {}

        first     = personal_row.get('FIRST_NAME') or ''
        last      = personal_row.get('LAST_NAME')  or ''
        full_name = f"{first} {last}".strip() or emp_row.get('EMPLOYEE_NAME', 'N/A')
        email     = personal_row.get('EMAIL') or emp_row.get('EMAIL', 'N/A')
        phone     = personal_row.get('PHONE') or 'N/A'

        # 3. Salary — try EMP_NRM_SALARY first (old employees)
        cursor.execute("""
            SELECT BASIC, HRA, ALLOWANCES, DEDUCTIONS, NET_SALARY
            FROM EMP_NRM_SALARY
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        sal_row    = cursor.fetchone() or {}
        basic      = float(sal_row.get('BASIC')      or 0.00)
        hra        = float(sal_row.get('HRA')         or 0.00)
        allowances = float(sal_row.get('ALLOWANCES')  or 0.00)
        deductions = float(sal_row.get('DEDUCTIONS')  or 0.00)
        net_salary = float(sal_row.get('NET_SALARY')  or 0.00)
        if net_salary == 0.00:
            net_salary = basic

        # 4. Department / Designation — try EMP_NRM_JOB_WORK first (old employees)
        cursor.execute("""
            SELECT DEPT_ID, DESIGNATION_ID
            FROM EMP_NRM_JOB_WORK
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        job_row    = cursor.fetchone() or {}
        dept_name  = 'N/A'
        desig_name = 'N/A'

        if job_row.get('DEPT_ID'):
            cursor.execute(
                "SELECT DEPT_NAME FROM EMP_NRM_DEPARTMENTS WHERE DEPT_ID = %s",
                (job_row['DEPT_ID'],)
            )
            d = cursor.fetchone()
            if d:
                dept_name = d.get('DEPT_NAME', 'N/A')

        if job_row.get('DESIGNATION_ID'):
            cursor.execute(
                "SELECT TITLE FROM EMP_NRM_DESIGNATIONS WHERE DESIGNATION_ID = %s",
                (job_row['DESIGNATION_ID'],)
            )
            d = cursor.fetchone()
            if d:
                desig_name = d.get('TITLE', 'N/A')

        # ── FALLBACK: if personal/job data missing, pull from EMPLOYEE_REGISTRATIONS ──
        # This covers employees registered via the admin registration form
        if phone == 'N/A' or dept_name == 'N/A' or desig_name == 'N/A':
            cursor.execute("""
                SELECT FIRST_NAME, LAST_NAME, FULL_NAME, PHONE,
                       DEPARTMENT_NAME, DESIGNATION_TITLE,
                       SALARY, JOINING_DATE
                FROM EMPLOYEE_REGISTRATIONS
                WHERE EMPLOYEE_ID = %s
            """, (employee_id,))
            reg_row = cursor.fetchone() or {}

            if reg_row:
                # Fill in missing personal details
                if phone == 'N/A':
                    phone = reg_row.get('PHONE') or 'N/A'

                if full_name == 'N/A' or not full_name.strip():
                    reg_first = reg_row.get('FIRST_NAME') or ''
                    reg_last  = reg_row.get('LAST_NAME')  or ''
                    full_name = f"{reg_first} {reg_last}".strip() or reg_row.get('FULL_NAME', 'N/A')

                # Fill in missing dept/designation
                if dept_name == 'N/A':
                    dept_name = reg_row.get('DEPARTMENT_NAME') or 'N/A'
                if desig_name == 'N/A':
                    desig_name = reg_row.get('DESIGNATION_TITLE') or 'N/A'

                # Fill in salary from EMPLOYEE_REGISTRATIONS if still zero
                if basic == 0.00 and reg_row.get('SALARY'):
                    try:
                        basic      = float(str(reg_row['SALARY']).replace(',', '').strip())
                        net_salary = basic
                    except (ValueError, TypeError):
                        pass

        joining_date = emp_row.get('APPLIED_DATE', 'N/A')
        if joining_date and hasattr(joining_date, 'strftime'):
            joining_date = joining_date.strftime('%d %b %Y')

        cursor.close()
        conn.close()

        return {
            'EMPLOYEE_ID':     employee_id,
            'FULL_NAME':       full_name,
            'EMAIL':           email,
            'PHONE':           phone,
            'DESIGNATION':     desig_name,
            'DEPARTMENT':      dept_name,
            'DATE_OF_JOINING': joining_date,
            'BASIC':           basic,
            'HRA':             hra,
            'ALLOWANCES':      allowances,
            'DEDUCTIONS':      deductions,
            'NET_SALARY':      net_salary,
        }

    except Exception as e:
        print(f"[PaySlip Error] fetch_payslip_data: {e}")
        import traceback
        traceback.print_exc()
        return None


# ─────────────────────────────────────────────
#  PDF GENERATOR
# ─────────────────────────────────────────────
BRAND_BLUE   = colors.HexColor("#1a3c6e")
BRAND_ACCENT = colors.HexColor("#2e86de")
LIGHT_GREY   = colors.HexColor("#f5f6fa")
WHITE_COLOR  = colors.white
BLACK_COLOR  = colors.black


def generate_payslip_pdf(emp, month, year):
    buffer = io.BytesIO()

    # A4 page dimensions in points
    PAGE_W, PAGE_H = A4          # 595.27 x 841.89 pt
    L_MARGIN = R_MARGIN = 15 * mm
    T_MARGIN = B_MARGIN = 12 * mm

    # ─────────────────────────────────────────────────────────
    # Canvas callback — draws LOGO (top-left) & STAMP (bottom-right)
    # on every page.
    # ─────────────────────────────────────────────────────────
    def draw_images(canvas_obj, doc_obj):
        canvas_obj.saveState()

        # ── Logo — top-left ──────────────────────────────────
        if os.path.exists(LOGO_PATH):
            logo_w = 28 * mm
            logo_h = 28 * mm
            x = L_MARGIN
            y = PAGE_H - T_MARGIN - logo_h   # pin to top edge
            canvas_obj.drawImage(
                LOGO_PATH, x, y,
                width=logo_w, height=logo_h,
                preserveAspectRatio=True,
                mask='auto'                  # respects PNG transparency
            )

        canvas_obj.restoreState()

    # ─────────────────────────────────────────────────────────
    # Build the document
    # ─────────────────────────────────────────────────────────
    doc = SimpleDocTemplate(
        buffer, pagesize=A4,
        rightMargin=R_MARGIN, leftMargin=L_MARGIN,
        topMargin=T_MARGIN,   bottomMargin=B_MARGIN
    )

    story = []

    # Paragraph style helper
    def ps(name, size, color, font="Helvetica", align=TA_LEFT, leading=None):
        kwargs = dict(fontSize=size, textColor=color, fontName=font, alignment=align)
        if leading:
            kwargs['leading'] = leading
        return ParagraphStyle(name, **kwargs)

    hdr_s    = ps("hdr",    22, WHITE_COLOR, "Helvetica-Bold", TA_CENTER, 28)
    sub_s    = ps("sub",    9,  WHITE_COLOR, "Helvetica",      TA_CENTER, 14)
    ttl_s    = ps("ttl",    13, WHITE_COLOR, "Helvetica-Bold", TA_CENTER)
    lbl_s    = ps("lbl",    9,  BRAND_BLUE,  "Helvetica-Bold")
    val_s    = ps("val",    9,  BLACK_COLOR, "Helvetica")
    chead_s  = ps("chead",  9,  WHITE_COLOR, "Helvetica-Bold", TA_CENTER)
    rlbl_s   = ps("rlbl",   9,  BRAND_BLUE,  "Helvetica-Bold")
    rval_s   = ps("rval",   9,  BLACK_COLOR, "Helvetica",      TA_RIGHT)
    tlbl_s   = ps("tlbl",   10, WHITE_COLOR, "Helvetica-Bold")
    tval_s   = ps("tval",   10, WHITE_COLOR, "Helvetica-Bold", TA_RIGHT)
    net_s    = ps("net",    14, WHITE_COLOR, "Helvetica-Bold", TA_CENTER)
    netsub_s = ps("netsub",  9, WHITE_COLOR, "Helvetica",      TA_CENTER)
    ftr_s    = ps("ftr",     8, colors.grey, "Helvetica",      TA_CENTER)
    note_s   = ps("note",    8, colors.HexColor("#555"), "Helvetica-Oblique", TA_CENTER)

    def tbl(data, widths, styles_list):
        t = Table(data, colWidths=widths)
        t.setStyle(TableStyle(styles_list))
        return t

    # ── Spacer so content clears the logo at the top ─────────
    # Logo is 28 mm tall; add matching top padding before the banner.
    story.append(Spacer(1, 28 * mm))

    # ── Company banner ────────────────────────────────────────
    story.append(tbl(
        [[Paragraph(f'<font size="22"><b>{COMPANY_NAME}</b></font>', hdr_s)]],
        [180 * mm],
        [("BACKGROUND",(0,0),(-1,-1),BRAND_BLUE),
         ("TOPPADDING",(0,0),(-1,-1),12),("BOTTOMPADDING",(0,0),(-1,-1),4),
         ("LEFTPADDING",(0,0),(-1,-1),10)]
    ))

    story.append(tbl(
        [[Paragraph(COMPANY_ADDRESS, sub_s),
          Paragraph(f'{COMPANY_EMAIL}  |  {COMPANY_PHONE}', sub_s)]],
        [90 * mm, 90 * mm],
        [("BACKGROUND",(0,0),(-1,-1),BRAND_BLUE),
         ("TOPPADDING",(0,0),(-1,-1),2),("BOTTOMPADDING",(0,0),(-1,-1),10),
         ("LEFTPADDING",(0,0),(-1,-1),10)]
    ))
    story.append(Spacer(1, 4 * mm))

    # ── Title bar ─────────────────────────────────────────────
    story.append(tbl(
        [[Paragraph(f'SALARY SLIP \u2014 {month.upper()} {year}', ttl_s)]],
        [180 * mm],
        [("BACKGROUND",(0,0),(-1,-1),BRAND_ACCENT),
         ("TOPPADDING",(0,0),(-1,-1),6),("BOTTOMPADDING",(0,0),(-1,-1),6)]
    ))
    story.append(Spacer(1, 5 * mm))

    # ── Employee details ──────────────────────────────────────
    def lv(lbl, val):
        return [Paragraph(lbl, lbl_s), Paragraph(str(val), val_s)]

    rows_data = [
        lv("Employee ID",     emp['EMPLOYEE_ID']),
        lv("Employee Name",   emp['FULL_NAME']),
        lv("Designation",     emp['DESIGNATION']),
        lv("Department",      emp['DEPARTMENT']),
        lv("Date of Joining", emp['DATE_OF_JOINING']),
        lv("Email",           emp['EMAIL']),
        lv("Phone",           emp['PHONE']),
        lv("Pay Period",      f"{month} {year}"),
    ]
    paired = []
    for i in range(0, len(rows_data), 2):
        left  = rows_data[i]
        right = rows_data[i + 1] if i + 1 < len(rows_data) else ["", ""]
        paired.append(left + [""] + right)

    story.append(tbl(paired, [38*mm, 50*mm, 6*mm, 38*mm, 48*mm], [
        ("ROWBACKGROUNDS",(0,0),(-1,-1),[LIGHT_GREY, WHITE_COLOR]),
        ("BOX",(0,0),(-1,-1),0.5,colors.HexColor("#dde3ec")),
        ("INNERGRID",(0,0),(-1,-1),0.3,colors.HexColor("#dde3ec")),
        ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
        ("LEFTPADDING",(0,0),(-1,-1),8),("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ]))
    story.append(Spacer(1, 5 * mm))

    # ── Salary table ──────────────────────────────────────────
    basic      = emp['BASIC']
    hra        = emp['HRA']
    allowances = emp['ALLOWANCES']
    deductions = emp['DEDUCTIONS']
    net_salary = emp['NET_SALARY']

    def fmt(v):
        return f"Rs. {v:,.2f}"

    sal_data = [
        [Paragraph("EARNINGS",   chead_s), Paragraph("AMOUNT", chead_s),
         Paragraph("DEDUCTIONS", chead_s), Paragraph("AMOUNT", chead_s)],
        [Paragraph("Basic Salary",         rlbl_s), Paragraph(fmt(basic),      rval_s),
         Paragraph("Provident Fund (PF)",  rlbl_s), Paragraph("Nil",           rval_s)],
        [Paragraph("House Rent Allowance", rlbl_s), Paragraph(fmt(hra),        rval_s),
         Paragraph("Professional Tax",     rlbl_s), Paragraph("Nil",           rval_s)],
        [Paragraph("Special Allowances",   rlbl_s), Paragraph(fmt(allowances), rval_s),
         Paragraph("TDS",                  rlbl_s), Paragraph("Nil",           rval_s)],
        [Paragraph("Other Allowances",     rlbl_s), Paragraph("Nil",           rval_s),
         Paragraph("Other Deductions",     rlbl_s), Paragraph(fmt(deductions), rval_s)],
        [Paragraph("GROSS EARNINGS",  tlbl_s), Paragraph(fmt(basic+hra+allowances), tval_s),
         Paragraph("TOTAL DEDUCTIONS", tlbl_s), Paragraph(fmt(deductions),          tval_s)],
    ]

    story.append(tbl(sal_data, [60*mm, 30*mm, 60*mm, 30*mm], [
        ("BACKGROUND",(0,0),(-1,0),BRAND_BLUE),
        ("TEXTCOLOR",(0,0),(-1,0),WHITE_COLOR),
        ("TOPPADDING",(0,0),(-1,0),7),("BOTTOMPADDING",(0,0),(-1,0),7),
        ("ROWBACKGROUNDS",(0,1),(-1,-2),[WHITE_COLOR,LIGHT_GREY]),
        ("TOPPADDING",(0,1),(-1,-2),5),("BOTTOMPADDING",(0,1),(-1,-2),5),
        ("BACKGROUND",(0,-1),(-1,-1),BRAND_ACCENT),
        ("TEXTCOLOR",(0,-1),(-1,-1),WHITE_COLOR),
        ("TOPPADDING",(0,-1),(-1,-1),7),("BOTTOMPADDING",(0,-1),(-1,-1),7),
        ("BOX",(0,0),(-1,-1),0.5,colors.HexColor("#dde3ec")),
        ("INNERGRID",(0,0),(-1,-1),0.3,colors.HexColor("#dde3ec")),
        ("LEFTPADDING",(0,0),(-1,-1),8),("RIGHTPADDING",(0,0),(-1,-1),8),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ]))
    story.append(Spacer(1, 5 * mm))

    # ── Net pay box ───────────────────────────────────────────
    story.append(tbl(
        [[Paragraph("NET SALARY PAYABLE", netsub_s), Paragraph(fmt(net_salary), net_s)]],
        [90*mm, 90*mm],
        [("BACKGROUND",(0,0),(-1,-1),BRAND_BLUE),
         ("TOPPADDING",(0,0),(-1,-1),10),("BOTTOMPADDING",(0,0),(-1,-1),10),
         ("LEFTPADDING",(0,0),(-1,-1),15),("VALIGN",(0,0),(-1,-1),"MIDDLE")]
    ))
    story.append(Spacer(1, 8 * mm))

    # ── Signature footer ──────────────────────────────────────
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#dde3ec")))
    story.append(Spacer(1, 3 * mm))
    story.append(tbl(
        [[Paragraph("Employee Signature", ftr_s), Paragraph("", ftr_s),
          Paragraph("Authorised Signatory", ftr_s)]],
        [60*mm, 60*mm, 60*mm],
        [("TOPPADDING",(0,0),(-1,-1),20),("BOTTOMPADDING",(0,0),(-1,-1),5),
         ("LINEABOVE",(0,0),(0,0),0.5,colors.grey),
         ("LINEABOVE",(2,0),(2,0),0.5,colors.grey),
         ("ALIGN",(0,0),(-1,-1),"CENTER")]
    ))
    story.append(Spacer(1, 3 * mm))

    gen_time = datetime.now().strftime("%d %b %Y %I:%M %p")
    story.append(Paragraph(
        f"Computer-generated pay slip. No physical signature required. | Generated on {gen_time}",
        note_s
    ))

    # ── Build — pass canvas callbacks so images are drawn ─────
    doc.build(
        story,
        onFirstPage=draw_images,    # logo + stamp on first page
        onLaterPages=draw_images    # also on subsequent pages (if any)
    )

    buffer.seek(0)
    return buffer.read()


# ─────────────────────────────────────────────
#  PAY SLIP ROUTES
# ─────────────────────────────────────────────

@app.route("/pay-slip")
def pay_slip_page():
    """Standalone pay slip page — employee must be logged in."""
    if session.get("login_type") != "employee":
        flash("Please login as employee first", "error")
        return redirect(url_for("home"))

    employee_id = session.get("employee_id")
    if not employee_id:
        flash("Session expired. Please login again.", "error")
        return redirect(url_for("home"))

    emp = fetch_payslip_data(employee_id)

    festival_today = None
    try:
        conn2 = get_db_connection()
        if conn2:
            cur2 = conn2.cursor(DICT_CURSOR)
            today = datetime.now().date()
            cur2.execute(
                "SELECT FESTIVAL_NAME FROM EMP_NRM_FESTIVALS WHERE TRUNC(FESTIVAL_DATE) = TRUNC(%s)",
                (today,)
            )
            frow = cur2.fetchone()
            if frow:
                festival_today = frow['FESTIVAL_NAME']
            cur2.close()
            conn2.close()
    except Exception:
        pass

    return render_template(
        "pay-slip.html",
        emp=emp,
        employee_id=employee_id,
        Employee_name=session.get("employee_name", "Employee"),
        profile_pic=session.get("profile_pic", "profile_photo/defaultpicture.jpg"),
        reg_id=employee_id,
        festival_today=festival_today,
    )


@app.route("/generate_payslip", methods=["POST"])
def generate_payslip():
    """Generate and return PDF pay slip for the logged-in employee."""
    if session.get("login_type") != "employee":
        return jsonify({"error": "Not authorised. Please login."}), 403

    employee_id = session.get("employee_id", "").strip()
    if not employee_id:
        return jsonify({"error": "Session expired. Please login again."}), 400

    month = request.form.get("month", datetime.now().strftime("%B"))
    year  = int(request.form.get("year", datetime.now().year))

    emp = fetch_payslip_data(employee_id)
    if not emp:
        return jsonify({"error": "Salary data not found. Please contact HR."}), 404

    pdf_bytes = generate_payslip_pdf(emp, month, year)
    filename  = f"PaySlip_{employee_id}_{month}_{year}.pdf"

    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=filename
    )

@app.route("/Kapardhi_Mashreq")
def kapardhi_mashreq():
    """
    Export Bill guidance page
    """
    return render_template("Kapardhi_Mashreq.html")


# ==================== BLOGGER PROXY ROUTES ====================

@app.route("/blogger")
def blogger():
    """
    Main blogger page
    Renders the HTML template - all data loaded via AJAX calls to microservice
    """
    return render_template("blogger.html", is_admin=False)


# Backward-compatible endpoint alias for older templates still using url_for('blogger_page').
app.add_url_rule("/blogger", endpoint="blogger_page", view_func=blogger)


def has_blogger_admin_access():
    """Allow blog admin actions for admin user sessions, employee sessions, or recent admin verification sessions."""
    if _has_employee_admin_access():
        blogger_admin_logger.info(
            "Auth granted via CH25006 employee access | session=%s",
            get_blogger_admin_session_snapshot(),
        )
        return True

    if session.get("usertype", "").lower() in {"admin", "administrator"}:
        blogger_admin_logger.info(
            "Auth granted via session usertype | session=%s",
            get_blogger_admin_session_snapshot(),
        )
        return True

    user_id = session.get("user_id")
    session_user = str(session.get("user", "")).strip()

    if session.get("login_type") == "user" and (user_id or session_user):
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor(DICT_CURSOR)
                cursor.execute(
                    """
                    SELECT USERTYPE
                    FROM NRM_USERS
                    WHERE ID = %s
                       OR LOWER(EMAIL) = LOWER(%s)
                       OR PHONE = %s
                    LIMIT 1
                    """,
                    (user_id, session_user, session_user),
                )
                user_row = cursor.fetchone()
                cursor.close()
                conn.close()

                resolved_usertype = str((user_row or {}).get("USERTYPE", "")).strip().lower()
                if resolved_usertype in {"admin", "administrator"}:
                    session["usertype"] = resolved_usertype
                    blogger_admin_logger.info(
                        "Auth granted via NRM_USERS lookup | resolved_usertype=%s session=%s",
                        resolved_usertype,
                        get_blogger_admin_session_snapshot(),
                    )
                    return True
        except Exception as e:
            blogger_admin_logger.exception(
                "Auth lookup failed | session=%s",
                get_blogger_admin_session_snapshot(),
            )
            print(f"❌ Blogger admin auth lookup failed: {e}")

    if not session.get("admin_verified"):
        blogger_admin_logger.warning(
            "Auth denied | admin_verified missing | session=%s",
            get_blogger_admin_session_snapshot(),
        )
        return False

    verified_at_str = session.get("verified_at")
    if not verified_at_str:
        blogger_admin_logger.warning(
            "Auth denied | verified_at missing | session=%s",
            get_blogger_admin_session_snapshot(),
        )
        return False

    try:
        verified_at = datetime.fromisoformat(verified_at_str)
    except ValueError:
        blogger_admin_logger.warning(
            "Auth denied | invalid verified_at format | session=%s",
            get_blogger_admin_session_snapshot(),
        )
        session.pop("admin_verified", None)
        session.pop("verified_user_id", None)
        session.pop("verified_email", None)
        session.pop("verified_at", None)
        return False

    if (datetime.now() - verified_at).total_seconds() > 600:
        blogger_admin_logger.warning(
            "Auth denied | admin verification expired | session=%s",
            get_blogger_admin_session_snapshot(),
        )
        session.pop("admin_verified", None)
        session.pop("verified_user_id", None)
        session.pop("verified_email", None)
        session.pop("verified_at", None)
        return False

    blogger_admin_logger.info(
        "Auth granted via recent admin verification | session=%s",
        get_blogger_admin_session_snapshot(),
    )
    return True


def _proxy_create_blog_post_impl():
    """Shared implementation for blog post creation routes."""
    is_form_submission = bool(request.form) and not request.is_json

    if not has_blogger_admin_access():
        blogger_admin_logger.warning(
            "Create post denied | unauthorized admin access | session=%s",
            get_blogger_admin_session_snapshot(),
        )
        if is_form_submission:
            flash("Admin access required to publish posts.", "error")
            return redirect(url_for("admin_blogger"))
        return jsonify({"success": False, "message": "Unauthorized"}), 403

    has_any_blog_session = bool(
        session.get("login_type")
        or session.get("user_id")
        or session.get("employee_id")
        or session.get("admin_verified")
    )

    if not has_any_blog_session:
        blogger_admin_logger.warning(
            "Create post denied before proxy | session=%s",
            get_blogger_admin_session_snapshot(),
        )
        if is_form_submission:
            flash("Your current session is not allowed to publish posts.", "error")
            return redirect(url_for("admin_blogger"))
        return jsonify({"success": False, "message": "Unauthorized"}), 403

    try:
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            data = request.form.to_dict()

        if not isinstance(data, dict) or not data:
            blogger_admin_logger.warning(
                "Create post received invalid payload | payload=%s form=%s session=%s",
                data,
                request.form.to_dict(),
                get_blogger_admin_session_snapshot(),
            )
            if is_form_submission:
                flash("Invalid request payload.", "error")
                return redirect(url_for("admin_blogger"))
            return jsonify({"success": False, "message": "Invalid request payload"}), 400

        if data.get("content_base64") and not data.get("content"):
            try:
                data["content"] = base64.b64decode(data.pop("content_base64")).decode("utf-8")
            except Exception:
                blogger_admin_logger.exception(
                    "Create post content decode failed | session=%s",
                    get_blogger_admin_session_snapshot(),
                )
                if is_form_submission:
                    flash("Invalid post content.", "error")
                    return redirect(url_for("admin_blogger"))
                return jsonify({"success": False, "message": "Invalid post content"}), 400

        blogger_admin_logger.info(
            "Create post request accepted | title=%s publish_date=%s is_published=%s is_locked=%s tags=%s remote_addr=%s session=%s",
            data.get("title", ""),
            data.get("publish_date", ""),
            data.get("is_published", ""),
            data.get("is_locked", ""),
            data.get("tags", ""),
            request.headers.get("X-Forwarded-For", request.remote_addr),
            get_blogger_admin_session_snapshot(),
        )

        if 'author' not in data:
            data['author'] = session.get('employee_name', 'Admin')

        headers = {
            "X-Login-Type": session.get("login_type", ""),
            "X-User-Type": session.get("usertype", ""),
            "X-Employee-ID": str(session.get("employee_id", session.get("verified_user_id", ""))),
            "X-Employee-Name": session.get("employee_name", session.get("verified_email", "Admin"))
        }

        response = requests.post(
            f"{BLOGGER_SERVICE_URL}/blogger/admin/new_post",
            json=data,
            headers=headers,
            timeout=15
        )
        blogger_admin_logger.info(
            "Create post proxy response | status=%s body=%s",
            response.status_code,
            response.text[:1000],
        )

        response_data = response.json()

        if is_form_submission:
            if response.ok and response_data.get("success"):
                flash("Blog post created successfully!", "success")
            else:
                flash(
                    f"Failed to create post: {response_data.get('message', 'Unknown error')}",
                    "error",
                )
            return redirect(url_for("admin_blogger"))

        return jsonify(response_data), response.status_code

    except Exception as e:
        blogger_admin_logger.exception(
            "Create post proxy failed | session=%s",
            get_blogger_admin_session_snapshot(),
        )
        print(f"❌ Blogger proxy error: {e}")
        if is_form_submission:
            flash("Request failed while submitting the post.", "error")
            return redirect(url_for("admin_blogger"))
        return jsonify({"success": False, "message": "Proxy error"}), 500


@app.route("/admin/blogger", methods=["GET", "POST"])
def admin_blogger():
    """
    Admin route alias for blogger management.

    This is kept for compatibility with any admin UI that expects /admin/blogger.
    Page access is allowed here; write actions are still protected by the API routes.
    """
    is_admin = has_blogger_admin_access()
    if not is_admin:
        flash("Access denied.", "error")
        return redirect(url_for("home"), code=303)

    if request.method == "POST":
        return _proxy_create_blog_post_impl()

    blogger_admin_logger.info(
        "Admin blogger page rendered | is_admin=%s session=%s",
        is_admin,
        get_blogger_admin_session_snapshot(),
    )
    return render_template(
        "blogger_admin.html",
        is_admin=is_admin,
        blogger_service_url=BLOGGER_SERVICE_URL,
    )


@app.route("/blogger/posts", methods=["GET"])
def proxy_get_blog_posts():
    """
    Proxy to blogger microservice - get all blog posts
    Passes through query parameters (year, month, locked_only)
    """
    try:
        params = request.args.to_dict()
        sess = requests.Session()
        sess.trust_env = False
        os.environ["no_proxy"] = INTERNAL_NO_PROXY
        response = sess.get(
            f"{BLOGGER_SERVICE_URL}/blogger/posts",
            params=params,
            headers={"Accept-Encoding": "identity"},
            proxies={"http": None, "https": None},
            timeout=15,
        )
        try:
            payload = response.json()
        except Exception:
            payload = {"success": False, "message": "Non-JSON response from blogger service"}
        return jsonify(payload), response.status_code

    except requests.exceptions.Timeout:
        print("❌ Blogger service timeout")
        return jsonify({"success": False, "message": "Service timeout"}), 504

    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to blogger service")
        return jsonify({"success": False, "message": "Service unavailable"}), 503

    except Exception as e:
        print(f"❌ Blogger proxy error: {e}")
        return jsonify({"success": False, "message": "Proxy error"}), 500


@app.route("/blogger/post/<int:post_id>", methods=["GET"])
def proxy_get_blog_post(post_id):
    """
    Proxy to blogger microservice - get single blog post
    Forwards user session info to check access permissions
    """
    try:
        # Prepare headers with session info
        headers = {
            "X-Login-Type": session.get("login_type", ""),
            "X-User-ID": str(session.get("user_id", "")),
            "X-Employee-ID": str(session.get("employee_id", ""))
        }
        
        # Call blogger microservice
        response = requests.get(
            f"{BLOGGER_SERVICE_URL}/blogger/post/{post_id}",
            headers=headers,
            timeout=10
        )
        
        return jsonify(response.json()), response.status_code
        
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "message": "Service timeout"}), 504
        
    except requests.exceptions.ConnectionError:
        return jsonify({"success": False, "message": "Service unavailable"}), 503
        
    except Exception as e:
        print(f"❌ Blogger proxy error: {e}")
        return jsonify({"success": False, "message": "Proxy error"}), 500


@app.route("/blogger/like/<int:post_id>", methods=["POST"])
def proxy_like_blog_post(post_id):
    """
    Proxy to blogger microservice - like a post.
    Forwards session info so authenticated users are deduplicated correctly.
    """
    try:
        headers = {
            "X-Login-Type": session.get("login_type", ""),
            "X-User-ID": str(session.get("user_id", "")),
            "X-Employee-ID": str(session.get("employee_id", ""))
        }
        sess = requests.Session()
        sess.trust_env = False
        headers["Accept-Encoding"] = "identity"
        response = sess.post(
            f"{BLOGGER_SERVICE_URL}/blogger/like/{post_id}",
            headers=headers,
            proxies={"http": None, "https": None},
            timeout=10,
        )
        try:
            payload = response.json()
        except Exception:
            payload = {"success": False, "message": "Non-JSON response"}
        return jsonify(payload), response.status_code

    except requests.exceptions.Timeout:
        return jsonify({"success": False, "message": "Service timeout"}), 504

    except requests.exceptions.ConnectionError:
        return jsonify({"success": False, "message": "Service unavailable"}), 503

    except Exception as e:
        print(f"❌ Blogger proxy error: {e}")
        return jsonify({"success": False, "message": "Proxy error"}), 500


@app.route("/blogger/subscribe", methods=["POST"])
def proxy_subscribe_to_blog():
    """
    Proxy to blogger microservice - email subscription
    """
    try:
        data = request.get_json()
        sess = requests.Session()
        sess.trust_env = False
        response = sess.post(
            f"{BLOGGER_SERVICE_URL}/blogger/subscribe",
            json=data,
            headers={"Accept-Encoding": "identity"},
            proxies={"http": None, "https": None},
            timeout=10,
        )
        try:
            payload = response.json()
        except Exception:
            payload = {"success": False, "message": "Non-JSON response"}
        return jsonify(payload), response.status_code

    except requests.exceptions.Timeout:
        return jsonify({"success": False, "message": "Service timeout"}), 504

    except requests.exceptions.ConnectionError:
        return jsonify({"success": False, "message": "Service unavailable"}), 503

    except Exception as e:
        print(f"❌ Blogger proxy error: {e}")
        return jsonify({"success": False, "message": "Proxy error"}), 500


@app.route("/blogger/unsubscribe", methods=["POST"])
def proxy_unsubscribe_from_blog():
    """
    Proxy to blogger microservice - unsubscribe
    """
    try:
        data = request.get_json()
        sess = requests.Session()
        sess.trust_env = False
        response = sess.post(
            f"{BLOGGER_SERVICE_URL}/blogger/unsubscribe",
            json=data,
            headers={"Accept-Encoding": "identity"},
            proxies={"http": None, "https": None},
            timeout=10,
        )
        try:
            payload = response.json()
        except Exception:
            payload = {"success": False, "message": "Non-JSON response"}
        return jsonify(payload), response.status_code

    except Exception as e:
        print(f"❌ Blogger proxy error: {e}")
        return jsonify({"success": False, "message": "Proxy error"}), 500


@app.route("/blogger/subscriber_count", methods=["GET"])
def proxy_get_subscriber_count():
    """
    Proxy to blogger microservice - get subscriber count
    """
    try:
        sess = requests.Session()
        sess.trust_env = False
        response = sess.get(
            f"{BLOGGER_SERVICE_URL}/blogger/subscriber_count",
            headers={"Accept-Encoding": "identity"},
            proxies={"http": None, "https": None},
            timeout=5,
        )
        try:
            payload = response.json()
        except Exception:
            payload = {"success": True, "count": 0}
        return jsonify(payload), response.status_code

    except Exception as e:
        print(f"❌ Blogger proxy error: {e}")
        return jsonify({"success": True, "count": 0}), 200


@app.route("/blogger/stats", methods=["GET"])
def proxy_get_blog_stats():
    """
    Proxy to blogger microservice - get blog stats
    """
    try:
        sess = requests.Session()
        sess.trust_env = False
        response = sess.get(
            f"{BLOGGER_SERVICE_URL}/blogger/stats",
            headers={"Accept-Encoding": "identity"},
            proxies={"http": None, "https": None},
            timeout=5,
        )
        try:
            payload = response.json()
        except Exception:
            payload = {"success": True, "posts": 0, "reads": 0, "likes": 0}
        return jsonify(payload), response.status_code

    except Exception as e:
        print(f"❌ Blogger proxy error: {e}")
        return jsonify({"success": True, "posts": 0, "reads": 0, "likes": 0}), 200


@app.route("/check_session", methods=["GET"])
def check_session():
    """
    Check if user is logged in and their login type
    Used by blogger page to determine access to locked posts
    
    This stays in app.py since it's reading local session
    """
    return jsonify({
        "logged_in": "login_type" in session,
        "login_type": session.get("login_type", ""),
        "user_id": session.get("user_id", ""),
        "employee_id": session.get("employee_id", ""),
        "usertype": session.get("usertype", ""),
        "user": session.get("user", ""),
        "admin_verified": session.get("admin_verified", False)
    })


# ==================== ADMIN PROXY ROUTES (Optional) ====================

@app.route("/blogger/admin/new_post", methods=["POST"])
def proxy_create_blog_post():
    """
    Proxy to blogger microservice - create new post
    Requires employee login
    """
    return _proxy_create_blog_post_impl()


@app.route("/blogger/admin/edit_post/<int:post_id>", methods=["PUT"])
def proxy_edit_blog_post(post_id):
    """
    Proxy to blogger microservice - edit post
    Requires employee login
    """
    if not has_blogger_admin_access():
        return jsonify({"success": False, "message": "Unauthorized"}), 403
    
    try:
        data = request.get_json()
        
        headers = {
            "X-Login-Type": "employee",
            "X-Employee-ID": str(session.get("employee_id", session.get("verified_user_id", "")))
        }
        
        response = requests.put(
            f"{BLOGGER_SERVICE_URL}/blogger/admin/edit_post/{post_id}",
            json=data,
            headers=headers,
            timeout=15
        )
        
        return jsonify(response.json()), response.status_code
        
    except Exception as e:
        print(f"❌ Blogger proxy error: {e}")
        return jsonify({"success": False, "message": "Proxy error"}), 500


@app.route("/blogger/admin/delete_post/<int:post_id>", methods=["DELETE"])
def proxy_delete_blog_post(post_id):
    """
    Proxy to blogger microservice - delete post
    Requires employee login
    """
    if not has_blogger_admin_access():
        return jsonify({"success": False, "message": "Unauthorized"}), 403
    
    try:
        headers = {
            "X-Login-Type": "employee",
            "X-Employee-ID": str(session.get("employee_id", session.get("verified_user_id", "")))
        }
        
        response = requests.delete(
            f"{BLOGGER_SERVICE_URL}/blogger/admin/delete_post/{post_id}",
            headers=headers,
            timeout=10
        )
        
        return jsonify(response.json()), response.status_code
        
    except Exception as e:
        print(f"❌ Blogger proxy error: {e}")
        return jsonify({"success": False, "message": "Proxy error"}), 500

# ══════════════════════════════════════════════════════════════
#  ADD RESOURCES — /admin/add-resources
# ══════════════════════════════════════════════════════════════

@app.route("/admin/add-resources", methods=["GET", "POST"])
def add_resources():
    if session.get("login_type") not in ("user", "employee"):
        return redirect(url_for("home"))
    if session.get("usertype", "").lower() not in ("admin", "administrator"):
        flash("Admin access required.", "danger")
        return redirect(url_for("resources"))
    return render_template("add_resources.html")

# ══════════════════════════════════════════════════════════════
#  ADMIN BATCH SCHEDULE — /admin/batch-schedule
# ══════════════════════════════════════════════════════════════

@app.route("/admin/batch-schedule", methods=["GET", "POST"])
def batch_schedule():
    if session.get("login_type") not in ("user", "employee"):
        return redirect(url_for("home"))
    if session.get("usertype", "").lower() not in ("admin", "administrator"):
        flash("Admin access required.", "danger")
        return redirect(url_for("resources"))

    courses = []
    conn = get_db_connection()
    if conn:
        cur = conn.cursor(DICT_CURSOR)
        try:
            cur.execute("SELECT id, course_name FROM nrm_courses ORDER BY course_name")
            courses = [{"ID": r["ID"], "COURSE_NAME": r["COURSE_NAME"]} for r in cur.fetchall()]
        except Exception as e:
            print(f"❌ batch_schedule: load courses error: {e}")
        finally:
            cur.close()
            conn.close()

    if request.method == "POST":
        course_id   = request.form.get("course_id", "0").strip()
        batch_type  = request.form.get("batch_type",  "regular").strip()
        language    = request.form.get("language",    "Telugu").strip()
        start_date  = request.form.get("start_date",  "").strip()
        end_date    = request.form.get("end_date",    "").strip() or None
        status      = request.form.get("status",      "upcoming").strip()
        notes       = request.form.get("notes",       "").strip() or None

        if not start_date:
            flash("Start date is required.", "danger")
            return redirect(url_for("batch_schedule"))

        if course_id == "0":
            flash("Please select an existing course. Course creation is handled only in Manage Courses.", "danger")
            return redirect(url_for("batch_schedule"))

        conn2 = get_db_connection()
        if conn2:
            cur2 = conn2.cursor()
            try:
                selected_course_id = int(course_id)

                cur2.execute("""
                    INSERT INTO NRM_BATCH_SCHEDULE
                        (COURSE_ID, DISPLAY_NAME, BATCH_TYPE, LANGUAGE,
                         START_DATE, END_DATE, STATUS, NOTES, CREATED_BY, CREATED_AT)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                """, (
                    selected_course_id,
                    None,
                    batch_type, language, start_date, end_date,
                    status, notes, session.get("user_id")
                ))
                conn2.commit()
                cache_delete("batches:current")
                flash("Batch scheduled successfully. Home page will reflect shortly.", "success")
            except Exception as e:
                conn2.rollback()
                print(f"❌ batch_schedule insert error: {e}")
                traceback.print_exc()
                flash("Error saving. Please check the Snowflake table exists (see SQL setup script).", "danger")
            finally:
                cur2.close()
                conn2.close()
        return redirect(url_for("batch_schedule"))

    # GET — load list
    batches = []
    conn3 = get_db_connection()
    if conn3:
        cur3 = conn3.cursor(DICT_CURSOR)
        try:
            cur3.execute("""
                SELECT
                    b.ID,
                    COALESCE(b.DISPLAY_NAME, c.COURSE_NAME) AS DISPLAY_NAME,
                    b.BATCH_TYPE,
                    b.LANGUAGE,
                    b.START_DATE,
                    b.END_DATE,
                    b.STATUS,
                    b.NOTES
                FROM NRM_BATCH_SCHEDULE b
                LEFT JOIN NRM_COURSES c ON b.COURSE_ID = c.ID
                WHERE b.STATUS != 'deleted'
                ORDER BY
                    CASE b.STATUS
                        WHEN 'current'  THEN 1
                        WHEN 'upcoming' THEN 2
                        ELSE 3
                    END,
                    b.START_DATE DESC
            """)
            batches = cur3.fetchall()
        except Exception as e:
            print(f"❌ batch_schedule: load list error: {e}")
            flash("Could not load batch list. Run the Snowflake setup SQL first.", "warning")
        finally:
            cur3.close()
            conn3.close()

    return render_template("batch-schedule.html",
                           courses=courses,
                           batches=batches)


@app.route("/admin/batch-schedule/<int:batch_id>/action", methods=["POST"])
def batch_schedule_action(batch_id):
    if session.get("usertype", "").lower() not in ("admin", "administrator"):
        return redirect(url_for("resources"))

    action = request.form.get("action", "")

    conn = get_db_connection()
    if conn:
        cur = conn.cursor()
        try:
            if action == "complete":
                cur.execute(
                    "UPDATE NRM_BATCH_SCHEDULE SET STATUS = %s WHERE ID = %s",
                    ("completed", batch_id)
                )
                success_msg = "Batch marked completed."
            elif action == "delete":
                cur.execute(
                    "SELECT COURSE_ID FROM NRM_BATCH_SCHEDULE WHERE ID = %s",
                    (batch_id,)
                )
                batch_row = cur.fetchone()
                linked_course_id = batch_row[0] if batch_row else None

                cur.execute(
                    "DELETE FROM NRM_BATCH_SCHEDULE WHERE ID = %s",
                    (batch_id,)
                )
                success_msg = "Batch deleted from database."

                # If this was the only schedule row for a course and there are no
                # registrations, clean up the orphan course automatically.
                if cur.rowcount > 0 and linked_course_id:
                    try:
                        cur.execute(
                            "SELECT COUNT(*) FROM NRM_BATCH_SCHEDULE WHERE COURSE_ID = %s",
                            (linked_course_id,)
                        )
                        remaining_batches = int((cur.fetchone() or [0])[0] or 0)

                        cur.execute(
                            "SELECT COUNT(*) FROM NRM_REGISTRATIONS WHERE COURSE_ID = %s",
                            (linked_course_id,)
                        )
                        registrations_count = int((cur.fetchone() or [0])[0] or 0)

                        if remaining_batches == 0 and registrations_count == 0:
                            cur.execute("DELETE FROM NRM_COURSES WHERE ID = %s", (linked_course_id,))
                            if cur.rowcount > 0:
                                success_msg += " Linked unused course also removed."
                    except Exception as cleanup_err:
                        print(f"⚠️ orphan course cleanup skipped: {cleanup_err}")
            elif action == "edit":
                edit_display_name = request.form.get("edit_display_name", "").strip()
                edit_batch_type = request.form.get("edit_batch_type", "regular").strip()
                edit_language = request.form.get("edit_language", "Telugu").strip()
                edit_start_date = request.form.get("edit_start_date", "").strip()
                edit_end_date = request.form.get("edit_end_date", "").strip() or None
                edit_status = request.form.get("edit_status", "upcoming").strip()
                edit_notes = request.form.get("edit_notes", "").strip() or None

                if not edit_display_name:
                    flash("Display name is required for edit.", "danger")
                    return redirect(url_for("batch_schedule"))
                if not edit_start_date:
                    flash("Start date is required for edit.", "danger")
                    return redirect(url_for("batch_schedule"))

                cur.execute(
                    """
                    UPDATE NRM_BATCH_SCHEDULE
                    SET DISPLAY_NAME = %s,
                        BATCH_TYPE = %s,
                        LANGUAGE = %s,
                        START_DATE = %s,
                        END_DATE = %s,
                        STATUS = %s,
                        NOTES = %s
                    WHERE ID = %s
                    """,
                    (
                        edit_display_name,
                        edit_batch_type,
                        edit_language,
                        edit_start_date,
                        edit_end_date,
                        edit_status,
                        edit_notes,
                        batch_id,
                    ),
                )
                success_msg = "Batch updated successfully."
            else:
                flash("Invalid batch action.", "danger")
                return redirect(url_for("batch_schedule"))

            if cur.rowcount == 0:
                conn.rollback()
                flash("Batch not found or already removed.", "warning")
                return redirect(url_for("batch_schedule"))

            conn.commit()
            cache_delete("batches:current")
            flash(success_msg, "success")
        except Exception as e:
            conn.rollback()
            print(f"❌ batch action error: {e}")
            flash("Error updating batch.", "danger")
        finally:
            cur.close()
            conn.close()

    return redirect(url_for("batch_schedule"))

        
def _internship_service_base_urls():
    current_host = (request.host_url or "").rstrip("/").lower()
    gateway_candidates = []
    if INTERNSHIP_SERVICE_URL:
        gateway_base = INTERNSHIP_SERVICE_URL.rstrip("/")
        gateway_candidates.append(gateway_base)
        if not gateway_base.endswith("/prod"):
            gateway_candidates.append(f"{gateway_base}/prod")

    candidates = [
        *gateway_candidates,
        INTERNSHIP_SERVICE_URL,
        INTERNSHIP_SERVICE_URL,
        "http://127.0.0.1:5050",
        "http://localhost:5050",
    ]
    normalized = []
    for raw in candidates:
        value = (raw or "").strip().rstrip("/")
        # Avoid recursively proxying back to this same Flask host.
        if value.lower() == current_host:
            continue
        if value and value not in normalized:
            normalized.append(value)
    return normalized

@app.route("/api/internship/apply", methods=["POST"])
@app.route("/internship-apply-proxy", methods=["POST"])
def proxy_internship_apply():
    try:
        print("🔥 Internship API HIT")

        # ✅ FIXED: Use INTERNSHIP-specific variables, not STUDENT ones
        os.environ["NO_PROXY"] = INTERNAL_NO_PROXY
        os.environ["no_proxy"] = INTERNAL_NO_PROXY

        with requests.Session() as s:
            s.trust_env = False
            s.proxies = {"http": None, "https": None}

            service_bases = _internship_service_base_urls()
            last_error = None
            retryable_statuses = {403, 404, 500, 502, 503, 504}

            if request.content_type and "multipart/form-data" in request.content_type:
                form_data = request.form.to_dict()
                buffered_files = {}
                for key in request.files:
                    file = request.files[key]
                    if file and file.filename:
                        file.stream.seek(0)
                        buffered_files[key] = (
                            file.filename,
                            file.stream.read(),
                            file.content_type or "application/octet-stream",
                        )

                print("📦 Form:", form_data)
                print("📁 Files:", list(buffered_files.keys()))

                response = None
                for base_url in service_bases:
                    try:
                        files_payload = {
                            key: (name, content, content_type)
                            for key, (name, content, content_type) in buffered_files.items()
                        }
                        response = s.post(
                            f"{base_url}/api/internship/apply",
                            data=form_data,
                            files=files_payload,
                            timeout=(8, 120),
                        )
                        if response.status_code in retryable_statuses:
                            print(f"⚠️ Internship service {base_url} returned {response.status_code}, trying next target")
                            continue
                        break
                    except requests.RequestException as exc:
                        last_error = exc
                        print(f"⚠️ Internship service unreachable at {base_url}: {exc}")
                        continue
            else:
                data = request.get_json(silent=True) or {}
                response = None
                for base_url in service_bases:
                    try:
                        response = s.post(
                            f"{base_url}/api/internship/apply",
                            json=data,
                            timeout=(8, 120),
                        )
                        if response.status_code in retryable_statuses:
                            print(f"⚠️ Internship service {base_url} returned {response.status_code}, trying next target")
                            continue
                        break
                    except requests.RequestException as exc:
                        last_error = exc
                        print(f"⚠️ Internship service unreachable at {base_url}: {exc}")
                        continue

            if response is None:
                return jsonify({
                    "success": False,
                    "message": "Internship service is unreachable from Flask proxy.",
                    "details": str(last_error) if last_error else "No reachable service target",
                    "targets": service_bases,
                }), 503

        try:
            return jsonify(response.json()), response.status_code
        except:
            return jsonify({
                "success": False,
                "message": "Non-JSON response",
                "raw": response.text[:200]
            }), response.status_code

    except Exception as e:
        print("❌ ERROR:", e)
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/internship/admin/applications", methods=["GET"])
def proxy_internship_admin_applications():
    """Admin-only endpoint: list internship applications directly from Snowflake."""
    if not _has_employee_admin_access():
        return jsonify({"success": False, "message": "Admin access required"}), 403

    conn = None
    cursor = None
    try:
        status_filter = (request.args.get("status") or "").strip().upper()

        conn = get_db_connection()
        if not conn:
            return jsonify({"success": False, "message": "Database connection failed"}), 500

        cursor = conn.cursor(DICT_CURSOR)
        cursor.execute("SELECT * FROM NRM_INTERNSHIP_APPLICATIONS ORDER BY SUBMITTED_AT DESC")
        rows = cursor.fetchall() or []

        applications = []
        for row in rows:
            status = str(row.get("STATUS") or "PENDING").strip().upper()
            if status_filter and status != status_filter:
                continue

            submitted_at = row.get("SUBMITTED_AT")
            submitted_at_value = submitted_at.isoformat() if hasattr(submitted_at, "isoformat") else (submitted_at or "")

            applications.append({
                "intern_id": row.get("INTERN_ID") or "",
                "submitted_at": submitted_at_value,
                "full_name": row.get("FULL_NAME") or "",
                "email": row.get("EMAIL") or "",
                "mobile": row.get("MOBILE") or "",
                "date_of_birth": row.get("DATE_OF_BIRTH") or "",
                "gender": row.get("GENDER") or "",
                "address": row.get("ADDRESS") or "",
                "college_name": row.get("COLLEGE_NAME") or "",
                "branch": row.get("BRANCH") or "",
                "year_of_study": row.get("YEAR_OF_STUDY") or "",
                "graduation_year": row.get("GRADUATION_YEAR") or "",
                "internship_domain": row.get("INTERNSHIP_DOMAIN") or "",
                "internship_duration": row.get("INTERNSHIP_DURATION") or "",
                "start_date": row.get("START_DATE") or "",
                "mode": row.get("MODE") or "",
                "why_chakora": row.get("WHY_CHAKORA") or "",
                "skills": row.get("SKILLS") or "",
                "resume_url": row.get("RESUME_URL") or "",
                "id_card_url": row.get("ID_CARD_URL") or "",
                "noc_url": row.get("NOC_URL") or "",
                "status": status,
            })

        payload = {
            "success": True,
            "applications": applications,
            "count": len(applications),
        }
        return jsonify(payload), 200

    except Exception as e:
        print(f"❌ Internship admin list DB error: {e}")
        return jsonify({"success": False, "message": f"Service error: {str(e)}"}), 500
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception:
            pass


@app.route("/api/internship/admin/select", methods=["POST"])
def proxy_internship_admin_select():
    """Admin-only proxy: finalize internship selection and trigger letter/email workflow."""
    if not _has_employee_admin_access():
        return jsonify({"success": False, "message": "Admin access required"}), 403

    try:
        body = request.get_json(silent=True) or {}
        intern_id = str(body.get("intern_id", "")).strip()
        start_date = str(body.get("start_date", "")).strip()
        end_date = str(body.get("end_date", "")).strip()

        if not intern_id or not start_date or not end_date:
            return jsonify({
                "success": False,
                "message": "intern_id, start_date and end_date are required"
            }), 400

        os.environ["NO_PROXY"] = INTERNAL_NO_PROXY
        os.environ["no_proxy"] = INTERNAL_NO_PROXY

        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            resp = internal_session.post(
                f"{INTERNSHIP_SERVICE_URL}/api/internship/admin/select",
                json=body,
                timeout=40,
                allow_redirects=False,
            )

        try:
            payload = resp.json()
        except ValueError:
            payload = {
                "success": False,
                "message": "Internship service returned non-JSON response",
                "upstream_status": resp.status_code,
                "upstream_preview": (resp.text or "")[:300],
            }
        return jsonify(payload), resp.status_code

    except requests.exceptions.ConnectionError as e:
        print(f"❌ Internship admin select connection error: {e}")
        return jsonify({"success": False, "message": "Internship service is offline"}), 503
    except requests.exceptions.Timeout:
        print("❌ Internship admin select timeout")
        return jsonify({"success": False, "message": "Request timed out"}), 504
    except Exception as e:
        print(f"❌ Internship admin select proxy error: {e}")
        return jsonify({"success": False, "message": f"Service error: {str(e)}"}), 500


@app.route("/api/internship/admin/status", methods=["POST"])
def proxy_internship_admin_status():
    """Admin-only proxy: update internship application status and notify applicant."""
    if not _has_employee_admin_access():
        return jsonify({"success": False, "message": "Admin access required"}), 403

    try:
        body = request.get_json(silent=True) or {}
        intern_id = str(body.get("intern_id", "")).strip()
        status = str(body.get("status", "")).strip().upper()

        if not intern_id or status not in {"APPROVED", "REJECTED"}:
            return jsonify({
                "success": False,
                "message": "intern_id and valid status (APPROVED/REJECTED) are required"
            }), 400

        os.environ["NO_PROXY"] = INTERNAL_NO_PROXY
        os.environ["no_proxy"] = INTERNAL_NO_PROXY

        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            resp = internal_session.post(
                f"{INTERNSHIP_SERVICE_URL}/api/internship/admin/status",
                json=body,
                timeout=40,
                allow_redirects=False,
            )

        try:
            payload = resp.json()
        except ValueError:
            payload = {
                "success": False,
                "message": "Internship service returned non-JSON response",
                "upstream_status": resp.status_code,
                "upstream_preview": (resp.text or "")[:300],
            }
        return jsonify(payload), resp.status_code

    except requests.exceptions.ConnectionError as e:
        print(f"❌ Internship admin status connection error: {e}")
        return jsonify({"success": False, "message": "Internship service is offline"}), 503
    except requests.exceptions.Timeout:
        print("❌ Internship admin status timeout")
        return jsonify({"success": False, "message": "Request timed out"}), 504
    except Exception as e:
        print(f"❌ Internship admin status proxy error: {e}")
        return jsonify({"success": False, "message": f"Service error: {str(e)}"}), 500


@app.route("/api/internship/send-certificate", methods=["POST"])
def proxy_internship_send_certificate():
    """Admin-only proxy: send internship certificate email."""
    if not _has_employee_admin_access():
        return jsonify({"success": False, "message": "Admin access required"}), 403

    try:
        body = request.get_json(silent=True) or {}
        intern_id = str(body.get("intern_id", "")).strip()
        if not intern_id:
            return jsonify({"success": False, "message": "intern_id is required"}), 400

        os.environ["NO_PROXY"] = INTERNAL_NO_PROXY
        os.environ["no_proxy"] = INTERNAL_NO_PROXY

        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            resp = internal_session.post(
                f"{INTERNSHIP_SERVICE_URL}/api/internship/send-certificate",
                json={"intern_id": intern_id},
                timeout=40,
                allow_redirects=False,
            )

        try:
            payload = resp.json()
        except ValueError:
            payload = {
                "success": False,
                "message": "Internship service returned non-JSON response",
                "upstream_status": resp.status_code,
                "upstream_preview": (resp.text or "")[:300],
            }
        return jsonify(payload), resp.status_code

    except requests.exceptions.ConnectionError as e:
        print(f"❌ Internship send-certificate connection error: {e}")
        return jsonify({"success": False, "message": "Internship service is offline"}), 503
    except requests.exceptions.Timeout:
        print("❌ Internship send-certificate timeout")
        return jsonify({"success": False, "message": "Request timed out"}), 504
    except Exception as e:
        print(f"❌ Internship send-certificate proxy error: {e}")
        return jsonify({"success": False, "message": f"Service error: {str(e)}"}), 500

# -------------------------------------------------------------
# Web Page Views
# -------------------------------------------------------------
@app.route("/login")
def index():
    return redirect(url_for('home'))

@app.route("/ope", methods=["GET", "POST"])
def login():
    if is_logged_in() and get_role() in ['admin', 'candidate']:
        if get_role() == 'admin':
            return redirect(url_for('admin_dashboard'))
        else:
            return redirect(url_for('candidate_dashboard'))
    error = None
    if request.method == "POST":
        username = request.form.get("username").strip()
        password = request.form.get("password")
        try:
            response = call_fastapi("POST", "/api/login", json={
                "username": username,
                "password": password
            })
            if response.status_code == 200:
                data = response.json()
                session['user_id'] = data['id']
                session['username'] = data['username']
                session['role'] = data['role']
                session['email'] = data['email']
                print(f"[Flask Proxy] User '{username}' logged in successfully with role '{data['role']}'.")
                if data['role'] == 'admin':
                    return redirect(url_for('admin_dashboard'))
                else:
                    return redirect(url_for('candidate_dashboard'))
            else:
                error = response.json().get("detail", "Invalid username or password")
                print(f"[Flask Proxy] Login failed for '{username}': {error}")
        except Exception as e:
            error = "Service communication failure: Connection actively refused by FastAPI backend."
    return render_template("login.html", error=error)

@app.route('/ope-redirect')
def ope_redirect():
    """Redirect into the in-app OPE (Online Practice Exam) login page"""
    return redirect(url_for('login'))

@app.route("/ope/logout")
def ope_logout():
    session.clear()
    return redirect(url_for('login'))

@app.route("/ope/dashboard")
def admin_dashboard():
    if not is_logged_in() or get_role() != 'admin':
        return redirect(url_for('login'))
    try:
        exams_res = call_fastapi("GET", "/api/exams")
        exams = exams_res.json() if exams_res.status_code == 200 else []

        results_res = call_fastapi("GET", "/api/results")
        results = results_res.json() if results_res.status_code == 200 else []

        violations_res = call_fastapi("GET", "/api/violations")
        violations = violations_res.json() if violations_res.status_code == 200 else []

    except Exception as e:
        exams, results, violations = [], [], []
        print(f"[Flask Proxy] Error fetching admin dashboard details from FastAPI: {str(e)}")

    return render_template(
        "ope_dashboard.html",
        username=session.get('username'),
        exams=exams,
        results=results,
        violations=violations
    )

@app.route("/candidate/dashboard")
def candidate_dashboard():
    if not is_logged_in() or get_role() != 'candidate':
        return redirect(url_for('login'))
    try:
        exams_res = call_fastapi("GET", "/api/exams")
        all_exams = exams_res.json() if exams_res.status_code == 200 else []

        results_res = call_fastapi("GET", "/api/results")
        all_results = results_res.json() if results_res.status_code == 200 else []

        my_results = [r for r in all_results if r['username'] == session.get('username') and r.get('exam_type', 'CERTIFICATION') == 'CERTIFICATION']

        # Split exams into two lists for the two dashboard tabs
        cert_exams = [e for e in all_exams if e.get('exam_type', 'CERTIFICATION') == 'CERTIFICATION']
        
        # Get pre-seeded MOCK_TEST exams
        seeded_mocks = [e for e in all_exams if e.get('exam_type') == 'MOCK_TEST']
        for s in seeded_mocks:
            s['is_custom_mock'] = False

        # ================================================================
        # FIX: Get published mock tests from mock_tests table
        # ================================================================
        mock_tests_res = call_fastapi("GET", "/api/admin/mock-tests")
        
        # Debug logging
        print(f"[Flask Proxy] Mock tests API response status: {mock_tests_res.status_code}")
        
        all_mock_tests = []
        if mock_tests_res.status_code == 200:
            try:
                all_mock_tests = mock_tests_res.json()
                print(f"[Flask Proxy] Found {len(all_mock_tests)} total mock tests")
                # Log each test's status
                for t in all_mock_tests:
                    print(f"[Flask Proxy] Test ID: {t.get('id')}, Title: {t.get('title')}, Published: {t.get('is_published')}")
            except Exception as e:
                print(f"[Flask Proxy] Error parsing mock tests response: {e}")
                all_mock_tests = []
        else:
            print(f"[Flask Proxy] Failed to fetch mock tests: {mock_tests_res.status_code}")
        
        # IMPORTANT: Only show published tests (is_published == 1)
        published_mocks = [t for t in all_mock_tests if t.get('is_published') == 1]
        for p in published_mocks:
            p['is_custom_mock'] = True
            print(f"[Flask Proxy] Added published mock: {p.get('title')} (ID: {p.get('id')})")

        mock_exams = seeded_mocks + published_mocks
        print(f"[Flask Proxy] Total mock exams: {len(mock_exams)} (Seeded: {len(seeded_mocks)}, Published: {len(published_mocks)})")

    except Exception as e:
        cert_exams, mock_exams, my_results = [], [], []
        print(f"[Flask Proxy] Error fetching candidate dashboard details from FastAPI: {str(e)}")
        import traceback
        traceback.print_exc()

    return render_template(
        "candidate_dashboard.html",
        username=session.get('username'),
        cert_exams=cert_exams,
        mock_exams=mock_exams,
        results=my_results
    )


# ========== NEW ADMIN ROUTES FOR QUESTION BANK ==========

@app.route("/admin/upload-questions")
def admin_upload_questions():
    """Admin page for uploading CSV/Excel questions"""
    if not is_logged_in() or get_role() != 'admin':
        return redirect(url_for('login'))
    
    # Get categories for dropdown
    try:
        cats_res = call_fastapi("GET", "/api/admin/categories")
        categories_data = cats_res.json() if cats_res.status_code == 200 else []
        categories = [c['category'] if isinstance(c, dict) else c for c in categories_data]
    except:
        categories = []
    
    return render_template("upload_questions.html", categories=categories, username=session.get('username'))

@app.route("/admin/question-bank")
def admin_question_bank():
    """Admin page for managing question bank"""
    if not is_logged_in() or get_role() != 'admin':
        return redirect(url_for('login'))
    
    return render_template("manage_question_bank.html", username=session.get('username'))

@app.route("/admin/create-mock-test")
def admin_create_mock_test():
    """Admin page for creating mock tests"""
    if not is_logged_in() or get_role() != 'admin':
        return redirect(url_for('login'))
    
    # Get categories from question bank
    try:
        cats_res = call_fastapi("GET", "/api/admin/categories")
        categories_data = cats_res.json() if cats_res.status_code == 200 else []
        categories = [c['category'] if isinstance(c, dict) else c for c in categories_data]
    except:
        categories = []
    
    return render_template("create_mock_test.html", categories=categories, username=session.get('username'))

@app.route("/admin/manage-tests")
def admin_manage_tests():
    """Admin page for managing mock tests"""
    if not is_logged_in() or get_role() != 'admin':
        return redirect(url_for('login'))
    
    return render_template("manage_mock_tests.html", username=session.get('username'))

# ========== NEW API PROXY ROUTES ==========

@app.route("/api/proxy/admin/upload-questions/preview", methods=["POST"])
def proxy_upload_preview():
    """Proxy for CSV/Excel file upload preview"""
    if not is_logged_in() or get_role() != 'admin':
        return jsonify({"error": "Unauthorized"}), 401
    
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files['file']
    files = {'file': (file.filename, file.stream, file.mimetype)}
    
    try:
        res = requests.post(f"{OPE_SERVICE_URL}/api/admin/upload-questions/preview", files=files, timeout=30)
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/admin/upload-questions/save", methods=["POST"])
def proxy_upload_save():
    """Proxy for saving uploaded questions"""
    if not is_logged_in() or get_role() != 'admin':
        return jsonify({"error": "Unauthorized"}), 401
    
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files['file']
    category = request.form.get('category', 'Uncategorized')
    
    # ✅ FIX: Log the category for debugging
    print(f"[Flask Proxy] Uploading file '{file.filename}' with category: '{category}'")
    
    # ✅ FIX: Ensure category is not empty
    if not category or category.strip() == '':
        category = 'Uncategorized'
    
    # ✅ FIX: Read file content into memory and send properly
    file_content = file.read()
    
    # ✅ FIX: Use a proper multipart form data with both file and category
    files = {
        'file': (file.filename, file_content, file.mimetype or 'application/octet-stream')
    }
    data = {
        'category': category
    }
    
    try:
        res = requests.post(
            f"{OPE_SERVICE_URL}/api/admin/upload-questions/save",
            files=files,
            data=data,
            timeout=60
        )
        
        print(f"[Flask Proxy] FastAPI response status: {res.status_code}")
        print(f"[Flask Proxy] FastAPI response: {res.text[:200]}")
        
        if res.status_code == 200:
            return jsonify(res.json()), 200
        else:
            try:
                error_data = res.json()
                return jsonify(error_data), res.status_code
            except:
                return jsonify({"error": res.text}), res.status_code
                
    except requests.exceptions.Timeout:
        return jsonify({"error": "Upload timed out. Please try again."}), 504
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Backend service unavailable"}), 503
    except Exception as e:
        print(f"[Flask Proxy] Upload error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/admin/question-bank", methods=["GET"])
def proxy_get_question_bank():
    """Proxy for getting question bank"""
    if not is_logged_in() or get_role() != 'admin':
        return jsonify({"error": "Unauthorized"}), 401
    
    page = request.args.get('page')
    page_size = request.args.get('page_size')
    category = request.args.get('category', '')
    search = request.args.get('search', '')
    limit = request.args.get('limit')
    
    url = f"{OPE_SERVICE_URL}/api/admin/question-bank"
    params = {}
    if page is not None:
        params['page'] = page
    if page_size is not None:
        params['page_size'] = page_size
    if category:
        params['category'] = category
    if search:
        params['search'] = search
    if limit is not None:
        params['limit'] = limit
    
    try:
        res = requests.get(url, params=params, timeout=30)
        if res.status_code == 200:
            data = res.json()
            # If the request did not specify pagination parameters, return the raw list of questions for compatibility
            if page is None and page_size is None and isinstance(data, dict) and 'questions' in data:
                return jsonify(data['questions']), 200
            return jsonify(data), res.status_code
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/admin/categories", methods=["GET"])
def proxy_get_categories():
    """Proxy for getting categories"""
    if not is_logged_in() or get_role() != 'admin':
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        res = call_fastapi("GET", "/api/admin/categories")
        if res.status_code == 200:
            data = res.json()
            # If request is from create-mock-test, return a list of strings to preserve compatibility
            referer = request.headers.get("Referer", "")
            if "create-mock-test" in referer:
                return jsonify([c['category'] if isinstance(c, dict) else c for c in data]), 200
            return jsonify(data), 200
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/admin/question-bank/<int:qid>", methods=["DELETE"])
def proxy_delete_question(qid):
    """Proxy for deleting a question"""
    if not is_logged_in() or get_role() != 'admin':
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        res = call_fastapi("DELETE", f"/api/admin/question-bank/{qid}")
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/admin/mock-tests", methods=["GET", "POST"])
def proxy_mock_tests():
    """Proxy for mock tests CRUD"""
    if not is_logged_in() or get_role() != 'admin':
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        if request.method == "POST":
            res = call_fastapi("POST", "/api/admin/mock-tests", json=request.json)
        else:
            res = call_fastapi("GET", "/api/admin/mock-tests")
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/admin/mock-tests/<int:test_id>/publish", methods=["PUT"])
def proxy_publish_test(test_id):
    """Proxy for publishing/unpublishing test"""
    if not is_logged_in() or get_role() != 'admin':
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        res = call_fastapi("PUT", f"/api/admin/mock-tests/{test_id}/publish")
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/admin/mock-tests/<int:test_id>", methods=["DELETE"])
def proxy_delete_mock_test(test_id):
    """Proxy for deleting mock test"""
    if not is_logged_in() or get_role() != 'admin':
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        res = call_fastapi("DELETE", f"/api/admin/mock-tests/{test_id}")
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/candidate/upcoming-tests", methods=["GET"])
def proxy_upcoming_tests():
    """Proxy for candidate upcoming tests"""
    if not is_logged_in() or get_role() != 'candidate':
        return jsonify({"error": "Unauthorized"}), 401
    
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({"error": "User ID not found in session"}), 400
    
    try:
        # Use direct requests call instead of call_fastapi for better control
        url = f"{OPE_SERVICE_URL}/api/candidate/upcoming-tests"
        params = {"user_id": user_id}
        
        print(f"[Flask Proxy] Fetching upcoming tests for user {user_id} from {url}")
        
        res = requests.get(url, params=params, timeout=10)
        
        print(f"[Flask Proxy] Upcoming tests response status: {res.status_code}")
        
        if res.status_code == 200:
            return jsonify(res.json()), 200
        else:
            return jsonify({"error": f"Backend returned {res.status_code}", "detail": res.text}), res.status_code
            
    except requests.exceptions.Timeout:
        print("[Flask Proxy] Upcoming tests timeout")
        return jsonify({"error": "Request timed out"}), 504
    except requests.exceptions.ConnectionError:
        print("[Flask Proxy] Upcoming tests connection error")
        return jsonify({"error": "Backend service unavailable"}), 503
    except Exception as e:
        print(f"[Flask Proxy] Upcoming tests error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/candidate/previous-attempts", methods=["GET"])
def proxy_previous_attempts():
    """Proxy for candidate previous attempts"""
    if not is_logged_in() or get_role() != 'candidate':
        return jsonify({"error": "Unauthorized"}), 401

    user_id = session.get('user_id')
    if not user_id:
        return jsonify({"error": "User ID not found in session"}), 400
    
    try:
        # Use direct requests call instead of call_fastapi for better control
        url = f"{OPE_SERVICE_URL}/api/candidate/previous-attempts"
        params = {"user_id": user_id}
        
        print(f"[Flask Proxy] Fetching previous attempts for user {user_id} from {url}")
        
        res = requests.get(url, params=params, timeout=10)
        
        print(f"[Flask Proxy] Previous attempts response status: {res.status_code}")
        
        if res.status_code == 200:
            return jsonify(res.json()), 200
        else:
            return jsonify({"error": f"Backend returned {res.status_code}", "detail": res.text}), res.status_code
            
    except requests.exceptions.Timeout:
        print("[Flask Proxy] Previous attempts timeout")
        return jsonify({"error": "Request timed out"}), 504
    except requests.exceptions.ConnectionError:
        print("[Flask Proxy] Previous attempts connection error")
        return jsonify({"error": "Backend service unavailable"}), 503
    except Exception as e:
        print(f"[Flask Proxy] Previous attempts error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/admin/categories/<category_name>", methods=["DELETE"])
def proxy_delete_category(category_name):
    """Proxy for deleting a category"""
    if not is_logged_in() or get_role() != 'admin':
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        res = call_fastapi("DELETE", f"/api/admin/categories/{category_name}")
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/admin/mock-tests/<int:test_id>", methods=["DELETE"])
def proxy_delete_mock_test_v2(test_id):
    """Proxy for deleting mock test"""
    if not is_logged_in() or get_role() != 'admin':
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        res = call_fastapi("DELETE", f"/api/admin/mock-tests/{test_id}")
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# New specific mock exam route to avoid ID collisions
@app.route("/exam/mock/<int:mock_id>")
def mock_exam(mock_id):
    if not is_logged_in() or get_role() != 'candidate':
        return redirect(url_for('login'))
    
    try:
        mock_res = call_fastapi("GET", f"/api/admin/mock-tests")
        if mock_res.status_code == 200:
            mock_tests = mock_res.json()
            mock_test = next((t for t in mock_tests if t['id'] == mock_id), None)
            
            if mock_test:
                # Get questions from mock test endpoint
                questions_res = call_fastapi("GET", f"/api/mock-tests/{mock_id}/questions")
                questions = questions_res.json() if questions_res.status_code == 200 else []
                
                if not questions:
                    return "This mock test does not have any questions yet.", 400
                
                return render_template(
                    "exam.html",
                    exam={
                        "id": mock_test['id'],
                        "title": mock_test['title'],
                        "description": mock_test.get('description', ''),
                        "duration_minutes": mock_test['duration_minutes'],
                        "exam_type": "MOCK_TEST"
                    },
                    user_id=session.get('user_id'),
                    username=session.get('username'),
                    is_custom_mock=True
                )
        return "Mock test not found", 404
    except Exception as e:
        return f"Error loading mock exam details: {str(e)}", 500

@app.route("/exam/<int:exam_id>")
def exam(exam_id):
    if not is_logged_in() or get_role() != 'candidate':
        return redirect(url_for('login'))
    
    try:
        exam_res = call_fastapi("GET", f"/api/exams/{exam_id}")
        if exam_res.status_code != 200:
            return "Exam not found", 404
        exam_data = exam_res.json()

        questions_res = call_fastapi("GET", f"/api/exams/{exam_id}/questions")
        questions = questions_res.json() if questions_res.status_code == 200 else []

        if not questions:
            return "This exam does not have any questions yet. Contact your administrator.", 400

    except Exception as e:
        return f"Error loading exam details: {str(e)}", 500

    return render_template(
        "exam.html",
        exam=exam_data,
        user_id=session.get('user_id'),
        username=session.get('username'),
        is_custom_mock=False
    )
@app.route("/exam/attempt/<int:attempt_id>/result")
def exam_result(attempt_id):
    if not is_logged_in():
        return redirect(url_for('login'))

    try:
        res = call_fastapi("GET", f"/api/attempts/{attempt_id}/result")

        if res.status_code != 200:
            return "Result record not found", 404

        result_data = res.json()

    except Exception as e:
        return f"Error loading result details: {str(e)}", 500

    return render_template(
        "result.html",
        result=result_data,
        is_admin=(get_role() == 'admin')
    )
# -------------------------------------------------------------
# AJAX Proxy Endpoints
# -------------------------------------------------------------
@app.route("/api/proxy/attempts/start", methods=["POST"])
def proxy_start_attempt():
    if not is_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        res = call_fastapi("POST", "/api/attempts/start", json=request.json)
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/exams/<int:exam_id>/questions", methods=["GET"])
def proxy_get_questions(exam_id):
    if not is_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        res = call_fastapi("GET", f"/api/exams/{exam_id}/questions")
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/mock-tests/<int:test_id>/questions", methods=["GET"])
def proxy_get_mock_questions(test_id):
    if not is_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        res = call_fastapi("GET", f"/api/mock-tests/{test_id}/questions")
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/attempts/<int:attempt_id>/answers", methods=["POST"])
def proxy_save_answers(attempt_id):
    if not is_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        res = call_fastapi("POST", f"/api/attempts/{attempt_id}/answers", json=request.json)
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/attempts/<int:attempt_id>/violations", methods=["POST"])
def proxy_log_violation(attempt_id):
    if not is_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        res = call_fastapi("POST", f"/api/attempts/{attempt_id}/violations", json=request.json)
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/attempts/<int:attempt_id>/submit", methods=["POST"])
def proxy_submit_exam(attempt_id):
    if not is_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        res = call_fastapi("POST", f"/api/attempts/{attempt_id}/submit")
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/attempts/<int:attempt_id>/upload-recording", methods=["POST"])
def proxy_upload_recording(attempt_id):
    if not is_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file part in the request"}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400
        files = {'file': (file.filename, file.stream, file.mimetype)}
        print(f"[Flask Proxy] Forwarding recording file to FastAPI for attempt {attempt_id}...")
        url = f"{OPE_SERVICE_URL}/api/attempts/{attempt_id}/upload-recording"
        res = requests.post(url, files=files, timeout=60)
        return jsonify(res.json()), res.status_code
    except Exception as e:
        print(f"[Flask Proxy] ERROR: Failed to upload recording: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/attempts/<int:attempt_id>/review", methods=["GET"])
def proxy_get_review(attempt_id):
    """Proxy for Mock Test post-attempt question review with correct answers + explanations."""
    if not is_logged_in():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        res = call_fastapi("GET", f"/api/attempts/{attempt_id}/review")
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Admin CRUD Proxies
@app.route("/api/proxy/exams", methods=["POST"])
def proxy_create_exam():
    if not is_logged_in() or get_role() != 'admin':
        return jsonify({"error": "Unauthorized"}), 401
    try:
        data = request.json
        data["created_by"] = session.get("user_id")
        res = call_fastapi("POST", "/api/exams", json=data)
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/exams/<int:exam_id>", methods=["PUT", "DELETE"])
def proxy_modify_exam(exam_id):
    if not is_logged_in() or get_role() != 'admin':
        return jsonify({"error": "Unauthorized"}), 401
    try:
        if request.method == "PUT":
            data = request.json
            data["created_by"] = session.get("user_id")
            res = call_fastapi("PUT", f"/api/exams/{exam_id}", json=data)
        else:
            res = call_fastapi("DELETE", f"/api/exams/{exam_id}")
        response_body = jsonify(res.json()) if res.content else jsonify({"message": "Deleted"})
        return response_body, res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/proxy/exams/<int:exam_id>/questions", methods=["POST"])
def proxy_create_question(exam_id):
    if not is_logged_in() or get_role() != 'admin':
        return jsonify({"error": "Unauthorized"}), 401
    try:
        res = call_fastapi("POST", f"/api/exams/{exam_id}/questions", json=request.json)
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# STM code integrated into billing_service
@app.route("/admin/create-payment-link", methods=["POST"])
def create_payment_link():
    """
    Called from your admin panel / order page when staff want to
    generate a one-time payment link for a client.
    Expected JSON body: { article_id, amount, currency, client_reference }
    """
    data = request.get_json(force=True)
    if not isinstance(data, dict):
        return jsonify({"error": "invalid request body"}), 400

    required = {"article_id", "amount"}
    missing = required - data.keys()
    if missing:
        return jsonify({"error": f"missing fields: {sorted(missing)}"}), 400

    article_id = str(data.get("article_id") or "").strip()
    if not article_id:
        return jsonify({"error": "article_id is required"}), 400

    currency = str(data.get("currency") or "USD").strip().upper()
    if len(currency) != 3 or not currency.isalpha():
        return jsonify({"error": "currency must be a 3-letter ISO code"}), 400

    print(
        "📤 Website create-payment-link | "
        f"article_id={article_id} amount={data.get('amount')} currency={currency}"
    )

    try:
        resp = requests.post(
            f"{BILLING_SERVICE_URL}/stm/generate",
            json={
                "article_id": article_id,
                "amount": data["amount"],
                "currency": currency,
                "client_reference": data.get("client_reference"),
            },
            headers=_stm_headers(),
            timeout=10,
        )
    except requests.RequestException as exc:
        return jsonify({"error": "Billing STM endpoint unavailable", "detail": str(exc)}), 502

    try:
        payload = resp.json()
    except ValueError:
        raw = (resp.text or "")[:400]
        return jsonify({
            "error": "Billing STM endpoint returned non-JSON response",
            "status_code": resp.status_code,
            "detail": raw,
        }), 502

    if not resp.ok:
        print(f"❌ Website create-payment-link upstream non-OK | status={resp.status_code} payload={payload}")
        if isinstance(payload, dict):
            detail = payload.get("detail")
            if isinstance(detail, dict):
                message = detail.get("message") or payload.get("error") or "Billing STM request failed"
                return jsonify({"error": message, "detail": detail}), resp.status_code
        return jsonify(payload if isinstance(payload, dict) else {"error": "Billing STM request failed"}), resp.status_code

    print(f"✅ Website create-payment-link success | token_id={payload.get('token_id')}")
    return jsonify(payload), 201


@app.route("/admin/payment-link-status/<token_id>", methods=["GET"])
def payment_link_status(token_id):
    """Optional: lets the admin panel poll whether a link has been paid."""
    print(f"📥 Website status poll | token_id={token_id}")
    try:
        resp = requests.get(
            f"{BILLING_SERVICE_URL}/stm/status/{token_id}",
            headers=_stm_headers(),
            timeout=10,
        )
    except requests.RequestException as exc:
        return jsonify({"error": "Billing STM endpoint unavailable", "detail": str(exc)}), 502

    try:
        payload = resp.json()
    except ValueError:
        raw = (resp.text or "")[:400]
        return jsonify({
            "error": "Billing STM status returned non-JSON response",
            "status_code": resp.status_code,
            "detail": raw,
        }), 502

    if not resp.ok:
        print(f"❌ Website status upstream non-OK | token_id={token_id} status={resp.status_code} payload={payload}")
        return jsonify(payload if isinstance(payload, dict) else {"error": "Billing STM status request failed"}), resp.status_code

    print(f"✅ Website status response | token_id={token_id} status={payload.get('status')}")
    return jsonify(payload)


@app.route("/create-link", methods=["GET"])
def create_link_page():
    """Simple internal form for staff to generate a link (server-rendered)."""
    return render_template("create_link.html")


@app.route("/payment-link-status/<token_id>", methods=["GET"])
def payment_link_status_page(token_id):
    """Client-facing status page for a payment token."""
    return render_template("payment_status.html", token_id=token_id)


@app.route("/stm/checkout/<token_id>", methods=["GET"])
def proxy_stm_checkout(token_id):
    """Public checkout URL that forwards to billing STM checkout endpoint."""
    print(f"📥 Website checkout proxy hit | token_id={token_id}")
    try:
        resp = requests.get(
            f"{BILLING_SERVICE_URL}/stm/checkout/{token_id}",
            headers=_stm_headers(),
            timeout=12,
            allow_redirects=False,
        )
    except requests.RequestException as exc:
        return jsonify({"error": "Billing STM checkout unavailable", "detail": str(exc)}), 502

    if resp.status_code in (301, 302, 303, 307, 308):
        redirect_target = resp.headers.get("Location")
        if redirect_target:
            print(f"✅ Website checkout redirect | token_id={token_id} location={redirect_target}")
            return redirect(redirect_target, code=302)

    try:
        payload = resp.json()
    except ValueError:
        payload = {"error": "Invalid checkout response from billing", "status_code": resp.status_code}
    print(f"ℹ️ Website checkout non-redirect response | token_id={token_id} status={resp.status_code} payload={payload}")
    return jsonify(payload), resp.status_code


@app.route("/stm/webhook/razorpay", methods=["POST"], strict_slashes=False)
def proxy_stm_razorpay_webhook():
    """Public webhook endpoint that forwards Razorpay callbacks to billing service."""
    raw_body = request.get_data()
    signature = request.headers.get("X-Razorpay-Signature", "")
    content_type = request.headers.get("Content-Type", "application/json")

    print(
        "📥 Website webhook hit /stm/webhook/razorpay | "
        f"sig_present={'Y' if bool(signature) else 'N'} body_len={len(raw_body)}"
    )

    try:
        resp = requests.post(
            f"{BILLING_SERVICE_URL}/stm/webhook/razorpay",
            data=raw_body,
            headers={
                "Content-Type": content_type,
                "X-Razorpay-Signature": signature,
            },
            timeout=15,
        )
    except requests.RequestException as exc:
        print(f"❌ Website webhook proxy failed: {exc}")
        return jsonify({"error": "Billing STM webhook unavailable", "detail": str(exc)}), 502

    body_preview = (resp.text or "")[:300]
    print(f"✅ Website webhook proxied | upstream_status={resp.status_code} body_preview={body_preview}")
    response = make_response(resp.content, resp.status_code)
    response.headers["Content-Type"] = resp.headers.get("Content-Type", "application/json")
    return response
# =============================================================
#  Interview Feedback proxy routes  (added)
#  Pure HTTP proxy to interview_service.py (FastAPI, Oracle-backed,
#  run separately e.g. `uvicorn interview_service:app --port 8500`).
#  No DB access happens here — mirrors call_fastapi() / the other
#  microservice proxy routes above.
# =============================================================
INTERVIEW_SERVICE_URL = os.getenv("INTERVIEW_SERVICE_URL", "http://127.0.0.1:8500").rstrip("/")

INTERVIEW_ROUNDS = ["Screening", "Technical Round 1", "Technical Round 2", "Hiring Manager Round", "Final Round"]
INTERVIEW_RECOMMENDATIONS = ["Strong Hire", "Hire", "No Hire", "Strong No Hire"]


def _interview_rec_class(rec):
    """Maps a recommendation string to a CSS class for color-coding."""
    if "Split" in rec:
        return "rec-split"
    if rec in ("Strong Hire", "Hire"):
        return "rec-hire"
    return "rec-no-hire"


def _interview_service_request(method, endpoint, json_body=None, params=None, timeout=None):
    """Call interview_service.py endpoints as a pure proxy."""
    if timeout is None:
        timeout = (2, 15)

    endpoint_path = endpoint if endpoint.startswith("/") else f"/{endpoint}"
    target_url = f"{INTERVIEW_SERVICE_URL}{endpoint_path}"

    os.environ["NO_PROXY"] = INTERVIEW_SERVICE_URL
    os.environ["no_proxy"] = INTERVIEW_SERVICE_URL

    try:
        with requests.Session() as internal_session:
            internal_session.trust_env = False
            internal_session.proxies = {"http": None, "https": None}
            response = internal_session.request(
                method=method.upper(),
                url=target_url,
                json=json_body,
                params=params,
                timeout=timeout,
            )
        try:
            data = response.json()
        except ValueError:
            data = {"success": False, "message": "Non-JSON response from interview_service"}
        return response.status_code, data
    except (requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
        print(f"⚠️ Interview service unreachable: {type(e).__name__}: {e}")
        return 503, {"success": False, "message": "Interview feedback service is currently unreachable."}
    except Exception as e:
        return 503, {"success": False, "message": f"Interview service request failed: {e}"}


@app.route("/interview")
def interview_dashboard():
    deleted = request.args.get("deleted", 0, type=int)
    status_code, data = _interview_service_request("GET", "/api/interview/feedback")
    if status_code != 200:
        return render_template(
            "interview_dashboard.html",
            candidates=[], rec_class=_interview_rec_class, total_submissions=0, deleted=deleted,
        )
    return render_template(
        "interview_dashboard.html",
        candidates=data["candidates"],
        rec_class=_interview_rec_class,
        total_submissions=data["total_submissions"],
        deleted=deleted,
    )


@app.route("/interview/submit", methods=["GET"])
def interview_submit_form():
    return render_template(
        "interview_submit.html",
        rounds=INTERVIEW_ROUNDS, recommendations=INTERVIEW_RECOMMENDATIONS,
        today=date.today().isoformat(), edit_mode=False, entry=None, form_action="/interview/submit",
    )


@app.route("/interview/submit", methods=["POST"])
def interview_submit_feedback():
    form = request.form
    payload = {
        "candidate_name": form["candidate_name"].strip(),
        "position": form["position"].strip(),
        "interview_round": form["interview_round"],
        "interviewer_name": form["interviewer_name"].strip(),
        "interview_date": form["interview_date"],
        "technical_skills": int(form["technical_skills"]),
        "problem_solving": int(form["problem_solving"]),
        "communication": int(form["communication"]),
        "culture_fit": int(form["culture_fit"]),
        "recommendation": form["recommendation"],
        "comments": form.get("comments", "").strip(),
    }
    status_code, data = _interview_service_request("POST", "/api/interview/feedback", json_body=payload)
    if status_code not in (200, 201):
        flash(data.get("message", "Could not submit feedback."))
        return redirect(url_for("interview_submit_form"))
    return redirect(url_for("interview_candidate_detail", candidate_name=payload["candidate_name"], submitted=1))


@app.route("/interview/candidate/<candidate_name>")
def interview_candidate_detail(candidate_name):
    submitted = request.args.get("submitted", 0, type=int)
    updated = request.args.get("updated", 0, type=int)
    status_code, data = _interview_service_request("GET", f"/api/interview/candidate/{candidate_name}")
    if status_code != 200:
        return redirect(url_for("interview_dashboard"))
    return render_template(
        "interview_candidate.html",
        candidate=data["candidate"], rounds=data["rounds"], rec_class=_interview_rec_class,
        submitted=submitted, updated=updated,
    )


@app.route("/interview/candidate/<candidate_name>/delete", methods=["POST"])
def interview_delete_candidate(candidate_name):
    _interview_service_request("DELETE", f"/api/interview/candidate/{candidate_name}")
    return redirect(url_for("interview_dashboard", deleted=1))


@app.route("/interview/feedback/<int:feedback_id>/edit", methods=["GET"])
def interview_edit_feedback_form(feedback_id):
    status_code, entry = _interview_service_request("GET", f"/api/interview/feedback/{feedback_id}")
    if status_code != 200:
        return redirect(url_for("interview_dashboard"))
    return render_template(
        "interview_submit.html",
        rounds=INTERVIEW_ROUNDS, recommendations=INTERVIEW_RECOMMENDATIONS, today=date.today().isoformat(),
        edit_mode=True, entry=entry, form_action=url_for("interview_edit_feedback", feedback_id=feedback_id),
    )


@app.route("/interview/feedback/<int:feedback_id>/edit", methods=["POST"])
def interview_edit_feedback(feedback_id):
    form = request.form
    payload = {
        "candidate_name": form["candidate_name"].strip(),
        "position": form["position"].strip(),
        "interview_round": form["interview_round"],
        "interviewer_name": form["interviewer_name"].strip(),
        "interview_date": form["interview_date"],
        "technical_skills": int(form["technical_skills"]),
        "problem_solving": int(form["problem_solving"]),
        "communication": int(form["communication"]),
        "culture_fit": int(form["culture_fit"]),
        "recommendation": form["recommendation"],
        "comments": form.get("comments", "").strip(),
    }
    status_code, data = _interview_service_request(
        "PUT", f"/api/interview/feedback/{feedback_id}", json_body=payload
    )
    if status_code != 200:
        flash(data.get("message", "Could not update feedback."))
        return redirect(url_for("interview_edit_feedback_form", feedback_id=feedback_id))
    return redirect(url_for("interview_candidate_detail", candidate_name=payload["candidate_name"], updated=1))


@app.route("/interview/feedback/<int:feedback_id>/delete", methods=["POST"])
def interview_delete_feedback_entry(feedback_id):
    status_code, entry = _interview_service_request("GET", f"/api/interview/feedback/{feedback_id}")
    if status_code != 200:
        return redirect(url_for("interview_dashboard"))
    candidate_name = entry["candidate_name"]

    del_status, del_data = _interview_service_request("DELETE", f"/api/interview/feedback/{feedback_id}")
    if del_status == 200 and del_data.get("candidate_has_remaining_rounds"):
        return redirect(url_for("interview_candidate_detail", candidate_name=candidate_name, updated=1))
    return redirect(url_for("interview_dashboard", deleted=1))


if __name__ == "__main__":
    print("🚀 Starting Dev Server → http://127.0.0.1:8080")
    app.run(host='0.0.0.0', port=8080, debug=True, use_reloader=False)
#if __name__ == "__main__":
    #print("🚀 Starting Production Server on 0.0.0.0:8080...")
    #serve(app, host='0.0.0.0', port=8080, threads=50, url_scheme='http')