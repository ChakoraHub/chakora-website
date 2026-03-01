

import sys
import io
import logging
import traceback
import boto3
import base64
import redis
import json
# Set higher log level to reduce debug noise
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)

# Fix encoding for Windows
if sys.stdout.encoding != 'UTF-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'UTF-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from flask import Flask, render_template, request, redirect, url_for, session, flash, send_from_directory, make_response, jsonify
import re
import smtplib
import calendar
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, date
from threading import Lock
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import urllib.parse
import snowflake.connector
from snowflake.connector import errors
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import requests
from requests.auth import HTTPBasicAuth

import os

# ==========================================
# MICROSERVICES CONFIGURATION
# ==========================================
#STUDENT_SERVICE_URL = os.getenv("STUDENT_SERVICE_URL", "http://127.0.0.1:8001")
#STUDENT_SERVICE_URL = "https://42nd9058b0.execute-api.eu-north-1.amazonaws.com/prod"
#STUDENT_SERVICE_URL = "http://13.62.242.164:8001"
EMPLOYEE_SERVICE_URL = "https://s1ww4erdag.execute-api.eu-north-1.amazonaws.com/prod"
FASTAPI_BASE_URL = "https://n9m17jqfn2.execute-api.eu-north-1.amazonaws.com/Billing-Prod"
app = Flask(__name__)
# Should be fine as-is

app.secret_key = 'temporary123'
app.permanent_session_lifetime = timedelta(days=7)

# AWS Step Functions client
sf_client = boto3.client(
    "stepfunctions",
    region_name="eu-north-1"
)

def get_auth():
    """Get authentication from session or return None"""
    username = session.get('username')
    password = session.get('password')
    if username and password:
        return HTTPBasicAuth(username, password)
    return None

def get_db_connection():
    """Helper to always return a fresh DB connection with RSA key authentication"""
    try:
        print("\n" + "="*60)
        print("🔍 ATTEMPTING DATABASE CONNECTION")
        print("="*60)
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        key_path = os.path.join(script_dir, 'rsa_key.p8')
        
        print(f"📂 Script directory: {script_dir}")
        print(f"🔑 Key path: {key_path}")
        print(f"🔍 Key exists: {os.path.exists(key_path)}")
        
        if not os.path.exists(key_path):
            print(f"❌ RSA key file not found at: {key_path}")
            return None
        
        print("🔄 Loading RSA private key...")
        with open(key_path, 'rb') as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )
        
        print("✅ RSA key loaded successfully")
        
        pkb = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        print("🔄 Connecting to Snowflake...")
        print(f"   User: ChakoraHub")
        print(f"   Account: gpguymt-ta88699")
        
        conn = snowflake.connector.connect(
            user='ChakoraHub',
            account='gpguymt-ta88699',
            private_key=pkb,
            warehouse='COMPUTE_WH',
            database='VSRSUBHASH$CHAKORA_DB',
            schema='CHAKORA',
            login_timeout=30,
            network_timeout=30
        )
        
        print("✅ ✅ ✅ Connected to Snowflake using RSA key")
        print("="*60 + "\n")
        return conn
        
    except Exception as e:
        print(f"❌ DB Connection Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        print("="*60 + "\n")

def load_nrm_festivals_from_db():
    conn = get_db_connection()
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute("SELECT festival_name, festival_date FROM nrm_festivals")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {row['FESTIVAL_DATE'].strftime('%Y-%m-%d'): row['FESTIVAL_NAME'] for row in rows}
 


def normalize(name):
    return name.strip().lower().replace(' ', '').replace('_', '')

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

def allowed_file(filename, category='images'):
    ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''
    return ext in app.config['ALLOWED_EXTENSIONS'].get(category, set())

LAMBDA_URL = 'https://lwug4xhfz27whiuu3acjfwsgtm0ttwja.lambda-url.eu-north-1.on.aws/'

# ✅ Central config dictionary
app.config['UPLOAD_FOLDERS'] = {
    'profile_pics': os.path.join('static', 'profile_pics'),
    'practice_tests': '/home/vsrsubhash/uploads/practice_tests'
}

app.config['UPLOAD_FOLDER'] = '/home/vsrsubhash/uploads'
app.config['SYLLABUS_FOLDER'] = os.path.join('/home/vsrsubhash/uploads', "syllabus")

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


# ================= API GATEWAY URLS =================
HOME_SERVICE_URL = "https://yhdgfdhbzj.execute-api.eu-north-1.amazonaws.com/prod"
RESOURCES_SERVICE_URL = "https://42nd9058b0.execute-api.eu-north-1.amazonaws.com/prod"



# ================= HOME (PROXY) =====================

@app.route("/")
def home():
    # ❌ Prevent any flash messages from appearing on home
    session.pop('_flashes', None)

    feedbacks = []
    current_batches = []
    upcoming_batches = []

    try:
        # ================== FEEDBACK (DIRECT DB FETCH) ==================
        conn = get_db_connection()
        if not conn:
            print("❌ DB connection failed in home")
            raise Exception("Database connection failed")

        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                COALESCE(
                    NULLIF(f.NAME, ''),
                    NULLIF(u.USERNAME, ''),
                    'Anonymous'
                ) AS username,
                f.FEEDBACK_MESSAGE
            FROM "VSRSUBHASH$CHAKORA_DB"."CHAKORA"."NRM_FEEDBACK" f
            LEFT JOIN "VSRSUBHASH$CHAKORA_DB"."CHAKORA"."NRM_USERS" u
                ON f.STUDENT_ID = u.ID
            WHERE f.FEEDBACK_MESSAGE IS NOT NULL
              AND TRIM(f.FEEDBACK_MESSAGE) <> ''
            ORDER BY f.SUBMITTED_AT DESC
            LIMIT 20
        """)

        rows = cursor.fetchall()
        print("🧪 FEEDBACK ROWS:", rows)

        for row in rows:
            feedbacks.append({
                "username": row[0],
                "feedback_message": row[1]
            })

        print("✅ FEEDBACK COUNT:", len(feedbacks))

        cursor.close()
        conn.close()

        # ================== BATCHES (HOME MICROSERVICE) ==================
        batches_response = requests.get(
            f"{HOME_SERVICE_URL}/home/batches",
            timeout=5
        )

        if batches_response.status_code == 200:
            batches = batches_response.json()
            current_batches = batches.get("current_batches", [])
            upcoming_batches = batches.get("upcoming_batches", [])
        else:
            print("⚠️ Failed to fetch batches:", batches_response.status_code)

    except Exception as e:
        print("❌ HOME ERROR:", e)

    return render_template(
        "home.html",
        current_batches=current_batches,
        upcoming_batches=upcoming_batches,
        feedbacks=feedbacks,
        user=session.get("user"),
        current_year=datetime.now().year
    )
@app.route("/home/enquiry", methods=["POST"])
def proxy_home_enquiry():
    resp = requests.post(
        f"{HOME_SERVICE_URL}/home/enquiry",
        json=request.get_json(),
        timeout=10
    )
    return jsonify(resp.json()), resp.status_code

@app.route("/resources")
def resources():

    if session.get("login_type") != "user":
        flash("Please login as user", "error")
        return redirect(url_for("home"))

    # ================= FAST PATH (CACHE) =================
    if session.get("resources_cached"):
        return render_template(
            "resources.html",
            username=session.get("cached_username"),
            usertype=session.get("cached_usertype"),
            profile_pic=session.get("cached_profile_pic"),
            offers=session.get("cached_offers"),
            festival_today=session.get("cached_festival"),
            greeting=session.get("cached_greeting"),
            reg_id=session.get("user_id")
        )

    # ================= FIRST LOAD ONLY ===================
    try:

        user_resp = requests.get(
            f"{RESOURCES_SERVICE_URL}/resources/user-info",
            params={"user_id": session.get("user_id")},
            timeout=5
        )

        if user_resp.ok:
            user = user_resp.json().get("user", {})
        else:
            user = {
                "username": session.get("user"),
                "usertype": session.get("usertype", "student"),
                "profile_pic": session.get("profile_pic")
            }

        offers_resp = requests.get(
            f"{RESOURCES_SERVICE_URL}/resources/offers",
            timeout=5
        )

        fest_resp = requests.get(
            f"{RESOURCES_SERVICE_URL}/resources/festivals",
            timeout=5
        )

        offers = offers_resp.json().get("offers", {}) if offers_resp.ok else {}
        festivals = fest_resp.json() if fest_resp.ok else {}

        # ========= SAVE TO SESSION =========
        session["resources_cached"] = True
        session["cached_username"] = user.get("username")
        session["cached_usertype"] = user.get("usertype")
        session["cached_profile_pic"] = user.get("profile_pic")
        session["cached_offers"] = offers
        session["cached_festival"] = festivals.get("festival_today")
        session["cached_greeting"] = festivals.get("greeting")

        return render_template(
            "resources.html",
            username=user.get("username"),
            usertype=user.get("usertype"),
            profile_pic=user.get("profile_pic"),
            offers=offers,
            festival_today=festivals.get("festival_today"),
            greeting=festivals.get("greeting"),
            reg_id=session.get("user_id")
        )

    except Exception as e:
        print("RESOURCES ERROR:", e)
        flash("Resources unavailable", "error")
        return redirect(url_for("home"))

# ================= LOGIN (BOTH TYPES) ===============
@app.route("/nrm_logins", methods=["POST"])
def nrm_logins():
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    login_type = request.form.get("login_type", "user").lower()

    conn = get_db_connection()
    cursor = conn.cursor(snowflake.connector.DictCursor)

    try:
        # ---------- USER LOGIN ----------
        if login_type == "user":
            cursor.execute("""
                SELECT u.ID, u.EMAIL, u.PHONE, u.USERTYPE, u.PROFILE_PIC, l.PASSWORD
                FROM NRM_USERS u
                JOIN NRM_LOGINS l ON u.ID = l.USER_ID
                WHERE LOWER(u.EMAIL)=LOWER(%s) OR u.PHONE=%s
                ORDER BY l.CREATED_AT DESC LIMIT 1
            """, (username, username))

            user = cursor.fetchone()
            if not user:
                flash("User not found", "error")
                return redirect(url_for("home"))

            if not check_password_hash(user["PASSWORD"], password):
                flash("Incorrect password", "error")
                return redirect(url_for("home"))

            session.clear()
            session["login_type"] = "user"
            session["user_id"] = user["ID"]
            session["user"] = user.get("EMAIL") or user.get("PHONE")
            session["usertype"] = user.get("USERTYPE", "student")
            session["profile_pic"] = user.get("PROFILE_PIC")
            session.permanent = True

            return redirect(url_for("resources"))

        # ---------- EMPLOYEE LOGIN ----------
        elif login_type == "employee":
            # For employees, username field usually carries the employee_id
            employee_id = request.form.get("employee_id", "").strip() or username

            cursor.execute("""
                SELECT e.EMPLOYEE_ID, e.EMPLOYEE_NAME, e.EMAIL, l.PASSWORD
                FROM EMP_NRM_EMPLOYEES e
                JOIN EMP_NRM_LOGINS l ON e.EMPLOYEE_ID = l.EMPLOYEE_ID
                WHERE e.EMPLOYEE_ID = %s    
                ORDER BY l.CREATED_AT DESC LIMIT 1
            """, (employee_id,))

            emp = cursor.fetchone()
            if not emp:
                flash("Employee not found", "error")
                return redirect(url_for("home"))

            # Hybrid check for Employee: handles plain text stored in Snowflake
            db_password = emp["PASSWORD"]
            if db_password.startswith('scrypt:') or db_password.startswith('pbkdf2:'):
                valid = check_password_hash(db_password, password)
            else:
                # Direct comparison for plain text passwords
                valid = (db_password == password)

            if not valid:
                flash("Incorrect password", "error")
                return redirect(url_for("home"))

            session.clear()
            session["login_type"] = "employee"
            session["employee_id"] = emp["EMPLOYEE_ID"]
            session["employee_name"] = emp["EMPLOYEE_NAME"]
            session["employee_email"] = emp["EMAIL"]
            session.permanent = True

            return redirect(url_for("employee_resources"))

    except Exception as e:
        print("❌ LOGIN ERROR:", e)
        flash("Login failed", "error")
        return redirect(url_for("home"))

    finally:
        cursor.close()
        conn.close()

@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out", "success")
    return redirect(url_for("home"))
# =========================================================
# FORGOT PASSWORD + OTP (EMPLOYEE + USER)
# =========================================================

from itsdangerous import URLSafeTimedSerializer

serializer = URLSafeTimedSerializer(app.secret_key)

@app.route("/forgot-password", methods=["GET","POST"])
def forgot_password():

    if request.method=="POST":

        login_type = request.form["type"]
        username = request.form["username"]

        conn = get_db_connection()
        cursor = conn.cursor()

        if login_type=="user":
            cursor.execute("""
            SELECT EMAIL FROM NRM_USERS
            WHERE LOWER(EMAIL)=LOWER(%s)
            """,(username,))
        else:
            cursor.execute("""
            SELECT EMAIL FROM EMP_NRM_EMPLOYEES
            WHERE LOWER(EMAIL)=LOWER(%s)
            """,(username,))

        row = cursor.fetchone()

        if not row:
            return "User not found"

        email=row[0]

        token = serializer.dumps(email)

        link = f"https://www.chakorahub.com/reset-password/{token}?type={login_type}"

        send_reset_email(email,link)

        return "Reset link sent to mail"

    return render_template("forgot_password.html")

def send_reset_email(to,link):

    sender="saitejatatineni5679@gmail.com"
    password="qciebujchundqnom"

    msg=MIMEMultipart()
    msg["Subject"]="Password Reset"
    msg["From"]=sender
    msg["To"]=to

    msg.attach(MIMEText(f"""
Click below to reset password:

{link}

Valid for 30 minutes.
"""))

    server=smtplib.SMTP("smtp.gmail.com",587)
    server.starttls()
    server.login(sender,password)
    server.send_message(msg)
    server.quit()

@app.route("/reset-password/<token>", methods=["GET","POST"])
def reset_password(token):

    login_type=request.args.get("type")

    email=serializer.loads(token,max_age=1800)

    if request.method=="POST":

        pwd=request.form["password"]

        hashed=generate_password_hash(pwd)

        conn=get_db_connection()
        cursor=conn.cursor()

        if login_type=="user":
            cursor.execute("""
            UPDATE NRM_LOGINS SET PASSWORD=%s
            WHERE USER_ID=(SELECT ID FROM NRM_USERS WHERE EMAIL=%s)
            """,(hashed,email))

        else:
            cursor.execute("""
            UPDATE EMP_NRM_LOGINS SET PASSWORD=%s
            WHERE EMPLOYEE_ID=(SELECT EMPLOYEE_ID FROM EMP_NRM_EMPLOYEES WHERE EMAIL=%s)
            """,(hashed,email))

        conn.commit()
        cursor.close()
        conn.close()

        return redirect(url_for("home"))

    return render_template("reset_password.html",email=email)

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
    """Employee resources page - Direct database access"""
    if session.get("login_type") != "employee":
        flash("Please login as employee first", "error")
        return redirect(url_for("home"))

    employee_id = session.get("employee_id")
    
    if not employee_id:
        flash("Session expired. Please login again.", "error")
        return redirect(url_for("home"))

    try:
        conn = get_db_connection()
        if not conn:
            flash("Database connection failed", "error")
            return redirect(url_for("home"))
            
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        # Get today's festival
        today = datetime.now().strftime('%Y-%m-%d')
        festival_today = None
        cursor.execute("SELECT FESTIVAL_NAME FROM EMP_NRM_FESTIVALS WHERE FESTIVAL_DATE = %s", (today,))
        festival_row = cursor.fetchone()
        if festival_row:
            festival_today = festival_row['FESTIVAL_NAME']
        
        # Fetch employee personal details from EMP_NRM_PERSONAL
        employee_data = {}
        cursor.execute("""
            SELECT 
                FIRST_NAME, LAST_NAME, DOB, GENDER,
                EMAIL, PHONE, ADDRESS, PROFILE_PIC
            FROM EMP_NRM_PERSONAL 
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        personal_row = cursor.fetchone()
        
        # Fetch job details from EMP_NRM_JOB_WORK
        job_data = {}
        cursor.execute("""
            SELECT 
                DEPT_ID, DESIGNATION_ID, MANAGER_ID
            FROM EMP_NRM_JOB_WORK 
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        job_row = cursor.fetchone()
        
        # Fetch department name
        dept_name = "N/A"
        if job_row and job_row.get('DEPT_ID'):
            cursor.execute("SELECT DEPT_NAME FROM EMP_NRM_DEPARTMENTS WHERE DEPT_ID = %s", (job_row['DEPT_ID'],))
            dept_result = cursor.fetchone()
            if dept_result:
                dept_name = dept_result['DEPT_NAME']
        
        # Fetch designation name
        designation_name = "N/A"
        if job_row and job_row.get('DESIGNATION_ID'):
            cursor.execute("SELECT TITLE FROM EMP_NRM_DESIGNATIONS WHERE DESIGNATION_ID = %s", (job_row['DESIGNATION_ID'],))
            desig_result = cursor.fetchone()
            if desig_result:
                designation_name = desig_result['TITLE']
        
        # Fetch manager name
        manager_name = "Not specified"
        if job_row and job_row.get('MANAGER_ID'):
            cursor.execute("SELECT MANAGER_NAME FROM EMP_NRM_MANAGERS WHERE MANAGER_ID = %s", (job_row['MANAGER_ID'],))
            mgr_result = cursor.fetchone()
            if mgr_result:
                manager_name = mgr_result['MANAGER_NAME']
        
        # Fetch joining date from EMP_NRM_EMPLOYEES
        joining_date = "Not specified"
        cursor.execute("SELECT APPLIED_DATE FROM EMP_NRM_EMPLOYEES WHERE EMPLOYEE_ID = %s", (employee_id,))
        emp_row = cursor.fetchone()
        if emp_row and emp_row.get('APPLIED_DATE'):
            joining_date = emp_row['APPLIED_DATE']
        
        if personal_row:
            full_name = f"{personal_row.get('FIRST_NAME', '')} {personal_row.get('LAST_NAME', '')}".strip()
            if not full_name:
                full_name = session.get("employee_name", "Employee")
            
            # Format address (split into current and permanent if needed)
            address = personal_row.get('ADDRESS', 'Not specified')
            employee_data = {
                'full_name': full_name,
                'employee_id': employee_id,
                'dob': personal_row.get('DOB', 'Not specified'),
                'gender': personal_row.get('GENDER', 'Not specified'),
                'email': personal_row.get('EMAIL', session.get("employee_email", "Not specified")),
                'phone': personal_row.get('PHONE', 'Not specified'),
                'department': dept_name,
                'designation': designation_name,
                'date_of_joining': joining_date,
                'manager': manager_name,
                'current_address': address,
                'permanent_address': address  # Using same address for both in absence of separate fields
            }
        else:
            # Fallback to session data
            employee_data = {
                'full_name': session.get("employee_name", "Employee"),
                'employee_id': employee_id,
                'dob': 'Not specified',
                'gender': 'Not specified',
                'email': session.get("employee_email", "Not specified"),
                'phone': 'Not specified',
                'department': dept_name or session.get("employee_department", "N/A"),
                'designation': designation_name or session.get("employee_designation", "N/A"),
                'date_of_joining': joining_date,
                'manager': manager_name,
                'current_address': 'Not specified',
                'permanent_address': 'Not specified'
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
            cursor.execute("SELECT BANK_DETAILS FROM EMP_NRM_PROFILE WHERE EMPLOYEE_ID = %s", (employee_id,))
            profile_row = cursor.fetchone()
            bank_details = "Not specified"
            if profile_row and profile_row.get('BANK_DETAILS'):
                bank_details = profile_row['BANK_DETAILS']
            
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
        leave_data = {'casual_leave': '12', 'sick_leave': '8', 'privilege_leave': '3'}  # Default
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
                'reviewer': manager_name or 'Jane Smith (Manager)',
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
        
        # Update profile pic if available from personal table
        profile_pic = session.get("profile_pic", "profile_photo/defaultpicture.jpg")
        if personal_row and personal_row.get('PROFILE_PIC'):
            profile_pic = personal_row['PROFILE_PIC']
        
        return render_template(
            "employee-resources.html",
            Employee_name=employee_data['full_name'],
            employee_name=employee_data['full_name'],
            profile_pic=profile_pic,
            festival_today=festival_today,
            reg_id=employee_id,
            employee_data=employee_data,
            salary_data=salary_data,
            leave_data=leave_data,
            leave_history=leave_history,
            id_card_data=id_card_data,
            queries=queries,
            appraisal_data=appraisal_data,
            appraisal_history=appraisal_history,
            profile_data=profile_data
        )

    except Exception as e:
        print("❌ Error in employee_resources:", e)
        traceback.print_exc()
        
        # Fallback with basic data
        employee_name = session.get("employee_name", "Employee")
        employee_data = {
            'full_name': employee_name,
            'employee_id': employee_id,
            'dob': 'Not specified',
            'gender': 'Not specified',
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
            profile_pic=session.get("profile_pic", "profile_photo/defaultpicture.jpg"),
            festival_today=None,
            reg_id=employee_id,
            employee_data=employee_data,
            salary_data={},
            leave_data={},
            leave_history=[],
            id_card_data={},
            queries=[],
            appraisal_data={},
            appraisal_history=[],
            profile_data={},
            error="Database connection failed, using minimal data"
        )

@app.route('/employee/personal-details', methods=['GET', 'POST'])
def personal_details():
    """Personal details page - Direct database access"""
    if session.get("login_type") != "employee":
        return redirect(url_for('home'))
    
    employee_id = session.get("employee_id")
    
    if request.method == 'POST':
        try:
            conn = get_db_connection()
            if not conn:
                flash("Database connection failed", "error")
                return redirect(url_for('personal_details'))
            
            cursor = conn.cursor()
            
            # Check if record exists
            cursor.execute("SELECT 1 FROM EMP_NRM_PERSONAL WHERE EMPLOYEE_ID = %s", (employee_id,))
            exists = cursor.fetchone()
            
            if exists:
                cursor.execute("""
                    UPDATE EMP_NRM_PERSONAL SET
                        FIRST_NAME = %s,
                        LAST_NAME = %s,
                        EMAIL = %s,
                        PHONE = %s,
                        ADDRESS = %s,
                        DOB = %s
                    WHERE EMPLOYEE_ID = %s
                """, (
                    request.form.get('firstName'),
                    request.form.get('lastName'),
                    request.form.get('email'),
                    request.form.get('phone'),
                    request.form.get('address'),
                    request.form.get('dob'),
                    employee_id
                ))
            else:
                cursor.execute("""
                    INSERT INTO EMP_NRM_PERSONAL 
                    (EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, ADDRESS, DOB)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    employee_id,
                    request.form.get('firstName'),
                    request.form.get('lastName'),
                    request.form.get('email'),
                    request.form.get('phone'),
                    request.form.get('address'),
                    request.form.get('dob')
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Update session
            session["employee_first_name"] = request.form.get('firstName')
            session["employee_last_name"] = request.form.get('lastName')
            session["employee_email"] = request.form.get('email')
            session["employee_phone"] = request.form.get('phone')
            
            flash("Personal details saved successfully!", "success")
                
        except Exception as e:
            print(f"❌ Update error: {e}")
            flash("Failed to save personal details", "error")
            
        return redirect(url_for('personal_details'))
    
    # GET request
    try:
        conn = get_db_connection()
        if not conn:
            return render_template('employee-personal-details.html',
                                 personal_data=None,
                                 employee_id=employee_id)
        
        cursor = conn.cursor(snowflake.connector.DictCursor)
        cursor.execute("""
            SELECT FIRST_NAME, LAST_NAME, EMAIL, PHONE, ADDRESS, DOB, PROFILE_PIC
            FROM EMP_NRM_PERSONAL 
            WHERE EMPLOYEE_ID = %s
            LIMIT 1
        """, (employee_id,))
        
        personal_row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if personal_row:
            personal_data = {
                'first_name': personal_row['FIRST_NAME'],
                'last_name': personal_row['LAST_NAME'],
                'email': personal_row['EMAIL'],
                'phone': personal_row['PHONE'],
                'address': personal_row['ADDRESS'],
                'dob': personal_row['DOB'].strftime('%Y-%m-%d') if personal_row['DOB'] else '',
                'profile_pic': personal_row['PROFILE_PIC']
            }
        else:
            personal_data = {
                'first_name': session.get("employee_first_name", ""),
                'last_name': session.get("employee_last_name", ""),
                'email': session.get("employee_email", ""),
                'phone': session.get("employee_phone", ""),
                'address': '',
                'dob': '',
                'profile_pic': session.get("profile_pic", "profile_photo/defaultpicture.jpg")
            }
        
        return render_template('employee-personal-details.html',
                             personal_data=personal_data,
                             employee_id=employee_id)
            
    except Exception as e:
        print(f"❌ Personal details error: {e}")
        return render_template('employee-personal-details.html',
                             personal_data=None,
                             employee_id=employee_id)

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
        cursor = conn.cursor(snowflake.connector.DictCursor)

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


@app.route('/employee/leave', methods=['GET', 'POST'])
def leave_tracker():
    """Leave tracker - direct DB"""
    if session.get("login_type") != "employee":
        return redirect(url_for('home'))
    
    employee_id = session.get("employee_id")
    
    if request.method == 'POST':
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO EMP_NRM_LEAVE 
                (EMPLOYEE_ID, START_DATE, END_DATE, REASON, STATUS, APPLIED_AT)
                VALUES (%s, %s, %s, %s, 'Pending', CURRENT_TIMESTAMP())
            """, (
                employee_id,
                request.form.get('start_date'),
                request.form.get('end_date'),
                request.form.get('reason')
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            flash("Leave application submitted successfully!", "success")
            return redirect(url_for('leave_tracker'))
            
        except Exception as e:
            print(f"❌ Leave application error: {e}")
            flash("Failed to submit leave", "error")
    
    # GET request
    month = request.args.get('month', datetime.now().month, type=int)
    year = request.args.get('year', datetime.now().year, type=int)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        # Get leave data
        cursor.execute("""
            SELECT LEAVE_ID, START_DATE, END_DATE, REASON, STATUS, APPLIED_AT
            FROM EMP_NRM_LEAVE
            WHERE EMPLOYEE_ID = %s
            ORDER BY APPLIED_AT DESC
        """, (employee_id,))
        leave_data = cursor.fetchall()
        
        # Get festivals
        cursor.execute("""
            SELECT FESTIVAL_NAME, FESTIVAL_DATE
            FROM EMP_NRM_FESTIVALS
            WHERE EXTRACT(YEAR FROM FESTIVAL_DATE) = %s 
            AND EXTRACT(MONTH FROM FESTIVAL_DATE) = %s
        """, (year, month))
        festivals = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Calendar calculation
        prev_month = month - 1 if month > 1 else 12
        prev_year = year if month > 1 else year - 1
        next_month = month + 1 if month < 12 else 1
        next_year = year if month < 12 else year + 1
        
        return render_template('emp-leave.html',
                             leave_data=leave_data,
                             calendar_data=[],  # Add calendar logic if needed
                             month=month,
                             year=year,
                             month_name=calendar.month_name[month],
                             prev_month=prev_month,
                             prev_year=prev_year,
                             next_month=next_month,
                             next_year=next_year,
                             employee_id=employee_id,
                             employee_name=session.get('employee_name', 'Employee'))
        
    except Exception as e:
        print(f"❌ Leave tracker error: {e}")
        return render_template('emp-leave.html',
                             leave_data=[],
                             calendar_data=[])
    
@app.route("/emp-leave", methods=["GET", "POST"])
def emp_leave():
    if session.get("login_type") != "employee":
        return redirect(url_for("home"))

    employee_id = session.get("employee_id")

    # ---------------- POST (Apply Leave) ----------------
    if request.method == "POST":
        payload = {
            "employee_id": employee_id,
            "leave_type": request.form["leave_type"],
            "from_date": request.form["from_date"],
            "to_date": request.form["to_date"],
            "reason": request.form.get("reason")
        }

        try:
            response = requests.post(
                f"{EMPLOYEE_SERVICE_URL}/api/employee/leave/apply",
                json=payload,
                timeout=5
            )
            result = response.json()

            if response.status_code == 200 and result.get("success"):
                flash("Leave applied successfully!", "success")
            else:
                flash(result.get("message", "Leave request failed"), "error")

        except requests.exceptions.RequestException:
            flash("Employee service unavailable", "error")

        return redirect(url_for("emp_leave"))

    # ---------------- GET (Tracker View) ----------------
    month = request.args.get("month", datetime.now().month, type=int)
    year = request.args.get("year", datetime.now().year, type=int)

    # Calendar calc stays in Flask
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1

    try:
        leave_res = requests.get(
            f"{EMPLOYEE_SERVICE_URL}/api/employee/leave/history",
            params={"employee_id": employee_id},
            timeout=5
        )

        fest_res = requests.get(
            f"{EMPLOYEE_SERVICE_URL}/api/employee/festivals",
            params={"year": year, "month": month},
            timeout=5
        )

        leave_data = leave_res.json() if leave_res.status_code == 200 else []
        festivals = fest_res.json() if fest_res.status_code == 200 else []

    except requests.exceptions.RequestException:
        leave_data = []
        festivals = []
        flash("Unable to load leave data", "error")

    return render_template(
        "emp-leave.html",
        leave_data=leave_data,
        festivals=festivals,
        month=month,
        year=year,
        month_name=calendar.month_name[month],
        prev_month=prev_month,
        prev_year=prev_year,
        next_month=next_month,
        next_year=next_year,
        employee_id=employee_id,
        employee_name=session.get("employee_name", "Employee")
    )


@app.route('/employee/id-card')
def id_card():
    if session.get("login_type") != "employee":
        return redirect(url_for('home'))

    employee_id = session.get("employee_id")

    try:
        conn = get_db_connection()
        cursor = conn.cursor(snowflake.connector.DictCursor)

        # ID card data
        cursor.execute("""
            SELECT ID_NUMBER, ISSUE_DATE, EXPIRY_DATE
            FROM EMP_NRM_IDCARD
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        id_card_data = cursor.fetchone()

        # Personal data
        cursor.execute("""
            SELECT FIRST_NAME, LAST_NAME, DOB, EMAIL, PHONE, ADDRESS, PROFILE_PIC
            FROM EMP_NRM_PERSONAL
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        personal_data = cursor.fetchone()

        # ✅ Employee data (FIXED COLUMN NAMES)
        cursor.execute("""
            SELECT 
                e.EMPLOYEE_ID,
                e.EMPLOYEE_NAME,
                e.STATUS,
                d.DEPT_NAME,
                des.TITLE AS DESIGNATION
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_JOB_WORK jw ON e.EMPLOYEE_ID = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS d ON jw.DEPT_ID = d.DEPT_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS des ON jw.DESIGNATION_ID = des.DESIGNATION_ID
            WHERE e.EMPLOYEE_ID = %s
        """, (employee_id,))
        employee_data = cursor.fetchone()

        cursor.close()
        conn.close()

        return render_template(
            'employee-idcard.html',
            id_card_data=id_card_data,
            personal_data=personal_data,
            employee_data=employee_data,
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
        )


@app.route('/employee/queries', methods=['GET', 'POST'])
def employee_queries():
    """Employee queries - direct DB"""
    if session.get("login_type") != "employee":
        return redirect(url_for('home'))
    
    employee_id = session.get("employee_id")
    
    if request.method == 'POST':
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO EMP_NRM_QUERIES 
                (EMPLOYEE_ID, QUERY_TEXT, STATUS, CREATED_AT)
                VALUES (%s, %s, 'Pending', CURRENT_TIMESTAMP())
            """, (employee_id, request.form.get('query_text')))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            flash("Query submitted successfully!", "success")
            return redirect(url_for('employee_queries'))
            
        except Exception as e:
            print(f"❌ Query submission error: {e}")
            flash("Failed to submit query", "error")
    
    # GET request
    try:
        conn = get_db_connection()
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        cursor.execute("""
            SELECT QUERY_ID, QUERY_TEXT, STATUS, CREATED_AT
            FROM EMP_NRM_QUERIES
            WHERE EMPLOYEE_ID = %s
            ORDER BY CREATED_AT DESC
        """, (employee_id,))
        
        queries_data = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return render_template('employee-queries.html', queries_data=queries_data)
        
    except Exception as e:
        print(f"❌ Queries error: {e}")
        return render_template('employee-queries.html', queries_data=[])

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

@app.route('/api/appraisal/goals', methods=['GET', 'POST'])
def appraisal_goals():
    """Handle goal setting operations - Direct DB"""
    if session.get("login_type") != "employee":
        return jsonify({'error': 'Not authenticated'}), 401
    
    employee_id = session.get('employee_id')
    conn = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        if request.method == 'GET':
            cursor.execute("""
                SELECT goal_id, goal_description, target_date, status, 
                       created_date, updated_date
                FROM EMP_NRM_APPRAISAL_GOALS 
                WHERE employee_id = %s 
                ORDER BY created_date DESC
            """, (employee_id,))
            goals = cursor.fetchall()
            
            return jsonify({'success': True, 'goals': goals})
            
        elif request.method == 'POST':
            data = request.json
            goal_description = data.get('goal_description')
            target_date = data.get('target_date')
            status = data.get('status', 'Planned')
            
            if not goal_description or not target_date:
                return jsonify({'error': 'Goal description and target date are required'}), 400
            
            cursor.execute("""
                INSERT INTO EMP_NRM_APPRAISAL_GOALS 
                (employee_id, goal_description, target_date, status)
                VALUES (%s, %s, %s, %s)
            """, (employee_id, goal_description, target_date, status))
            
            conn.commit()
            
            return jsonify({
                'success': True,
                'message': 'Goal added successfully'
            })
            
    except Exception as e:
        print(f"❌ Appraisal goals error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/appraisal/trainings', methods=['GET', 'POST'])
def appraisal_trainings():
    """Handle training operations - Direct DB"""
    if session.get("login_type") != "employee":
        return jsonify({'error': 'Not authenticated'}), 401
    
    employee_id = session.get('employee_id')
    conn = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        if request.method == 'GET':
            cursor.execute("""
                SELECT training_id, training_name, completion_date, status, 
                       skills_acquired, created_date, updated_date
                FROM EMP_NRM_APPRAISAL_TRAININGS 
                WHERE employee_id = %s 
                ORDER BY created_date DESC
            """, (employee_id,))
            trainings = cursor.fetchall()
            
            return jsonify({'success': True, 'trainings': trainings})
            
        elif request.method == 'POST':
            data = request.json
            training_name = data.get('training_name')
            completion_date = data.get('completion_date')
            status = data.get('status', 'Completed')
            skills_acquired = data.get('skills_acquired', '')
            
            if not training_name or not completion_date:
                return jsonify({'error': 'Training name and completion date required'}), 400
            
            cursor.execute("""
                INSERT INTO EMP_NRM_APPRAISAL_TRAININGS 
                (employee_id, training_name, completion_date, status, skills_acquired)
                VALUES (%s, %s, %s, %s, %s)
            """, (employee_id, training_name, completion_date, status, skills_acquired))
            
            conn.commit()
            
            return jsonify({
                'success': True,
                'message': 'Training added successfully'
            })
            
    except Exception as e:
        print(f"❌ Appraisal trainings error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


        
        
        
@app.route('/api/appraisal/summary')
def appraisal_summary():
    if session.get("login_type") != "employee":
        return jsonify({'error': 'Not authenticated'}), 401

    employee_id = session.get('employee_id')
    conn = get_db_connection()

    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)

        cursor.execute("""
            SELECT 
                e.EMPLOYEE_NAME,
                d.DEPT_NAME,
                des.TITLE AS DESIGNATION,
                a.FINAL_RATING,
                a.COMMENTS,
                a.APPRAISAL_DATE
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_JOB_WORK jw ON e.EMPLOYEE_ID = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS d ON jw.DEPT_ID = d.DEPT_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS des ON jw.DESIGNATION_ID = des.DESIGNATION_ID
            LEFT JOIN EMP_NRM_APPRAISAL_SUMMARY a ON e.EMPLOYEE_ID = a.EMPLOYEE_ID
            WHERE e.EMPLOYEE_ID = %s
            ORDER BY a.APPRAISAL_DATE DESC
            LIMIT 1
        """, (employee_id,))
        appraisal_data = cursor.fetchone() or {}

        cursor.execute(
            "SELECT COUNT(*) CNT FROM EMP_NRM_APPRAISAL_GOALS WHERE EMPLOYEE_ID = %s",
            (employee_id,))
        goals_count = cursor.fetchone()['CNT']

        cursor.execute(
            "SELECT COUNT(*) CNT FROM EMP_NRM_APPRAISAL_TRAININGS WHERE EMPLOYEE_ID = %s AND STATUS = 'Completed'",
            (employee_id,))
        trainings_count = cursor.fetchone()['CNT']

        cursor.close()
        conn.close()

        summary = {
            'employee_info': {
                'name': appraisal_data.get('EMPLOYEE_NAME', ''),
                'department': appraisal_data.get('DEPT_NAME', ''),
                'designation': appraisal_data.get('DESIGNATION', '')
            },
            'appraisal_data': appraisal_data,
            'stats': {
                'goals_count': goals_count,
                'trainings_count': trainings_count
            }
        }

        return jsonify({'success': True, 'summary': summary})

    except Exception as e:
        print("❌ Appraisal summary error:", e)
        return jsonify({'error': str(e)}), 500
# ==========================================
# EMPLOYEE LOGOUT
# ==========================================
@app.route('/Emp-logout')
def Emp_logout():
    """Employee logout"""
    session.clear()
    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('home'))





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
                    f.FEEDBACK_MESSAGE,
                    COALESCE(u.USERNAME, f.NAME, 'Anonymous') as username,
                    f.SUBMITTED_AT
                FROM chakora.NRM_FEEDBACK f
                LEFT JOIN chakora.NRM_USERS u ON f.STUDENT_ID = u.ID
                WHERE f.FEEDBACK_MESSAGE IS NOT NULL 
                  AND TRIM(f.FEEDBACK_MESSAGE) != ''
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


# Serve admin.html page
@app.route('/meeting/admin')
@app.route('/meeting/admin/')
def admin_page():
    try:
        # Serve from templates folder
        return send_from_directory('templates', 'admin.html')
    except Exception as e:
        return f"Error loading page: {str(e)}", 500

# GET /meeting/api/slots - Proxy to Lambda
@app.route('/meeting/api/slots', methods=['GET'])
def proxy_get_slots():
    try:
        # Get date parameter
        date = request.args.get('date')
        if not date:
            return jsonify({'error': 'date parameter required'}), 400
        
        # Get Authorization header from request
        auth_header = request.headers.get('Authorization', '')
        
        # Build Lambda URL
        lambda_url = f'{LAMBDA_URL}slots?date={date}'
        
        print(f"Proxying to Lambda: {lambda_url}")
        print(f"Auth header: {auth_header[:20] if auth_header else 'None'}...")
        
        # Make request to Lambda
        response = requests.get(
            lambda_url,
            headers={'Authorization': auth_header} if auth_header else {},
            timeout=10
        )
        
        print(f"Lambda response status: {response.status_code}")
        print(f"Lambda response: {response.text[:200]}...")
        
        # Return Lambda response
        return make_response(
            response.text,
            response.status_code,
            {'Content-Type': 'application/json'}
        )
        
    except requests.Timeout:
        print("Lambda request timeout")
        return jsonify({'error': 'Request timeout'}), 504
    except requests.RequestException as e:
        print(f"Lambda request error: {str(e)}")
        return jsonify({'error': f'Lambda error: {str(e)}'}), 500
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Server error: {str(e)}'}), 500

# DELETE /meeting/api/cancel - Cancel booking
@app.route('/meeting/api/cancel', methods=['DELETE'])
def proxy_cancel_booking():
    try:
        auth_header = request.headers.get('Authorization', '')
        booking_id = request.args.get('booking_id')
        
        lambda_url = f'{LAMBDA_URL}cancel?booking_id={booking_id}'
        
        response = requests.delete(
            lambda_url,
            headers={'Authorization': auth_header} if auth_header else {},
            timeout=10
        )
        
        return make_response(
            response.text,
            response.status_code,
            {'Content-Type': 'application/json'}
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# GET /meeting/api/mybookings - Get user's bookings
@app.route('/meeting/api/mybookings', methods=['GET'])
def proxy_my_bookings():
    try:
        auth_header = request.headers.get('Authorization', '')
        
        lambda_url = f'{LAMBDA_URL}mybookings'
        
        response = requests.get(
            lambda_url,
            headers={'Authorization': auth_header} if auth_header else {},
            timeout=10
        )
        
        return make_response(
            response.text,
            response.status_code,
            {'Content-Type': 'application/json'}
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# GET /meeting/api/admin/bookings - Proxy to Lambda
@app.route('/meeting/api/admin/bookings', methods=['GET'])
def proxy_admin_bookings():
    try:
        auth_header = request.headers.get('Authorization', '')
        status = request.args.get('status', 'PENDING')
        
        lambda_url = f'{LAMBDA_URL}admin/bookings?status={status}'
        
        response = requests.get(
            lambda_url,
            headers={'Authorization': auth_header} if auth_header else {},
            timeout=10
        )
        
        return make_response(
            response.text,
            response.status_code,
            {'Content-Type': 'application/json'}
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# PUT /meeting/api/admin/approve - Proxy to Lambda
@app.route('/meeting/api/admin/approve', methods=['PUT'])
def proxy_admin_approve():
    try:
        auth_header = request.headers.get('Authorization', '')
        payload = request.get_json()
        
        lambda_url = f'{LAMBDA_URL}admin/approve'
        
        response = requests.put(
            lambda_url,
            json=payload,
            headers={
                'Authorization': auth_header,
                'Content-Type': 'application/json'
            } if auth_header else {'Content-Type': 'application/json'},
            timeout=10
        )
        
        return make_response(
            response.text,
            response.status_code,
            {'Content-Type': 'application/json'}
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# POST /meeting/api/book - Proxy to Lambda
@app.route('/meeting/api/book', methods=['POST'])
def proxy_post_book():
    try:
        # Get Authorization header
        auth_header = request.headers.get('Authorization', '')
        
        # Get JSON payload
        payload = request.get_json()
        if not payload:
            return jsonify({'error': 'Invalid JSON payload'}), 400
        
        # Build Lambda URL
        lambda_url = f'{LAMBDA_URL}book'
        
        print(f"Proxying POST to Lambda: {lambda_url}")
        print(f"Payload: {payload}")
        
        # Make request to Lambda
        response = requests.post(
            lambda_url,
            json=payload,
            headers={
                'Authorization': auth_header,
                'Content-Type': 'application/json'
            } if auth_header else {'Content-Type': 'application/json'},
            timeout=10
        )
        
        print(f"Lambda response status: {response.status_code}")
        print(f"Lambda response: {response.text}")
        
        # Return Lambda response
        return make_response(
            response.text,
            response.status_code,
            {'Content-Type': 'application/json'}
        )
        
    except requests.Timeout:
        print("Lambda request timeout")
        return jsonify({'error': 'Request timeout'}), 504
    except requests.RequestException as e:
        print(f"Lambda request error: {str(e)}")
        return jsonify({'error': f'Lambda error: {str(e)}'}), 500
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Server error: {str(e)}'}), 500

@app.route('/meeting/api/test')
def test_lambda():
    try:
        response = requests.get(f'{LAMBDA_URL}slots?date=2025-10-25', timeout=5)
        return jsonify({
            'lambda_url': LAMBDA_URL,
            'status': response.status_code,
            'response': response.text[:200]
        })
    except Exception as e:
        return jsonify({
            'error': str(e),
            'lambda_url': LAMBDA_URL
        }), 500



# Register



def is_password_valid(password):
    if len(password) < 8:
        return False
    special_chars = re.findall(r'[\W_]', password)
    has_upper = re.search(r'[A-Z]', password)
    has_number = re.search(r'\d', password)
    return len(special_chars) == 1 and has_upper and has_number


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get("username").strip()
        email = request.form.get("email").strip()
        phone = request.form.get("phone").strip()
        location = request.form.get("location").strip()
        gothram = request.form.get("gothram").strip()
        password = request.form.get("password", "").strip()
        confirm_password = request.form.get("confirm_password", "").strip()

        # Phone validation
        if not phone.isdigit() or len(phone) != 10:
            flash("Phone number must be exactly 10 digits.", "danger")
            return redirect(url_for('register'))

        # Password validation
        if not is_password_valid(password):
            flash(
                "Password must contain exactly one special character, one uppercase letter, and one number.",
                "danger"
            )
            return redirect(url_for('register'))

        if password != confirm_password:
            flash("Passwords do not match.", "danger")
            return redirect(url_for('register'))

        try:
            conn = get_db_connection()
            cur = conn.cursor(snowflake.connector.DictCursor)

            # 🔍 Admin-precreated user check
            cur.execute("""
                SELECT ID
                FROM NRM_USERS
                WHERE EMAIL = %s
                  AND USERNAME = %s
            """, (email, username))

            user = cur.fetchone()

            if not user:
                flash("You are not registered by admin.", "danger")
                return redirect(url_for('register'))

            user_id = user['ID']

            # 🚫 Prevent re-registration
            cur.execute(
                "SELECT 1 FROM NRM_LOGINS WHERE USER_ID = %s",
                (user_id,)
            )
            if cur.fetchone():
                flash("Account already activated. Please log in.", "info")
                return redirect(url_for('home'))

            # 🔐 HASH PASSWORD (THIS WAS MISSING)
            hashed_password = generate_password_hash(password)

            # 🔐 Create login
            cur.execute("""
                INSERT INTO NRM_LOGINS
                (USER_ID, PASSWORD, IS_ACTIVE)
                VALUES (%s, %s, 'N')
            """, (user_id, hashed_password))

            # 🎓 Create student profile
            cur.execute("""
                INSERT INTO NRM_STUDENTS
                (USER_ID, GOTHRAM, LOCATION, REGISTRATION_SOURCE)
                VALUES (%s, %s, %s, 'public')
            """, (user_id, gothram, location))

            # 🔁 Update usertype to student
            cur.execute("""
                UPDATE NRM_USERS
                SET USERTYPE = 'student'
                WHERE ID = %s
            """, (user_id,))

            conn.commit()
            flash("Registration successful. Please log in.", "success")
            return redirect(url_for('home'))

        except Exception as e:
            conn.rollback()
            print("DB ERROR:", e)
            flash("Something went wrong. Please contact admin.", "danger")
            return redirect(url_for('register'))

        finally:
            cur.close()
            conn.close()

    return render_template("register-full.html")

        

            
            
            
    


        

  
#OFFERS PAGE:

@app.route('/offers_page', methods=['GET', 'POST'])
def offers_page():
    if 'user' not in session:
        return redirect(url_for('user_nrm_logins'))

    conn = get_db_connection()
    cursor = conn.cursor(snowflake.connector.DictCursor)

    cursor.execute("SELECT id, course_name, course_fee FROM nrm_courses ORDER BY course_name ASC")
    courses = cursor.fetchall()

    cursor.execute("""
        SELECT o.id, o.discount_percentage, o.valid_from, o.valid_to, o.is_active,
               c.course_name, c.course_fee
        FROM nrm_offers o
        JOIN nrm_courses c ON o.course_id = c.id
        ORDER BY o.created_at DESC
    """)
    offers = cursor.fetchall()

    cursor.close()
    conn.close()

    # ✅ Apply mapping with normalize
    for c in courses:
        db_name_norm = normalize(c["course_name"])
        c["display_name"] = COURSE_MAP.get(db_name_norm, c["course_name"])

    for o in offers:
        db_name_norm = normalize(o["course_name"])
        o["display_name"] = COURSE_MAP.get(db_name_norm, o["course_name"])

    return render_template("offers.html", courses=courses, offers=offers)

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
    cur = conn.cursor(snowflake.connector.DictCursor)

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
    cur = conn.cursor(snowflake.connector.DictCursor)

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
    conn = get_db_connection()
    cur = conn.cursor(snowflake.connector.DictCursor)

    # Get all courses
    cur.execute("SELECT ID, COURSE_NAME FROM NRM_COURSES")
    courses = cur.fetchall()

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
@app.route('/generate-student-report')
def generate_student_report():
    connection = get_db_connection()
    report_data = {}

    if connection:
        cursor = connection.cursor(snowflake.connector.DictCursor)

        # Get all courses
        cursor.execute("SELECT ID, COURSE_NAME FROM NRM_COURSES ORDER BY COURSE_NAME")
        courses = cursor.fetchall()

        for course in courses:
            course_id = course['ID']           # ✅ FIX
            course_name = course['COURSE_NAME']

            # Get total students for this course
            cursor.execute("""
                SELECT COUNT(*) as total_students
                FROM nrm_registrations
                WHERE course_id = %s
            """, (course_id,))
        total_students = cursor.fetchone()['TOTAL_STUDENTS']
        
            # Get students with feedback for this course
        cursor.execute("""
                SELECT COUNT(DISTINCT f.student_id) as feedback_count
                FROM nrm_feedback f
                JOIN nrm_registrations r ON f.student_id = r.student_id
                WHERE r.course_id = %s
            """, (course_id,))
        row = cursor.fetchone()
        feedback_count = row['FEEDBACK_COUNT'] if row else 0

            # Calculate completion percentage
        completion_percentage = (feedback_count / total_students * 100) if total_students > 0 else 0

        report_data[course_name] = {
                'total_students': total_students,
                'feedback_count': feedback_count,
                'completion_percentage': round(completion_percentage, 2)
            }

        cursor.close()
        connection.close()

    return render_template('generate-student-report.html', report_data=report_data)

@app.route("/student-report-view")
def student_report_view():
    reg_id = request.args.get("reg_id")

    if not reg_id:
        return "Registration ID missing", 400

    try:
        conn = get_db_connection()
        if not conn:
            return "DB connection failed", 500

        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                u.USERNAME            AS FIRST_NAME,
                ''                    AS LAST_NAME,
                'N/A'                 AS ADDRESS,
                'N/A'                 AS EMPLOYED,
                'N/A'                 AS EXPERIENCE,
                r.REGISTRATION_ID,
                c.COURSE_NAME
            FROM NRM_REGISTRATIONS r
            JOIN NRM_USERS u
              ON r.STUDENT_ID = u.ID
            LEFT JOIN NRM_COURSES c
              ON r.COURSE_ID = c.ID
            WHERE r.REGISTRATION_ID = %s
        """, (reg_id,))

        student = cursor.fetchone()
        cursor.close()
        conn.close()

        if not student:
            return "Student not found", 404

        return render_template("report.html", student=student)

    except Exception as e:
        print("❌ ERROR in student_report_view:", e)
        return str(e), 500

# ==========================================
# FETCH STUDENTS FOR REPORT PAGE
# ==========================================

@app.route("/students")
def fetch_students():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"status": "error", "message": "DB connection failed"})

        cursor = conn.cursor()

        cursor.execute("""
            
            SELECT 
                u.USERNAME,
                r.REGISTRATION_ID,
                u.EMAIL,
                u.PHONE,
                'N/A' AS ADDRESS
            FROM NRM_USERS u
            JOIN NRM_REGISTRATIONS r
            ON u.ID = r.STUDENT_ID
            LIMIT 20
        """)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        data = []
        for r in rows:
            data.append({
                "first_name": r[0],
                "registration_id": r[1],
                "email": r[2],
                "phone": r[3],
                "address": r[4]
            })

        return jsonify({"status": "success", "data": data})

    except Exception as e:
        print("❌ ERROR in /students:", e)
        return jsonify({"status": "error", "message": str(e)})
        
@app.route("/generate-student-report")
def student_report_page():
    return render_template("generate-student-report.html")

@app.route('/delete_student/<registration_id>', methods=['POST'])
def delete_student(registration_id):
    connection = get_db_connection()
    if not connection:
        return jsonify({"success": False, "message": "Database connection failed."})

    try:
        cursor = connection.cursor(snowflake.connector.DictCursor)

        # Get student_id and user_id via registration_id
        cursor.execute("""
            SELECT r.student_id, u.id as user_id, u.username
            FROM nrm_registrations r
            JOIN nrm_students s ON r.student_id = s.id
            JOIN nrm_users u ON u.email = s.email
            WHERE r.registration_id = %s
        """, (registration_id,))
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "message": "Student not found."})

        student_id = row["student_id"]
        user_id = row["user_id"]
        username = row["username"]

        # Delete dependencies in correct order
        cursor.execute("DELETE FROM nrm_feedback WHERE student_id = %s", (student_id,))
        cursor.execute("DELETE FROM nrm_enquiries WHERE student_id = %s", (student_id,))
        cursor.execute("DELETE FROM nrm_meeting_queue WHERE student_id = %s", (student_id,))
        cursor.execute("DELETE FROM nrm_registrations WHERE student_id = %s", (student_id,))
        cursor.execute("DELETE FROM nrm_billing_entries WHERE user_id = %s", (user_id,))
        cursor.execute("DELETE FROM nrm_session_bookings WHERE username = %s", (username,))
        cursor.execute("DELETE FROM nrm_logins WHERE user_id = %s", (user_id,))
        cursor.execute("DELETE FROM nrm_users WHERE id = %s", (user_id,))
        cursor.execute("DELETE FROM nrm_students WHERE id = %s", (student_id,))

        connection.commit()
        return jsonify({"success": True, "message": "Student deleted successfully."})

    except Exception as e:
        connection.rollback()
        print(f"❌ Delete Error: {e}")
        return jsonify({"success": False, "message": f"Error deleting student: {str(e)}"})

    finally:
        cursor.close()
        connection.close()

# Set session lifetime globally
# Keep the user logged in for 7 days unless they logout
app.permanent_session_lifetime = timedelta(days=7)

                 

@app.route("/aboutus")
def aboutus():
    return render_template("aboutus.html")


# -------------------------------------------------------
# ADMIN REGISTER (GET + POST) — FINAL & CORRECT
# -------------------------------------------------------
@app.route('/validate_admin_registration', methods=['POST'])
def validate_admin_registration():
    data = request.json or {}
    email = data.get('email', '').strip()
    phone = data.get('phone', '').strip()

    if not email or not phone:
        return jsonify({'valid': False, 'message': 'Email and phone are required'})

    conn = get_db_connection()
    if not conn:
        return jsonify({'valid': False, 'message': 'Database error'})

    try:
        cur = conn.cursor()

        # 🔐 CORRECT QUERY: Join through nrm_students -> nrm_users
        cur.execute("""
            SELECT 1
            FROM nrm_registrations r
            JOIN nrm_students s ON r.student_id = s.id
            JOIN nrm_users u ON s.user_id = u.id
            WHERE u.email = %s AND u.phone = %s
        """, (email, phone))

        if not cur.fetchone():
            return jsonify({
                'valid': False,
                'message': 'User not found in admin registration'
            })

        return jsonify({
            'valid': True,
            'message': 'Admin registration verified'
        })

    except Exception as e:
        print("Validation error:", e)
        return jsonify({'valid': False, 'message': 'Validation error'})
    finally:
        cur.close()
        conn.close()

@app.route("/admin-register", methods=["GET", "POST"])
def admin_register():
    connection = get_db_connection()
    courses, languages = [], []

    # ---------- LOAD DROPDOWNS ----------
    if connection:
        cursor = connection.cursor(snowflake.connector.DictCursor)

        cursor.execute("SELECT id, course_name, course_code FROM nrm_courses ORDER BY course_name")
        rows = cursor.fetchall()
        courses = [{"ID": r["ID"], "COURSE_NAME": r["COURSE_NAME"], "COURSE_CODE": r["COURSE_CODE"]} for r in rows]

        cursor.execute("SELECT id, language FROM nrm_languages ORDER BY language")
        rows = cursor.fetchall()
        languages = [{"ID": r["ID"], "LANGUAGE": r["LANGUAGE"]} for r in rows]

        cursor.close()

    # ---------- GET REQUEST ----------
    if request.method == "GET":
        return render_template("admin-register.html", courses=courses, languages=languages)

    # ---------- POST REQUEST (JSON) ----------
    data = request.get_json()
    
    if not data:
        return jsonify({"success": False, "message": "No data received"}), 400

    fname = data.get("first_name", "").strip()
    lname = data.get("last_name", "").strip()
    email = data.get("email", "").strip().lower()
    phone = data.get("phone", "").strip()
    location_val = data.get("location", "").strip()
    course_raw = data.get("course")
    language_raw = data.get("language")
    start_date_raw = data.get("start_date", "").strip()

    # ---------- FIELD VALIDATION ----------
    if not fname:
        return jsonify({"success": False, "message": "First name is required."}), 400

    if not lname:
        return jsonify({"success": False, "message": "Last name is required."}), 400

    if not email:
        return jsonify({"success": False, "message": "Email is required."}), 400

    if not (email.endswith("@gmail.com") or email.endswith("@chakorahub.com")):
        return jsonify({"success": False, "message": "Email must end with @gmail.com or @chakorahub.com"}), 400

    if not phone.isdigit() or len(phone) != 10:
        return jsonify({"success": False, "message": "Phone number must be exactly 10 digits."}), 400

    if not location_val:
        return jsonify({"success": False, "message": "Location is required."}), 400

    try:
        course_id = int(course_raw)
        language_id = int(language_raw)
    except:
        return jsonify({"success": False, "message": "Invalid course or language."}), 400

    if not start_date_raw:
        return jsonify({"success": False, "message": "Start date is required."}), 400

    try:
        start_date = datetime.strptime(start_date_raw, "%Y-%m-%d")
    except:
        return jsonify({"success": False, "message": "Invalid date format."}), 400

    # ---------- DATABASE INSERT ----------
    cursor = None
    try:
        cursor = connection.cursor()

        # Check for duplicate email in nrm_users
        cursor.execute("SELECT 1 FROM chakora.nrm_users WHERE UPPER(email) = UPPER(%s)", (email,))
        if cursor.fetchone():
            return jsonify({"success": False, "message": "Email already exists."}), 400

        # Check for duplicate phone in nrm_users
        cursor.execute("SELECT 1 FROM chakora.nrm_users WHERE phone = %s", (phone,))
        if cursor.fetchone():
            return jsonify({"success": False, "message": "Phone number already exists."}), 400

        # Get course code
        cursor.execute("SELECT course_code FROM chakora.nrm_courses WHERE id = %s", (course_id,))
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "message": "Invalid course selected."}), 400
        course_code = row[0]

        # Get active status ID
        cursor.execute("SELECT id FROM chakora.nrm_statuses WHERE UPPER(status) = %s LIMIT 1", ('ACTIVE',))
        row = cursor.fetchone()
        if not row:
            return jsonify({"success": False, "message": "Active status missing in DB."}), 400
        active_id = row[0]

        # Generate registration ID
        cursor.execute("SELECT COUNT(*) FROM chakora.nrm_registrations WHERE course_id = %s", (course_id,))
        seq = cursor.fetchone()[0] + 1
        initials = fname[0].upper() + lname[0].upper()
        reg_id = f"{course_code}{initials}{str(seq).zfill(3)}{start_date.strftime('%d%m')}"

        # Step 1: Insert into nrm_users FIRST (username = "firstname lastname")
        full_name = f"{fname} {lname}"
        cursor.execute("""
            INSERT INTO chakora.nrm_users (username, email, phone, profile_pic, usertype, registration_source)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (full_name, email, phone, 'default.jpg', 'student', 'admin'))
        
        cursor.execute("SELECT id FROM chakora.nrm_users WHERE UPPER(email) = UPPER(%s) ORDER BY id DESC LIMIT 1", (email,))
        user_id = cursor.fetchone()[0]

        # Step 2: Insert into nrm_students (ONLY has: location, registration_source, user_id)
        cursor.execute("""
            INSERT INTO chakora.nrm_students (location, registration_source, user_id)
            VALUES (%s, %s, %s)
        """, (location_val, 'admin', user_id))

        cursor.execute("SELECT id FROM chakora.nrm_students WHERE user_id = %s ORDER BY id DESC LIMIT 1", (user_id,))
        student_id = cursor.fetchone()[0]

        # Step 3: Insert into nrm_logins (user_id, password, is_active)
        from werkzeug.security import generate_password_hash
        hashed_pwd = generate_password_hash("changeme123")
        cursor.execute("""
            INSERT INTO chakora.nrm_logins (user_id, password, is_active)
            VALUES (%s, %s, %s)
        """, (user_id, hashed_pwd, 'Y'))

        # Step 4: Insert into nrm_registrations
        cursor.execute("""
            INSERT INTO chakora.nrm_registrations
            (registration_id, student_id, course_id, language_id, start_date, status_id, created_dt)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (reg_id, student_id, course_id, language_id, start_date_raw, active_id, datetime.now()))

        connection.commit()
        
        return jsonify({
            "success": True, 
            "message": f"Registration successful! ID: {reg_id}"
        }), 200

    except Exception as e:
        if connection:
            connection.rollback()
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False, 
            "message": f"Error: {str(e)}"
        }), 500

    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if connection:
            try: connection.close()
            except: pass


# ---------- DUPLICATE CHECKS ----------
@app.route('/check_admin_email', methods=['POST'])
def check_admin_email():
    email = request.json.get('email', '').strip()
    exists = False
    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT 1 FROM nrm_users WHERE email = %s", (email,))
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


           
   

#nrm_enquiries
# Zapier Webhook URL
ZAPIER_WEBHOOK_URL = "https://hooks.zapier.com/hooks/catch/25218395/u8dirlt/"

# ==========================================
# ENQUIRY ROUTES (Proxy to Student Service)
# ==========================================
@app.route('/submit_nrm_enquiries', methods=['POST'])
def submit_nrm_enquiries():
    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
    
    name = request.form.get('name', '').strip()
    email = request.form.get('email', '').strip()
    phone = request.form.get('phone', '').strip()
    enquiry_text = request.form.get('enquiry', '').strip()

    if not all([name, email, phone, enquiry_text]):
        return jsonify({'success': False, 'message': 'All fields are required.'}) if is_ajax else redirect(url_for('home'))

    # Step Functions Payload
    payload = {
        "name": name,
        "email": email,
        "phone": phone,
        "enquiry": enquiry_text,
        "source": "guest"
    }

    try:
        # TRIGGER ONLY - Don't do a manual SQL insert here!
        sf_client.start_execution(
            stateMachineArn="arn:aws:states:eu-north-1:196527705786:stateMachine:ChakoraHub-Enquiry",
            input=json.dumps(payload)
        )
        
        if is_ajax:
            return jsonify({'success': True, 'message': 'Enquiry submitted successfully!'})
        return redirect(url_for('home'))

    except Exception as e:
        print(f"❌ AWS Error: {e}")
        return jsonify({'success': False, 'message': str(e)}) if is_ajax else redirect(url_for('home'))
    
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
import traceback

# Allowed extensions per category
BASE_UPLOADS = os.path.join(os.getcwd(), "uploads")
app.config['UPLOAD_FOLDER'] = BASE_UPLOADS
app.config['SYLLABUS_FOLDER'] = os.path.join(BASE_UPLOADS, "syllabus")

#------------------ Syllabus Routes ------------------#
# ========================= SYLLABUS MAIN PAGE =========================
@app.route('/syllabus', methods=["GET", "POST"])
def syllabus_page():
    conn = get_db_connection()
    cursor = conn.cursor(snowflake.connector.DictCursor)

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
        cursor = conn.cursor(snowflake.connector.DictCursor)
        cursor.execute("SELECT profile_pic FROM nrm_users WHERE username = %s", (username,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        if user and user['profile_pic']:
            user_image = user['profile_pic']

    return render_template('syllabus.html', courses=courses, user_image=user_image)

# ========================= FETCH A SPECIFIC SYLLABUS =========================
@app.route('/syllabus/<int:course_id>')
def get_syllabus(course_id):
    conn = get_db_connection()
    cursor = conn.cursor(snowflake.connector.DictCursor)

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

    return jsonify({
        'course_name': course['course_name'],
        'pdf_url': course['file_path'],
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
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute("SELECT file_path FROM nrm_syllabus WHERE id = %s", (course_id,))
    result = cursor.fetchone()

    if result:
        try:
            file_path = result['file_path']
            # Ensure path is safe
            if file_path and os.path.exists(file_path.lstrip('/')):
                os.remove(file_path.lstrip('/'))

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

# ================== Upload Page ================== #
@app.route('/admin/upload', methods=['GET'])
def upload_page():
    try:
        if 'user' not in session:
            return "<h3>Access denied</h3>"

        username = session['user']
        conn = get_db_connection()
        cursor = conn.cursor(snowflake.connector.DictCursor)
        cursor.execute("SELECT usertype FROM nrm_users WHERE email = %s", (username,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if not user or user['usertype'].lower() != 'admin':
            return "<h3>Access denied</h3>"

        session['role'] = 'admin'

        # ✅ Build uploaded_files dict
        uploaded_files = {}
        for ftype in app.config['ALLOWED_EXTENSIONS'].keys():
            # 🔹 Special handling for practice_test
            if ftype == "practice_test":
                base_folder = app.config['UPLOAD_FOLDERS']['practice_tests']
            else:
                base_folder = os.path.join(app.config['UPLOAD_FOLDER'], ftype)

            if os.path.exists(base_folder):
                files_by_cat = []
                for category in os.listdir(base_folder):
                    cat_folder = os.path.join(base_folder, category)
                    if os.path.isdir(cat_folder):
                        for fname in os.listdir(cat_folder):
                            files_by_cat.append({
                                "name": fname,
                                "category": category,
                                "path": f"{ftype}/{category}/{fname}"
                            })
                uploaded_files[ftype] = files_by_cat
            else:
                uploaded_files[ftype] = []

        return render_template(
            'upload.html',
            username=username,
            usertype=user['usertype'],
            uploaded_files=uploaded_files
        )

    except Exception:
        return f"<pre>{traceback.format_exc()}</pre>"

# ================== Upload Handler ================== #
@app.route('/admin/upload_file/<file_type>', methods=['POST'])
def upload_file_handler(file_type):
    try:
        if 'file' not in request.files:
            flash("No file selected.")
            return redirect(url_for('upload_page'))

        file = request.files['file']
        category = request.form.get("category")

        if file.filename == '':
            flash("No file selected.")
            return redirect(url_for('upload_page'))

        filename = secure_filename(file.filename)
        ext = filename.rsplit('.', 1)[-1].lower()

        # ✅ Validate extension
        if file_type not in app.config['ALLOWED_EXTENSIONS'] or ext not in app.config['ALLOWED_EXTENSIONS'][file_type]:
            flash("❌ Invalid file type or extension.")
            return redirect(url_for('upload_page'))

        # ✅ Build upload folder
        if file_type == "practice_test":
            base_folder = app.config['UPLOAD_FOLDERS']['practice_tests']
        else:
            base_folder = os.path.join(app.config['UPLOAD_FOLDER'], file_type)

        upload_folder = os.path.join(base_folder, category)
        os.makedirs(upload_folder, exist_ok=True)

        file.save(os.path.join(upload_folder, filename))

        flash(f"✅ {file_type.upper()} uploaded successfully!")
        return redirect(url_for('upload_page'))

    except Exception as e:
        print(f"❌ Upload error: {e}")
        flash("❌ Upload failed. Please try again.")
        return redirect(url_for('upload_page'))

# ================== Delete File ================== #
@app.route('/delete_file', methods=['POST'])
def delete_file():
    if 'user' not in session:
        return "<h3>Access denied</h3>"

    email = session['user']
    conn = get_db_connection()
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute("SELECT usertype FROM nrm_users WHERE email = %s", (email,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()

    if not user or user['usertype'].lower() != 'admin':
        return "<h3>Access denied: Admins only</h3>"

    rel_path = request.form.get('file_path')
    if not rel_path:
        flash("No file path provided.", "warning")
        return redirect(url_for('upload_page'))

    parts = rel_path.split('/')
    file_type = parts[0]
    category = parts[1] if len(parts) > 1 else None

    # ✅ Resolve correct base folder
    if file_type == "practice_test":
        base_folder = app.config['UPLOAD_FOLDERS']['practice_tests']
    else:
        base_folder = os.path.join(app.config['UPLOAD_FOLDER'], file_type)

    abs_path = os.path.join(base_folder, *parts[1:])

    try:
        if os.path.exists(abs_path):
            os.remove(abs_path)
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

# ================== Category View ================== #
@app.route('/view/<tech>/<file_type>')
def view_category_files(tech, file_type):
    usertype = session.get('usertype', 'public')

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
        'category_view.html',
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

#@app.route('/meeting')
#def meeting_home():
#    """Render the meeting join page."""
#    return render_template('meeting.html')

@app.route('/meeting')
def meeting_home():
    """Render the meeting join page."""
    try:
        # Check if user is logged in
        # if 'user' not in session:
        #    return redirect(url_for('user_nrm_logins'))  # Redirect to login if not authenticated
        
        # Get user details from session
        username = session.get('username', 'Guest')
        user_type = session.get('user_type', 'new')  # 'new' or 'existing'
        
        return render_template('meeting.html', 
                             username=username, 
                             user_type=user_type)
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error in /meeting route: {error_details}")
        return f"Error loading meeting page: {str(e)}", 500

@app.route('/meeting/join', methods=['POST'])
def join_queue():
    """Add user to the queue."""
    global meeting_start_time, meeting_end_time

    username = request.form.get('username')
    if not username:
        return "Username required", 400

    with queue_lock:
        # Start meeting session if not started
        if meeting_start_time is None:
            meeting_start_time = datetime.now()
            meeting_end_time = meeting_start_time + TOTAL_MEETING_TIME

        # Check if meeting time is over
        if datetime.now() > meeting_end_time:
            return jsonify({'status': 'ended', 'message': 'Meeting has ended.'})

        # Add user if not already in queue
        if username not in [u['username'] for u in nrm_meeting_queue]:
            nrm_meeting_queue.append({
                'username': username,
                'join_time': datetime.now(),
                'start_time': None,
                'end_time': None,
                'status': 'waiting'
            })

    return jsonify({'status': 'joined', 'message': 'You joined the queue.'})

@app.route('/meeting/next', methods=['GET'])
def next_user():
    """Assign next user their time slot."""
    with queue_lock:
        if not nrm_meeting_queue:
            return jsonify({'status': 'empty', 'message': 'No users in queue.'})

        current_time = datetime.now()

        # Check current user
        for user in nrm_meeting_queue:
            if user['status'] == 'in_progress':
                if current_time >= user['end_time']:
                    user['status'] = 'done'
                    continue
                else:
                    remaining_time = (user['end_time'] - current_time).seconds
                    return jsonify({'status': 'in_progress',
                                    'user': user['username'],
                                    'remaining_time': remaining_time})

        # Start next waiting user
        for user in nrm_meeting_queue:
            if user['status'] == 'waiting':
                user['start_time'] = current_time
                user['end_time'] = min(current_time + USER_TIME_LIMIT, meeting_end_time)
                user['status'] = 'in_progress'
                remaining_time = (user['end_time'] - current_time).seconds
                return jsonify({'status': 'in_progress',
                                'user': user['username'],
                                'remaining_time': remaining_time})

        return jsonify({'status': 'ended', 'message': 'Meeting time is over or no waiting users.'})

@app.route('/meeting/leave', methods=['POST'])
def leave():
    """Mark current user as done."""
    username = request.form.get('username')
    with queue_lock:
        for user in nrm_meeting_queue:
            if user['username'] == username and user['status'] == 'in_progress':
                user['status'] = 'done'
                break
    return jsonify({'status': 'left'})

@app.route('/meeting/queue')
def queue_status():
    """Render the queue status_id page."""
    with queue_lock:
        return render_template('queue.html', queue=nrm_meeting_queue, end_time=meeting_end_time)

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
    cursor = conn.cursor(snowflake.connector.DictCursor)

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

    try:
        response = requests.post(
            f"{STUDENT_SERVICE_URL}/api/student/profile",
            json={"user_id": user_id},
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            address = data.get("address", "")
            return render_template('profile.html', address=address)
        else:
            return render_template('profile.html', address="")

    except requests.RequestException as e:
        print(f"❌ Profile service error: {e}")
        return render_template('profile.html', address="")

@app.route('/save_address', methods=['POST'])
def save_address():
    """Save student address"""
    user_id = session.get('user_id')

    if not user_id:
        flash("Please login first.")
        return redirect(url_for('home'))

    address = request.form.get('address', '')

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

# ✅ Helper to fetch all nrm_festivals from DB as dictionary
def get_nrm_festivals():
    conn = get_db_connection()
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute("SELECT festival_date, festival_name FROM nrm_festivals")
    rows = cursor.fetchall()
    conn.close()
    # Convert to { 'YYYY-MM-DD': 'Festival Name', ... }
    return {row['festival_date'].strftime('%Y-%m-%d'): row['festival_name'] for row in rows}

# ================== 📅 Calendar Route ==================
@app.route('/calendar')
def calendar_page():
    try:
        month = int(request.args.get('month', datetime.now().month))
        year = int(request.args.get('year', datetime.now().year))
    except ValueError:
        month = datetime.now().month
        year = datetime.now().year

    conn = get_db_connection()
    cursor = conn.cursor(snowflake.connector.DictCursor)

    # Step 1: Get festivals
    cursor.execute("""
        SELECT festival_date, festival_name
        FROM nrm_festivals
        WHERE MONTH(festival_date) = %s AND YEAR(festival_date) = %s
    """, (month, year))
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
        WHERE MONTH(s.session_date) = %s AND YEAR(s.session_date) = %s
        ORDER BY s.session_date, t.slot_label
    """, (month, year))
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
            status = (
                "Booked"
                if slot in booked_slots_dict.get(date_str, set())
                else "Not booked"
            )
            bookings_status.append({
                "slot": slot,
                "status": status
            })

        calendar_data[date_str] = {
            "festival": festival,
            "bookings": bookings_status
        }

    return render_template(
        'calendar.html',
        month=month,
        year=year,
        calendar_data=calendar_data,
        session_role=session.get('usertype', ''),
        username=session.get('user', '').split('@')[0]
    )

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
    if 'file' not in request.files:
        flash("No file selected.")
        return redirect(url_for('upload_page'))

    file = request.files['file']
    subject = request.form.get('subject')

    if file.filename == '':
        flash("No file selected.")
        return redirect(url_for('upload_page'))

    filename = secure_filename(file.filename)
    ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''

    # ✅ Validate subject + extension
    if subject not in app.config['ALLOWED_SUBJECTS']:
        flash("Invalid subject.")
        return redirect(url_for('upload_page'))

    if ext not in app.config['ALLOWED_EXTENSIONS']['practice_test']:
        flash("Invalid file type for practice test.")
        return redirect(url_for('upload_page'))

    # ✅ Get central folder
    upload_folder = os.path.join(app.config['UPLOAD_FOLDERS']['practice_tests'], subject)
    os.makedirs(upload_folder, exist_ok=True)
    file_path = os.path.join(upload_folder, filename)

    try:
        file.save(file_path)
        flash('✅ Practice test uploaded successfully!')
    except Exception as e:
        print(f"❌ Error saving file: {e}")
        flash('❌ Failed to upload file.')

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
@app.route('/generate-certificate', methods=['GET', 'POST'])
def generate_certificate():
    reg_id = None
    student = None
    status_id = None
    status_name = None
    registration_date = None  # ✅ ADD THIS

    if request.method == 'POST':
        reg_id = request.form.get('reg_id', '').strip()

        conn = get_db_connection()
        cursor = conn.cursor(snowflake.connector.DictCursor)

        query = """
            SELECT
                u.USERNAME AS STUDENT_NAME,
                c.COURSE_NAME,
                r.STATUS_ID,
                st.STATUS AS STATUS_NAME,
                COALESCE(r.START_DATE, r.CREATED_DT) AS REGISTRATION_DATE
            FROM NRM_REGISTRATIONS r
            JOIN NRM_STUDENTS s ON r.STUDENT_ID = s.ID
            JOIN NRM_USERS u ON s.USER_ID = u.ID
            JOIN NRM_COURSES c ON r.COURSE_ID = c.ID
            LEFT JOIN NRM_STATUSES st ON r.STATUS_ID = st.ID
            WHERE r.REGISTRATION_ID = %s
        """
        cursor.execute(query, (reg_id,))
        student = cursor.fetchone()

        if student:
            status_id = student["STATUS_ID"]
            status_name = student["STATUS_NAME"]
            
            # ✅ ADD DATE FORMATTING
            reg_date = student["REGISTRATION_DATE"]
            if reg_date:
                registration_date = reg_date.strftime("%d %B %Y")
            else:
                registration_date = datetime.now().strftime("%d %B %Y")

        cursor.close()
        conn.close()

    return render_template(
        'generate-certificate.html',
        reg_id=reg_id,
        student=student,
        status_id=status_id,
        status_name=status_name,
        current_date=registration_date  # ✅ PASS THIS
    )
# ---------- Blogger Config ----------
app.config['BLOGGER_UPLOAD_FOLDER'] = '/home/vsrsubhash/uploads/blogger'

# ---------- Blogger Upload ----------
@app.route('/admin/upload/blogger', methods=['POST'])
def upload_blogger():
    if 'user' not in session or session.get('role') != 'admin':
        return "<h3>Access denied</h3>"

    file = request.files.get('file')
    if not file or file.filename == '':
        flash('Please select a file.')
        return redirect(url_for('upload_page'))

    filename = secure_filename(file.filename)
    save_path = os.path.join(app.config['BLOGGER_UPLOAD_FOLDER'], filename)

    try:
        file.save(save_path)
        flash('Blog uploaded successfully.')
    except Exception as e:
        flash(f"Upload failed: {e}", "danger")
        traceback.print_exc()

    return redirect(url_for('upload_page'))

# ---------- Blogger Display ----------
@app.route('/blogger')
def blogger_page():
    month = request.args.get('month', type=int)  # month number 1-12
    months_list = [(i, calendar.month_name[i]) for i in range(1, 13)]

    try:
        files = os.listdir(app.config['BLOGGER_UPLOAD_FOLDER'])
        files = [f for f in files if os.path.isfile(os.path.join(app.config['BLOGGER_UPLOAD_FOLDER'], f))]

        if month:
            filtered = []
            for f in files:
                file_path = os.path.join(app.config['BLOGGER_UPLOAD_FOLDER'], f)
                mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                if mtime.month == month:
                    filtered.append(f)
            files = filtered

    except Exception as e:
        flash(f"Error loading files: {e}", "danger")
        traceback.print_exc()
        files = []

    return render_template(
        'blogger.html',
        files=files,
        months=months_list,
        selected_month=calendar.month_name[month] if month else None
    )

# ---------- Serve Blogger File ----------
@app.route('/blogger/<filename>')
def serve_blog_file(filename):
    try:
        return send_from_directory(app.config['BLOGGER_UPLOAD_FOLDER'], filename)
    except Exception as e:
        flash(f"Error loading file: {e}", "danger")
        traceback.print_exc()
        return redirect(url_for('blogger_page'))

# ---------- Delete Blogger File ----------
@app.route('/blogger/delete/<filename>', methods=['POST'])
def delete_blogger_file(filename):
    try:
        file_path = os.path.join(app.config['BLOGGER_UPLOAD_FOLDER'], filename)

        if os.path.exists(file_path) and os.path.isfile(file_path):
            os.remove(file_path)
            flash("Deleted successfully", "success")
        else:
            flash("File not found.", "danger")

    except Exception as e:
        flash(f"Error deleting file: {e}", "danger")
        traceback.print_exc()

    return redirect(url_for('blogger_page'))

# ------------------ Book Session + My Bookings (Combined) ------------------
@app.route('/book-session', methods=['GET', 'POST'])
def book_session():
    if 'user' not in session:
        return redirect(url_for('user_nrm_logins'))

    email = session['user']
    conn = get_db_connection()
    cursor = conn.cursor(snowflake.connector.DictCursor)

    # Fetch username
    cursor.execute("SELECT username FROM nrm_users WHERE email = %s", (email,))
    user_row = cursor.fetchone()
    if not user_row:
        flash("User not found in nrm_users table.")
        cursor.close()
        conn.close()
        return redirect(url_for('user_nrm_logins'))

    username = user_row['username']

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

        course_id = course_row['id']

        # Get time slot id
        cursor.execute("SELECT id FROM nrm_time_slots WHERE slot_label = %s", (selected_time,))
        time_slot_row = cursor.fetchone()
        if not time_slot_row:
            flash("Time slot not found.")
            cursor.close()
            conn.close()
            return redirect(url_for('book_session'))

        time_slot_id = time_slot_row['id']

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

        # Insert booking
        cursor.execute("""
            INSERT INTO nrm_session_bookings (username, course_id, session_date, time_slot_id)
            VALUES (%s, %s, %s, %s)
        """, (username, course_id, selected_date, time_slot_id))
        conn.commit()
        flash("✅ Session booked successfully!")

    # ---------- Fetch User's Bookings ----------
    query = """
        SELECT b.id, c.course_name, b.session_date, t.slot_label
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
# REDIS CONFIGURATION (Add after imports)
# ==========================================
REDIS_HOST = os.getenv('REDIS_HOST', '13.62.242.164')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

# Create Redis connection
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_connect_timeout=5
    )
    redis_client.ping()
    print("✅ Redis connected successfully")
except Exception as e:
    print(f"⚠️ Redis connection failed: {e}")
    redis_client = None

# Cache TTL settings
CACHE_TTL_SHORT = 300  # 5 minutes
CACHE_TTL_MEDIUM = 1800  # 30 minutes
CACHE_TTL_LONG = 3600  # 1 hour

# ==========================================
# REDIS HELPER FUNCTIONS
# ==========================================
def cache_get(key: str):
    """Get value from Redis cache"""
    if not redis_client:
        return None
    try:
        value = redis_client.get(key)
        if value:
            return json.loads(value)
        return None
    except Exception as e:
        print(f"⚠️ Redis GET error: {e}")
        return None

def cache_set(key: str, value: any, ttl: int = CACHE_TTL_MEDIUM):
    """Set value in Redis cache with TTL"""
    if not redis_client:
        return False
    try:
        redis_client.setex(key, ttl, json.dumps(value))
        return True
    except Exception as e:
        print(f"⚠️ Redis SET error: {e}")
        return False

def cache_delete(key: str):
    """Delete key from Redis cache"""
    if not redis_client:
        return False
    try:
        redis_client.delete(key)
        return True
    except Exception as e:
        print(f"⚠️ Redis DELETE error: {e}")
        return False

def cache_delete_pattern(pattern: str):
    """Delete all keys matching pattern"""
    if not redis_client:
        return False
    try:
        keys = redis_client.keys(pattern)
        if keys:
            redis_client.delete(*keys)
        return True
    except Exception as e:
        print(f"⚠️ Redis DELETE PATTERN error: {e}")
        return False

# ==========================================
# UPDATED BILLING ROUTE WITH REDIS & S3
# ==========================================
@app.route('/billing', methods=['GET', 'POST'])
def billing():
    """
    Flask proxy route for billing with Redis caching and S3 file upload
    """
    
    # GET REQUEST: Render the billing form
    if request.method == 'GET':
        try:
            # Check Redis cache for courses
            cache_key = "billing:courses"
            cached_courses = cache_get(cache_key)
            
            if cached_courses:
                print("✅ Using cached courses")
                courses = cached_courses
            else:
                # Fetch from FastAPI
                auth = get_auth()
                response = requests.get(
                    f"{FASTAPI_BASE_URL}/courses",
                    auth=auth,
                    timeout=10
                )
                
                if response.status_code == 200:
                    courses = response.json().get('courses', [])
                    # Cache the courses
                    cache_set(cache_key, courses, ttl=CACHE_TTL_LONG)
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
                
                # Invalidate cache for this user's billing history
                cache_delete_pattern(f"billing:history:{phone}*")
                
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
# BILLING HISTORY WITH REDIS CACHING
# ==========================================
@app.route('/billing-history')
def billing_history():
    """View billing history for logged-in user with Redis caching"""
    try:
        auth = get_auth()
        if not auth:
            flash("❌ Please login first", "danger")
            return redirect(url_for('login'))
        
        phone = session.get('phone')
        if not phone:
            flash("❌ Phone number not found in session", "danger")
            return redirect(url_for('billing'))
        
        # Check Redis cache
        cache_key = f"billing:history:{phone}"
        cached_history = cache_get(cache_key)
        
        if cached_history:
            print("✅ Using cached billing history")
            billing_entries = cached_history.get('entries', [])
        else:
            # Fetch from FastAPI
            response = requests.get(
                f"{FASTAPI_BASE_URL}/billing-history",
                params={"phone": phone},
                auth=auth,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                billing_entries = data.get('entries', [])
                # Cache the result
                cache_set(cache_key, {'entries': billing_entries}, ttl=CACHE_TTL_MEDIUM)
            else:
                flash("❌ Could not fetch billing history", "danger")
                return redirect(url_for('billing'))
        
        return render_template("billing_history.html", entries=billing_entries)
            
    except Exception as e:
        flash(f"❌ Error: {str(e)}", "danger")
        return redirect(url_for('billing'))

# ==========================================
# REDIS STATS ENDPOINT (ADMIN)
# ==========================================
@app.route('/admin/redis-stats')
def redis_stats():
    """View Redis cache statistics (admin only)"""
    if session.get('usertype') != 'admin':
        flash("❌ Admin access required", "danger")
        return redirect(url_for('home'))
    
    if not redis_client:
        return jsonify({
            "connected": False,
            "error": "Redis not configured"
        })
    
    try:
        info = redis_client.info()
        stats = {
            "connected": True,
            "used_memory": info.get('used_memory_human'),
            "total_keys": redis_client.dbsize(),
            "connected_clients": info.get('connected_clients'),
            "uptime_days": info.get('uptime_in_days'),
            "total_commands": info.get('total_commands_processed'),
            "hit_rate": round(
                info.get('keyspace_hits', 0) / 
                max(info.get('keyspace_hits', 0) + info.get('keyspace_misses', 1), 1) * 100, 
                2
            )
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
    """Clear Redis cache (admin only)"""
    if session.get('usertype') != 'admin':
        flash("❌ Admin access required", "danger")
        return redirect(url_for('home'))
    
    pattern = request.form.get('pattern')
    
    try:
        if pattern:
            cache_delete_pattern(pattern)
            flash(f"✅ Cleared cache matching: {pattern}", "success")
        else:
            if redis_client:
                redis_client.flushdb()
                flash("✅ All cache cleared", "success")
            else:
                flash("⚠️ Redis not available", "warning")
    except Exception as e:
        flash(f"❌ Error clearing cache: {str(e)}", "danger")
    
    return redirect(url_for('redis_stats'))
'''#------billing------
# Course fees mapping
COURSE_FEES = {
    "Informatica + IICS": 13000,
    "Informatica MDM": 18000,
    "IICS (ONLY)": 4000,
    "UNIX": 13000,
    "Oracle(SQL & PLSQL)": 13000,
    "ORACLE (SQL & PL/SQL)": 13000,
    "Advanced PL/SQL": 18000,
    "BDM": 18000,
    "Core Python + Web Development": 13000,
    "HTML": 13000
}

def get_fee_by_course(course_name):
    return COURSE_FEES.get(course_name.strip(), 0)


@app.route('/billing', methods=['GET', 'POST'])
def billing():
    """
    Flask proxy route for billing
    - GET: Render the billing form page
    - POST: Forward billing data to FastAPI backend
    """
    
    # ==========================================
    # GET REQUEST: Render the billing form
    # ==========================================
    if request.method == 'GET':
        try:
            # Optionally fetch courses from FastAPI for dropdown
            auth = get_auth()
            response = requests.get(
                f"{FASTAPI_BASE_URL}/courses",  # Assuming you have this endpoint
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
    
    # ==========================================
    # POST REQUEST: Forward to FastAPI
    # ==========================================
    if request.method == 'POST':
        try:
            # Extract form data
            billing_type = request.form.get('billing_type', '').strip()
            billing_category = request.form.get('billing_category', '').strip()
            payment_method = request.form.get('payment_mode', '').strip()
            phone = request.form.get('phone', '').strip()
            amount = request.form.get('amount', 0)
            upi_txn_id = request.form.get('upi_txn_id', '').strip() or None
            
            # Validate required fields
            if not all([billing_type, billing_category, payment_method, phone, amount]):
                flash("❌ All required fields must be filled", "danger")
                return redirect(url_for('billing'))
            
            # Convert amount to float
            try:
                amount = float(amount)
                if amount <= 0:
                    raise ValueError("Amount must be greater than 0")
            except (ValueError, TypeError) as e:
                flash(f"❌ Invalid amount: {str(e)}", "danger")
                return redirect(url_for('billing'))
            
            # Prepare payload for FastAPI
            payload = {
                "billing_type": billing_type,
                "billing_category": billing_category,
                "payment_method": payment_method,
                "amount": amount,
                "phone": phone,
                "currency": "INR",
                "upi_txn_id": upi_txn_id,
                "receipt_file_path": None,  # Can be enhanced for file upload
                "payload": {}
            }
            
            # Get authentication
            auth = get_auth()
            if not auth:
                flash("❌ Please login first", "danger")
                return redirect(url_for('login'))
            
            # Forward request to FastAPI
            response = requests.post(
                f"{FASTAPI_BASE_URL}/billing-create",
                json=payload,
                auth=auth,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            # Handle response
            if response.status_code == 200:
                data = response.json()
                transaction_uuid = data.get('transaction_uuid', 'N/A')
                amount_paid = data.get('amount', 0)
                
                flash(
                    f"✅ Payment of ₹{amount_paid} recorded successfully! "
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
# OPTIONAL: Billing history view
# ==========================================
@app.route('/billing-history')
def billing_history():
    """
    View billing history for logged-in user
    Proxies request to FastAPI
    """
    try:
        auth = get_auth()
        if not auth:
            flash("❌ Please login first", "danger")
            return redirect(url_for('login'))
        
        # Get user's phone from session (assuming it's stored during login)
        phone = session.get('phone')
        if not phone:
            flash("❌ Phone number not found in session", "danger")
            return redirect(url_for('billing'))
        
        # Fetch billing history from FastAPI
        response = requests.get(
            f"{FASTAPI_BASE_URL}/billing-history",
            params={"phone": phone},
            auth=auth,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            billing_entries = data.get('entries', [])
            return render_template("billing_history.html", entries=billing_entries)
        else:
            flash("❌ Could not fetch billing history", "danger")
            return redirect(url_for('billing'))
            
    except Exception as e:
        flash(f"❌ Error: {str(e)}", "danger")
        return redirect(url_for('billing'))


# ==========================================
# OPTIONAL: Health check
# ==========================================
@app.route('/billing/health')
def billing_health():
    """
    Health check endpoint to verify backend connectivity
    """
    try:
        response = requests.get(
            f"{FASTAPI_BASE_URL}/billing-test",
            timeout=5
        )
        
        if response.status_code == 200:
            return {
                "status": "healthy",
                "backend": "connected",
                "message": "Backend is reachable"
            }, 200
        else:
            return {
                "status": "unhealthy",
                "backend": "error",
                "message": f"Backend returned {response.status_code}"
            }, 503
            
    except Exception as e:
        return {
            "status": "unhealthy",
            "backend": "unreachable",
            "message": str(e)
        }, 503

# @app.route('/billing', methods=['GET', 'POST'])
# def billing():
#     try:
#         # Connect to Snowflake using RSA key authentication
#         conn = get_db_connection()
#         cursor = conn.cursor(snowflake.connector.DictCursor)
#     except Exception as e:
#         flash(f"❌ Database connection error: {e}", "danger")
#         return render_template("billing.html")

#     if request.method == 'POST':
#         try:
#             # Get form inputs
#             phone = str(request.form.get('phone', '')).strip()
#             course_name = str(request.form.get('course_name', '')).strip()
#             upi = str(request.form.get('upi', '')).strip()
#             payment_input = request.form.get('payment', 0)

#             try:
#                 current_payment = float(payment_input)
#             except (ValueError, TypeError):
#                 current_payment = 0

#             # --- Get user_id from phone ---
#             cursor.execute("SELECT id FROM nrm_users WHERE phone = %s", (phone,))
#             user_row = cursor.fetchone()
#             if not user_row:
#                 flash("❌ User with this phone number not found.", "danger")
#                 return redirect(url_for('billing'))

#             user_id = user_row['id']

#             # --- Get course_id from course_name ---
#             cursor.execute("SELECT id FROM nrm_courses WHERE course_name = %s", (course_name,))
#             course_row = cursor.fetchone()
#             if not course_row:
#                 flash("❌ Course not found.", "danger")
#                 return redirect(url_for('billing'))

#             course_id = course_row['id']

#             # --- Calculate total paid so far ---
#             fee = get_fee_by_course(course_name)
#             cursor.execute("""
#                 SELECT SUM(amount) AS total_paid
#                 FROM nrm_billing_entries
#                 WHERE user_id = %s AND course_id = %s
#             """, (user_id, course_id))
#             result = cursor.fetchone()
#             total_paid_so_far = float(result['total_paid']) if result['total_paid'] else 0
#             new_total_paid = total_paid_so_far + current_payment
#             balance = max(fee - new_total_paid, 0)

#             # --- Correct status logic: Completed only if total_paid == fee ---
#             if new_total_paid == fee:
#                 status_id = 1  # Completed
#             else:
#                 status_id = 2  # Active / Partial

#             # --- Insert billing entry ---
#             cursor.execute("""
#                 INSERT INTO nrm_billing_entries
#                 (user_id, course_id, upi_id, amount, discount, status_id, billing_timestamp)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s)
#             """, (user_id, course_id, upi, current_payment, 0.00, status_id, datetime.now()))

#             # --- Update registration status ---
#             cursor.execute("""
#                 UPDATE nrm_registrations
#                 SET status_id = %s
#                 WHERE student_id = %s AND course_id = %s
#             """, (status_id, user_id, course_id))

#             conn.commit()
#             flash(f"✅ Payment of {current_payment} recorded. Total Paid: {new_total_paid}/{fee}, Balance: {balance}", "success")

#         except Exception as e:
#             flash(f"❌ Error processing billing: {e}", "danger")

#         return redirect(url_for('billing'))

#     # Fetch courses for dropdown (optional)
#     cursor.execute("SELECT course_name FROM nrm_courses ORDER BY course_name")
#     courses = cursor.fetchall()
#     return render_template("billing.html", courses=courses)'''

# Employee_Report
@app.route('/employee-report')
def employee_report():
    conn = get_db_connection()
    if conn is None:
        return "❌ Failed to connect to database", 500

    cursor = conn.cursor(snowflake.connector.DictCursor)

    try:
        cursor.execute("""
            SELECT
              s.first_name AS `First Name`,
              s.last_name AS `Last Name`,
              s.email AS `Email`,
              s.phone AS `Phone Number`,
              r.start_date AS `Hire Date`,
              r.registration_id AS `Reg ID`,
              st.status AS `Status`,
              'Not Available' AS `Msinfo32`,
              'Admin' AS `Job Title`
            FROM
              nrm_registrations r
            JOIN
              nrm_students s ON r.student_id = s.id
            JOIN
              nrm_courses c ON r.course_id = c.id
            LEFT JOIN
              nrm_statuses st ON r.status_id = st.id
            WHERE
              c.course_name = 'Chakora Hub Admin'
        """)
        employee_data = cursor.fetchall()
        print("✅ Retrieved rows:", len(employee_data))
    except Exception as e:
        print("❌ Query Error:", e)
        employee_data = []
    finally:
        cursor.close()
        conn.close()

    return render_template('employee-report.html', employee_data=employee_data)




# -------------------------
# Routes
# -------------------------



   
           

# ==========================================================
# 360 DASHBOARD ROUTES
# ==========================================================

@app.route('/dashboard')
def dashboard():
    """
    Render the main 360 Dashboard Portal page.
    """
    if not session.get('logged_in'):
        return redirect(url_for('employee_home'))
    
    # Get current module from query parameters
    current_module = request.args.get('module', 'welcome')
    
    # For Infra360, fetch data if needed
    infra_data = None
    if current_module == 'infra360':
        infra_data = get_infra360_data()
    
    return render_template('dashboard.html', 
                         current_module=current_module,
                         infra_data=infra_data)

def get_infra360_data():
    """
    Fetch data for Infra360 module display.
    """
    if not session.get('logged_in'):
        return None
    
    employee_id = session.get('employee_id')
    conn = get_db_connection()
    
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
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
        
        cursor.close()
        conn.close()
        
        # Format the data
        infra_data = {
            'emp_name': f"{personal_data[0]} {personal_data[1]}" if personal_data else employee_data[1] if employee_data else 'Employee',
            'emp_id': employee_data[0] if employee_data else employee_id,
            'department': employee_data[3] if employee_data and employee_data[3] else employee_data[2] if employee_data else 'N/A',
            'designation': employee_data[4] if employee_data and employee_data[4] else 'N/A',
            'profile_pic': personal_data[2] if personal_data and personal_data[2] else None,
            'asset_name': asset_data[0] if asset_data else 'No Asset Assigned',
            'asset_type': asset_data[0] if asset_data else 'N/A',
            'serial_number': asset_data[1] if asset_data else 'N/A',
            'allocation_date': asset_data[2].strftime('%Y-%m-%d') if asset_data and asset_data[2] else 'N/A',
            'status': asset_data[3] if asset_data else 'N/A',
            'enquiries': [{'ENQUIRY': row[0], 'STATUS': row[1]} for row in enquiries_data] if enquiries_data else []
        }
        
        return infra_data
        
    except Exception as e:
        print(f"Error fetching Infra360 data: {e}")
        if conn:
            conn.close()
        return None

# ==========================================================
# ELEARN360 PAGE
# ==========================================================

@app.route('/elearn360')
def elearn360_home():
    """
    Serves the Elearn 360 static dashboard page.
    Modules inside (Skill Set, Certification, WILP, R&D, Workshop)
    are handled by client-side JS in Elearn360.html.
    """
    if not session.get('logged_in'):
        return redirect(url_for('employee_home'))
    return render_template('elearn360.html')

# ==========================================================
# INFRA 360 EMPLOYEE PORTAL PAGE
# ==========================================================
@app.route('/infra360')
def infra_360():
    """
    Render the infra_360 Employee Portal page.
    """
    if not session.get('logged_in'):
        return redirect(url_for('employee_home'))
    return render_template('infra-360.html')

# ==========================================================
# personal_360 EMPLOYEE PORTAL PAGE
# ==========================================================

@app.route('/personal360')
def personal_360():
    """
    Render the personal_360 Employee Portal page.
    """
    if not session.get('logged_in'):
        return redirect(url_for('employee_home'))
    return render_template('personal360.html')

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
        cursor = conn.cursor(snowflake.connector.DictCursor)
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
        cursor = conn.cursor(snowflake.connector.DictCursor)
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
        cursor = conn.cursor(snowflake.connector.DictCursor)
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
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        email = request.form.get('email', '').strip()
        phone = request.form.get('phone', '').strip()
        feedback_text = request.form.get('feedback', '').strip()

        if not all([name, email, phone, feedback_text]):
            session['feedback_error'] = "Please fill in all fields."
            return redirect(url_for('feedback_form'))

        conn = None
        cursor = None
        try:
            student_id = session.get('user_id')

            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO "VSRSUBHASH$CHAKORA_DB"."CHAKORA"."NRM_FEEDBACK"
                (STUDENT_ID, NAME, EMAIL, PHONE, FEEDBACK_MESSAGE, SUBMITTED_AT)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
            """, (student_id, name, email, phone, feedback_text))

            conn.commit()
            session['feedback_submitted'] = True
            session['feedback_name'] = name

        except Exception as e:
            print("Feedback error:", e)
            session['feedback_error'] = "Error submitting feedback."

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        return redirect(url_for('feedback_form'))

    # ---------------- GET ----------------
    user_data = None
    logged_in = False

    feedback_submitted = session.pop('feedback_submitted', False)
    feedback_name = session.pop('feedback_name', None)
    feedback_error = session.pop('feedback_error', None)

    if session.get('user_id'):
        conn = get_db_connection()
        cursor = conn.cursor(snowflake.connector.DictCursor)
        cursor.execute("""
            SELECT USERNAME, EMAIL, PHONE
            FROM "VSRSUBHASH$CHAKORA_DB"."CHAKORA"."NRM_USERS"
            WHERE ID = %s
        """, (session['user_id'],))

        row = cursor.fetchone()
        if row:
            user_data = {
                'username': row['USERNAME'],
                'email': row['EMAIL'],
                'phone': row['PHONE']
            }
            logged_in = True

        cursor.close()
        conn.close()

    return render_template(
        'student-feedback.html',
        user=user_data,
        logged_in=logged_in,
        feedback_submitted=feedback_submitted,
        feedback_name=feedback_name,
        feedback_error=feedback_error
    )

@app.route('/api/feedbacks')
def api_feedbacks():
    """Get all feedbacks"""
    try:
        response = requests.get(
            f"{STUDENT_SERVICE_URL}/api/student/feedbacks",
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
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
        cur = conn.cursor(snowflake.connector.DictCursor)

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
    cur = conn.cursor(snowflake.connector.DictCursor)
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
    cur = conn.cursor(snowflake.connector.DictCursor)
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
    cur = conn.cursor(snowflake.connector.DictCursor)
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
    cur = conn.cursor(snowflake.connector.DictCursor)
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
    cur = conn.cursor(snowflake.connector.DictCursor)

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
from flask import render_template, request, current_app
import base64  
# =========================
# LAMBDA URL
# =========================
LAMBDA_URL = "https://m8e0rv8b4i.execute-api.eu-north-1.amazonaws.com/default/brs-upload"


# ==================================================
# STEP 1: BRS SUBMIT (NEXT)
# ==================================================
@app.route("/industry/brs", methods=["GET", "POST"])
def industry_brs():

    if request.method == "GET":
        return render_template("submit_brs.html")

    try:
        form = request.form
        file = request.files.get("file")

        if not file:
            flash("❌ Please upload a BRS document.", "error")
            return redirect(url_for("industry_brs"))

        file_bytes = file.read()
        encoded_file = base64.b64encode(file_bytes).decode("utf-8")

        payload = {
            "project_id": form.get("project_id"),
            "project_name": form.get("project_name"),
            "project_description": form.get("project_description"),
            "client_name": form.get("client_name"),
            "department": form.get("department"),
            "requirement_type": form.get("requirement_type"),
            "priority": form.get("priority"),
            "business_objective": form.get("business_objective"),
            "scope": form.get("scope"),
            "start_date": form.get("start_date"),
            "end_date": form.get("end_date"),
            "contact_email": form.get("contact_email"),
            "contact_phone": form.get("contact_phone"),
            "filename": file.filename,
            "filedata": encoded_file
        }

        resp = requests.post(LAMBDA_URL, json=payload, timeout=30)
        result = resp.json()

        if resp.status_code == 200:
            # 🔐 Store message only (NO flash here)
            session["brs_flash_msg"] = (
                f"BRS submitted successfully! "
                f"Your BRS ID is {result.get('brs_id')}"
            )
            return redirect(url_for("alignment_charter"))

        else:
            flash(f"❌ {result.get('error')}", "error")
            return redirect(url_for("industry_brs"))

    except Exception:
        current_app.logger.exception("BRS submit error")
        flash("❌ Internal server error. Please try again.", "error")
        return redirect(url_for("industry_brs"))


# ==================================================
# STEP 2: ALIGNMENT PAGE (DISPLAY ONLY)
# ==================================================
@app.route("/alignment-charter")
def alignment_charter():
    return render_template("alignment_charter.html")


# ==================================================
# STEP 3: ALIGNMENT SUBMIT (ONLY ONE FLASH HERE ✅)
# ==================================================
@app.route("/alignment-submit", methods=["POST"])
def alignment_submit():

    # Take stored message
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
@app.route("/applicationform")
def applicationform():
    return render_template("applicationform.html")

def send_application_email(user_email, name, application_id):
    try:
        ses.send_email(
            Source=ADMIN_EMAIL,
            Destination={
                "ToAddresses": [user_email],
                "CcAddresses": [ADMIN_EMAIL]
            },
            Message={
                "Subject": {
                    "Data": f"Industry Application Submitted - {application_id}"
                },
                "Body": {
                    "Html": {
                        "Data": f"""
                        <h3>Dear {name},</h3>

                        <p>Your industry collaboration application has been submitted successfully.</p>

                        <p><b>Application ID:</b> {application_id}</p>

                        <p>Please keep this Application ID for future reference.</p>

                        <br>
                        <p>Regards,<br>
                        ChakoraHub Team</p>
                        """
                    }
                }
            }
        )
        print("✅ Email sent to user and admin")

    except Exception as e:
        print("❌ Email error:", e)

@app.route("/submit-application", methods=["POST"])
def submit_application():
    conn = cursor = None
    try:
        full_name = request.form.get("full_name")
        email = request.form.get("email")
        phone = request.form.get("phone")
        organisation = request.form.get("organisation")
        collaboration_type = request.form.get("collaboration_type")
        project_title = request.form.get("project_title")
        description = request.form.get("description")
        start_date = request.form.get("start_date")

        if not all([full_name, email, phone]):
            flash("Please fill all required fields", "error")
            return redirect(url_for("applicationform"))  # ✅ CORRECT

        application_id = f"APP-{uuid.uuid4().hex[:8].upper()}"

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO INDUSTRY_APPLICATIONS
            (APPLICATION_ID, FULL_NAME, EMAIL, PHONE,
             ORGANISATION, COLLABORATION_TYPE,
             PROJECT_TITLE, DESCRIPTION, START_DATE)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            application_id, full_name, email, phone,
            organisation, collaboration_type,
            project_title, description, start_date
        ))

        conn.commit()

        send_application_email(email, full_name, application_id)

        flash(
            f"Application submitted successfully! "
            f"Your Application ID is {application_id}",
            "success"
        )

        return redirect(url_for("applicationform"))  # ✅ CORRECT

    except Exception as e:
        if conn:
            conn.rollback()
        print("❌ Application Error:", e)
        flash("Error submitting application", "error")
        return redirect(url_for("applicationform"))  # ✅ CORRECT

    finally:
        if cursor: cursor.close()
        if conn: conn.close()



#-------------singoff---------------#
@app.route("/signoff-form")
def signoff_form():
    return render_template("organizationsingoff.html")
# ======================================
# ORGANIZATION SIGN-OFF (SNOWFLAKE INSERT)
# ======================================
@app.route("/org-signoff", methods=["GET","POST"])
def org_signoff():

    if not session.get("org_logged_in"):
        return redirect(url_for("org_track_login"))

    if request.method == "POST":
        org_name = request.form.get("org_name")
        authorized_person = request.form.get("authorized_person")
        
        brs_id = request.form.get("brs_id")
        approval_notes = request.form.get("approval_notes")
        approval_status = request.form.get("approval_status")

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO INDUSTRY_ORG_SIGNOFF
            (ORG_NAME, AUTHORIZED_PERSON,  BRS_ID, APPROVAL_NOTES, APPROVAL_STATUS)
            VALUES (%s,%s,%s,%s,%s)
        """, (
            org_name, authorized_person, brs_id, approval_notes, approval_status
        ))

        conn.commit()
        cursor.close()
        conn.close()

        return render_template(
            "organizationsingoff.html",
            message="✔ Organization Sign-Off submitted successfully!",
            message_type="success"
        )

    return render_template("organizationsingoff.html")

@app.route('/api/project-status/<project_id>', methods=['GET'])
def track_project_status(project_id):
    conn = cursor = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(snowflake.connector.DictCursor)

        # 1️⃣ Project basic details
        cursor.execute("""
            SELECT 
                PROJECT_ID,
                PROJECT_NAME,
                OWNER,
                START_DATE,
                CURRENT_PHASE_ORDER,
                CURRENT_PHASE_NAME
            FROM PROJECTS
            WHERE PROJECT_ID = %s
            LIMIT 1
        """, (project_id,))
        project = cursor.fetchone()

        if not project:
            return jsonify({'error': 'Project not found'}), 404

        # 2️⃣ All phases (master)
        cursor.execute("""
            SELECT PHASE_ORDER AS "order", PHASE_NAME AS "name"
            FROM PROJECT_PHASES
            ORDER BY PHASE_ORDER
        """)
        all_phases = cursor.fetchall()

        # 3️⃣ Phase history
        cursor.execute("""
            SELECT 
                UPDATED_AT AS "date",
                PHASE_NAME AS "new_phase",
                NOTES
            FROM PROJECT_PHASE_HISTORY
            WHERE PROJECT_ID = %s
            ORDER BY UPDATED_AT DESC
        """, (project_id,))
        history = cursor.fetchall()

        response = {
            "project_id": project["PROJECT_ID"],
            "project_name": project["PROJECT_NAME"],
            "owner": project["OWNER"],
            "start_date": project["START_DATE"].isoformat() if project["START_DATE"] else None,
            "current_phase": {
                "order": project["CURRENT_PHASE_ORDER"],
                "name": project["CURRENT_PHASE_NAME"]
            },
            "all_phases": all_phases,
            "history": history
        }

        return jsonify(response), 200

    except Exception as e:
        print("❌ Project status error:", e)
        traceback.print_exc()
        return jsonify({'error': 'Internal server error'}), 500

    finally:
        if cursor: cursor.close()
        if conn: conn.close()




@app.route("/project-dashboard")
def project_dashboard():
    return render_template("project-dashboard.html")

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
    return jsonify({"logged_in": bool(session.get("org_logged_in"))})

# =====================================================
# COMMON LOGIN (DB BASED – USED BY BOTH PAGES)
# =====================================================
@app.route("/track-login", methods=["POST"])
def track_login():
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()

    if not username or not password:
        return jsonify({"success": False, "error": "Username & password required"})

    conn = cursor = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"success": False, "error": "DB connection failed"})

        cursor = conn.cursor(snowflake.connector.DictCursor)

        cursor.execute("""
            SELECT u.ID AS USER_ID, l.PASSWORD
            FROM NRM_USERS u
            JOIN NRM_LOGINS l ON u.ID = l.USER_ID
            WHERE LOWER(TRIM(u.EMAIL)) = LOWER(TRIM(%s))
               OR TRIM(u.PHONE) = TRIM(%s)
            ORDER BY l.CREATED_AT DESC
            LIMIT 1
        """, (username, username))

        user = cursor.fetchone()
        if not user:
            return jsonify({"success": False, "error": "Invalid credentials"})

        db_pwd = (user["PASSWORD"] or "").strip()

        if db_pwd.startswith(("scrypt:", "$2a$", "$2b$", "pbkdf2:")):
            valid = check_password_hash(db_pwd, password)
        else:
            valid = (db_pwd == password)

        if not valid:
            return jsonify({"success": False, "error": "Invalid credentials"})

        # ✅ Sessions for BOTH pages
        session["track_logged_in"] = True
        session["org_logged_in"] = True
        session["track_user_id"] = user["USER_ID"]
        session.permanent = True

        return jsonify({"success": True})

    except Exception as e:
        print("❌ Login error:", e)
        return jsonify({"success": False, "error": "Login failed"})

    finally:
        if cursor: cursor.close()
        if conn: conn.close()



@app.route('/add_course', methods=['GET'])
def add_course():
    if 'user' not in session:
        flash("Please login first.", "error")
        return redirect(url_for('home'))

    return render_template("add-course.html")


@app.route('/save_course', methods=['POST'])
def save_course():
    if 'user' not in session:
        flash("Please login first.", "error")
        return redirect(url_for('home'))

    course_name = request.form.get('course_name', '').strip()
    course_code = request.form.get('course_code', '').strip()

    if not course_name or not course_code:
        flash("Please fill all fields.", "error")
        return redirect(url_for('add_course'))

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            "SELECT 1 FROM nrm_courses WHERE course_code = %s",
            (course_code,)
        )
        if cur.fetchone():
            flash("Course code already exists!", "error")
            return redirect(url_for('add_course'))

        cur.execute("""
            INSERT INTO nrm_courses (course_name, course_code, course_fee)
            VALUES (%s, %s, 0)
        """, (course_name, course_code))

        conn.commit()
        flash("Course added successfully!", "success")

    except Exception as e:
        print("❌ Error adding course:", e)
        flash("Error saving course. Check logs.", "error")

    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

    return redirect(url_for('add_course'))



if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080, debug=True)
