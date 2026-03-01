import sys
import io
import logging
import traceback
import boto3
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
from datetime import datetime, timedelta
from threading import Lock
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import urllib.parse
import snowflake.connector
from snowflake.connector import errors
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
# In your Flask app (app.py or main.py)
import requests


# Helper to always return a fresh DB connection with RSA key authentication
def get_db_connection():
    try:
        # Load RSA private key
        with open('rsa_key.p8', 'rb') as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )
        
        # Convert private key to bytes
        pkb = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        # Connect to Snowflake using RSA key
        conn = snowflake.connector.connect(
            user='ChakoraHub',
            account='gpguymt-ta88699',
            private_key=pkb,
            warehouse='COMPUTE_WH',
            database='"VSRSUBHASH$CHAKORA_DB"',
            schema="CHAKORA"
        )
        print("✅ Connected to Snowflake using RSA key")
        return conn
        
    except Exception as e:
        print("❌ DB Connection Error:", e)
        return None

def load_nrm_festivals_from_db():
    conn = get_db_connection()
    cursor = conn.cursor(snowflake.connector.DictCursor)
    cursor.execute("SELECT festival_name, festival_date FROM nrm_festivals")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {row['FESTIVAL_DATE'].strftime('%Y-%m-%d'): row['FESTIVAL_NAME'] for row in rows}

# Course mapping for display names
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


app = Flask(__name__)
app.secret_key = 'temporary123'

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

# Utility
def allowed_file(filename, category='images'):
    ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''
    return ext in app.config['ALLOWED_EXTENSIONS'].get(category, set())

# Set session lifetime globally
app.permanent_session_lifetime = timedelta(days=7)

# ✅✅✅ FIXED LOGIN ROUTE ✅✅✅
@app.route('/nrm_logins', methods=['POST'])
def user_nrm_logins():
    print("🔐 Combined Login route called")

    login_type = request.form.get("login_type", "user").strip().lower()
    password = request.form.get("password", "").strip()

    if not password:
        flash("Please enter password.", "error")
        return redirect(url_for("home"))

    conn = get_db_connection()
    if conn is None:
        flash("Database connection failed.", "error")
        return redirect(url_for("home"))

    cursor = conn.cursor(snowflake.connector.DictCursor)

    try:
        # -------------------------------------------------------
        # 🔥 EMPLOYEE LOGIN
        # -------------------------------------------------------
        if login_type == "employee":
            employee_id = request.form.get("employee_id", "").strip()

            if not employee_id:
                flash("Please enter Employee ID.", "error")
                return redirect(url_for("home"))

            print("Employee Login →", employee_id)

            cursor.execute("""
                SELECT EMPLOYEE_ID, PASSWORD 
                FROM EMP_NRM_LOGINS
                WHERE EMPLOYEE_ID = %s
                LIMIT 1
            """, (employee_id,))
            emp = cursor.fetchone()

            print("Employee Query Result:", emp)

            if not emp:
                flash("Employee ID not found.", "error")
                return redirect(url_for("home"))

            db_password = emp.get("PASSWORD") or ""

            # Check password
            if db_password.startswith("scrypt:"):
                valid = check_password_hash(db_password, password)
            else:
                valid = (db_password == password)

            if not valid:
                flash("Incorrect Employee Password.", "error")
                return redirect(url_for("home"))

            # Fetch employee name
            cursor.execute("""
                SELECT EMPLOYEE_NAME
                FROM EMP_NRM_EMPLOYEES
                WHERE EMP_ID = %s
            """, (employee_id,))
            emp_info = cursor.fetchone()

            session["employee_id"] = employee_id
            session["employee_name"] = emp_info["EMPLOYEE_NAME"] if emp_info else "Employee"

            flash("Employee Login Successful!", "success")
            return redirect(url_for("employee_resources"))

        # -------------------------------------------------------
        # 🔥 USER LOGIN
        # -------------------------------------------------------
        username = request.form.get("username", "").strip()

        if not username:
            flash("Please enter Email or Phone.", "error")
            return redirect(url_for("home"))

        print("User Login →", username)

        cursor.execute("""
            SELECT 
                u.ID AS user_id,
                u.USERNAME,
                u.EMAIL,
                u.PHONE,
                u.USERTYPE,
                u.PROFILE_PIC,
                l.ID AS login_id,
                l.PASSWORD
            FROM nrm_users u
            JOIN nrm_logins l ON u.ID = l.USER_ID
            WHERE u.EMAIL = %s OR u.PHONE = %s
            ORDER BY l.CREATED_AT DESC
            LIMIT 1
        """, (username, username))

        user = cursor.fetchone()

        print("User Query Result:", user)

        if not user:
            flash("User not found.", "error")
            return redirect(url_for("home"))

        db_password = user.get("PASSWORD") or ""

        if db_password.startswith("scrypt:"):
            valid = check_password_hash(db_password, password)
        else:
            valid = (db_password == password)

        if not valid:
            flash("Incorrect User Password.", "error")
            return redirect(url_for("home"))

        # Store session
        session["user"] = user["EMAIL"] or user["PHONE"]
        session["user_id"] = user["USER_ID"]
        session["login_id"] = user["LOGIN_ID"]
        session["usertype"] = (user["USERTYPE"] or "student").lower()
        session["profile_pic"] = user["PROFILE_PIC"] or "profile_photo/defaultpicture.jpg"

        flash("User Login Successful!", "success")
        return redirect(url_for("resources"))

    except Exception as e:
        print("❌ LOGIN ERROR:", e)
        traceback.print_exc()
        flash("Unexpected error occurred.", "error")
        return redirect(url_for("home"))

    finally:
        cursor.close()
        conn.close()

    


# ✅✅✅ COMPLETELY FIXED FEEDBACK ROUTE ✅✅✅
@app.route('/feedback', methods=['GET', 'POST'])
def feedback_form():
    if request.method == 'POST':
        # Handle POST request
        name = request.form.get('name', '').strip()
        email = request.form.get('email', '').strip()
        phone = request.form.get('phone', '').strip()
        feedback_text = request.form.get('feedback', '').strip()

        if not all([name, email, phone, feedback_text]):
            flash("Please fill in all fields.", "error")
            return redirect(url_for('feedback_form'))

        conn = None
        cursor = None
        
        try:
            conn = get_db_connection()
            if not conn:
                flash("Database connection failed. Please try again later.", "error")
                return redirect(url_for('feedback_form'))

            cursor = conn.cursor()

            # 🔹 FIX: Find or create student in nrm_students table
            cursor.execute("""
                SELECT id 
                FROM chakora.nrm_students 
                WHERE email = %s OR phone = %s
                LIMIT 1
            """, (email, phone))
            
            student_row = cursor.fetchone()
            
            if student_row:
                # Student exists - use their ID as student_id
                student_id = student_row[0]
                print(f"✅ Found existing student: {student_id}")
                
                # Update student info if needed
                cursor.execute("""
                    UPDATE chakora.nrm_students 
                    SET first_name = %s, last_name = %s, phone = %s
                    WHERE id = %s
                """, (name.split()[0] if name else '', 
                      ' '.join(name.split()[1:]) if name and len(name.split()) > 1 else '', 
                      phone, student_id))
                
            else:
                # Create new student
                first_name = name.split()[0] if name else ''
                last_name = ' '.join(name.split()[1:]) if name and len(name.split()) > 1 else ''
                
                cursor.execute("""
                    INSERT INTO chakora.nrm_students 
                    (first_name, last_name, email, phone, registration_source)
                    VALUES (%s, %s, %s, %s, 'website_feedback')
                """, (first_name, last_name, email, phone))
                
                # Get the new student ID
                cursor.execute("""
                    SELECT id FROM chakora.nrm_students 
                    WHERE email = %s 
                    ORDER BY id DESC LIMIT 1
                """, (email,))
                new_student = cursor.fetchone()
                student_id = new_student[0] if new_student else None
                print(f"✅ Created new student: {student_id}")

            # Insert feedback with student_id
            if student_id:
                cursor.execute("""
                    INSERT INTO chakora.nrm_feedback 
                    (student_id, feedback_message, submitted_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP())
                """, (student_id, feedback_text))
                
                conn.commit()
                print(f"✅ Feedback saved for student_id: {student_id}")
                flash("Feedback submitted successfully!", "success")
            else:
                flash("Error creating student profile.", "error")

        except Exception as e:
            print(f"❌ Database error: {e}")
            import traceback
            traceback.print_exc()
            if conn:
                conn.rollback()
            flash("Error submitting feedback. Please try again.", "error")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        session['feedback_submitted'] = True
        session['feedback_name'] = name
        return redirect(url_for('feedback_form'))
    
    else:
        # GET request handling
        useremail = session.get('user')
        user_data = {}
        logged_in = False
        feedback_submitted = session.pop('feedback_submitted', False)
        feedback_name = session.pop('feedback_name', None)

        if useremail:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                try:
                    # Get student data
                    cursor.execute("""
                        SELECT first_name, last_name, email, phone 
                        FROM chakora.nrm_students 
                        WHERE email = %s OR phone = %s
                        LIMIT 1
                    """, (useremail, useremail))
                    user_data = cursor.fetchone()
                    
                    if user_data:
                        logged_in = True
                        # Format name for display
                        user_data = {
                            'username': f"{user_data[0]} {user_data[1]}".strip(),
                            'email': user_data[2],
                            'phone': user_data[3]
                        }
                        
                except Exception as e:
                    print(f"Error fetching student data: {e}")
                finally:
                    cursor.close()
                    conn.close()

        return render_template(
            'student-feedback.html',
            user=user_data,
            logged_in=logged_in,
            feedback_submitted=feedback_submitted,
            feedback_name=feedback_name
        )



@app.route('/feedback-scroll')
def feedback_scroll():
    feedbacks = []
    conn = get_db_connection()
    
    if conn:
        print("✅ DB connection established")
        cursor = conn.cursor()
        try:
            # 🔹 FIX: Get feedback with student names from nrm_students
            cursor.execute("""
                SELECT 
                    f.feedback_message,
                    COALESCE(s.first_name || ' ' || COALESCE(s.last_name, ''), 'Anonymous') as student_name,
                    f.submitted_at
                FROM chakora.nrm_feedback f
                LEFT JOIN chakora.nrm_students s ON f.student_id = s.id
                WHERE f.feedback_message IS NOT NULL 
                  AND TRIM(f.feedback_message) != ''
                ORDER BY f.submitted_at DESC
            """)
            rows = cursor.fetchall()
            
            # Format the data properly
            for row in rows:
                feedbacks.append({
                    'feedback_message': row[0],
                    'name': row[1].strip() if row[1] else 'Anonymous'
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

# ✅ FIXED API ENDPOINT for nrm_students
@app.route('/api/feedbacks')
def api_feedbacks():
    feedbacks = []
    conn = get_db_connection()
    
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT 
                    f.feedback_message,
                    COALESCE(s.first_name || ' ' || COALESCE(s.last_name, ''), 'Anonymous') as student_name,
                    f.submitted_at
                FROM chakora.nrm_feedback f
                LEFT JOIN chakora.nrm_students s ON f.student_id = s.id
                WHERE f.feedback_message IS NOT NULL 
                  AND TRIM(f.feedback_message) != ''
                ORDER BY f.submitted_at DESC
            """)
            rows = cursor.fetchall()
            
            for row in rows:
                feedbacks.append({
                    'feedback_message': row[0],
                    'name': row[1].strip() if row[1] else 'Anonymous'
                })
                
            print(f"📦 API Feedbacks: {len(feedbacks)}")
        except Exception as e:
            print("❌ SQL Error in API:", e)
            import traceback
            traceback.print_exc()
        finally:
            cursor.close()
            conn.close()
    else:
        print("❌ No DB connection for API")
    
    return jsonify(feedbacks)                                                            



@app.route('/', methods=['GET', 'POST'])
def home():
    try:
        # If logged in, render home.html
        if 'user' in session:
            return render_template("home.html")

        search_query = request.args.get('query', '').strip()

        conn = get_db_connection()
        cursor = conn.cursor()

        # Get active courses
        cursor.execute("""
            SELECT DISTINCT r.course_id AS course_id, r.language_id AS language_id
            FROM chakora.nrm_registrations r
            JOIN chakora.nrm_statuses s ON r.status_id = s.id
            WHERE s.status = 'Active'
        """)
        active_courses = cursor.fetchall()

        current_batches = []
        upcoming_batches = []
        
        # 🔹 FIXED: Fetch feedbacks with student names from nrm_students
        feedbacks = []
        try:
            cursor.execute("""
                SELECT 
                    COALESCE(s.first_name || ' ' || COALESCE(s.last_name, ''), 'Anonymous') as student_name,
                    f.feedback_message,
                    f.submitted_at
                FROM chakora.nrm_feedback f
                LEFT JOIN chakora.nrm_students s ON f.student_id = s.id
                WHERE f.feedback_message IS NOT NULL 
                  AND TRIM(f.feedback_message) != ''
                ORDER BY f.submitted_at DESC
                LIMIT 20
            """)
            feedback_rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            for row in feedback_rows:
                feedbacks.append({
                    'username': row[0].strip() if row[0] else 'Anonymous',
                    'feedback_message': row[1],
                    'submitted_at': row[2]
                })
            
            print(f"✅ Fetched {len(feedbacks)} feedbacks from nrm_students")
            
        except Exception as fb_error:
            print(f"❌ Feedback fetch error: {fb_error}")
            import traceback
            traceback.print_exc()
            feedbacks = []

        # Rest of your existing batch processing code...
        # [Keep your existing batch processing logic here]

        cursor.close()
        conn.close()

        return render_template(
            "home.html",
            current_batches=current_batches,
            upcoming_batches=upcoming_batches,
            query=search_query,
            feedbacks=feedbacks,
            current_year=datetime.now().year
        )

    except Exception as e:
        print("❌ Home Error:", e)
        import traceback
        traceback.print_exc()
        return f"Error: {str(e)}"


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
        traceback.print_exc()
        flash("Error loading solutions page. Please try again.")
        return redirect(url_for('home'))



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

# ✅ LOGOUT
@app.route('/logout')
def logout():
    print("DEBUG >> Session contents at logout:", dict(session))

    login_id = session.get('login_id')

    if login_id:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    UPDATE nrm_logins
                    SET is_active = 'N'
                    WHERE id = %s
                """, (login_id,))
                conn.commit()
                print("✅ is_active set to N for login_id:", login_id)
            except Exception as e:
                print("❌ Logout error:", e)
            finally:
                cursor.close()
                conn.close()

    session.clear()
    flash("You have been logged out.")
    return redirect(url_for('home'))

# ✅ RESOURCES ROUTE WITH DETAILED ERROR HANDLING + CACHING ENABLED
@app.route('/resources', methods=['GET', 'POST'])
def resources():
    useremail = session.get('user')
    print(f"🔍 Resources: User from session = {useremail}")
    
    if not useremail:
        flash("Please login first.")
        return redirect(url_for('home'))

    conn = None
    cursor = None
    
    try:
        print("📡 Step 1: Connecting to database...")
        conn = get_db_connection()
        if not conn:
            print("❌ Step 1 FAILED: Database connection failed")
            flash("Database connection failed.")
            return redirect(url_for('home'))
        print("✅ Step 1: Database connected")
            
        cursor = conn.cursor(snowflake.connector.DictCursor)

        # ✅ Get user info
        print(f"📡 Step 2: Fetching user info for {useremail}...")
        cursor.execute("""
            SELECT
                u.ID AS user_id, 
                u.USERTYPE, 
                u.USERNAME, 
                u.PROFILE_PIC,
                u.EMAIL,
                u.PHONE
            FROM nrm_users u
            WHERE u.EMAIL = %s OR u.PHONE = %s
            LIMIT 1
        """, (useremail, useremail))
        
        user_row = cursor.fetchone()
        print(f"✅ Step 2: User row fetched = {user_row}")

        if not user_row:
            print("❌ Step 2 FAILED: User not found")
            session.clear()
            flash("User not found. Please login again.")
            cursor.close()
            conn.close()
            return redirect(url_for('home'))

        # Extract user data
        print("📡 Step 3: Extracting user data...")
        user_id = user_row.get('USER_ID') or user_row.get('ID')
        db_username = user_row.get('USERNAME')
        profile_pic = user_row.get('PROFILE_PIC') or 'profile_photo/defaultpicture.jpg'
        usertype_raw = user_row.get('USERTYPE') or 'student'
        usertype = usertype_raw.lower()
        email = user_row.get('EMAIL')
        phone = user_row.get('PHONE')

        username = db_username or useremail.split('@')[0]
        location_name = ''
        reg_id = ''
        print(f"✅ Step 3: Username={username}, Usertype={usertype}, ProfilePic={profile_pic}")

        # Build resources from filesystem
        print("📡 Step 4: Building resources from filesystem...")
        base_path = app.config.get('UPLOAD_FOLDER', '/home/vsrsubhash/uploads')
        print(f"   Base path: {base_path}")
        
        subject_list = [
            'Informatica', 'Unix', 'Oracle', 'IICS',
            'Python for web development', 'Informatica MDM',
            'Informatica BDM', 'Python for automation'
        ]
        
        resources_dict = {}
        for subject in subject_list:
            folder_name = subject.replace(' ', '_')
            
            ppt_dir = os.path.join(base_path, 'ppts', folder_name)
            code_dir = os.path.join(base_path, 'code', folder_name)
            interview_dir = os.path.join(base_path, 'interview_questions', folder_name)

            ppt_files = []
            code_files = []
            interview_files = []

            if os.path.isdir(ppt_dir):
                ppt_files = sorted(os.listdir(ppt_dir))
            if os.path.isdir(code_dir):
                code_files = sorted(os.listdir(code_dir))
            if os.path.isdir(interview_dir):
                interview_files = sorted(os.listdir(interview_dir))

            resources_dict[subject] = {
                'ppts': ppt_files,
                'code': code_files,
                'interview': interview_files
            }

        print(f"✅ Step 4: Resources built for {len(resources_dict)} subjects")

        # Fetch offers
        print("📡 Step 5: Fetching active offers...")
        today = datetime.today().strftime('%Y-%m-%d')
        
        try:
            cursor.execute("""
                SELECT 
                    c.COURSE_NAME, 
                    c.COURSE_FEE, 
                    o.DISCOUNT_PERCENTAGE,
                    o.VALID_FROM, 
                    o.VALID_TO
                FROM nrm_offers o
                JOIN nrm_courses c ON c.ID = o.COURSE_ID
                WHERE o.IS_ACTIVE = TRUE
                  AND (
                      (o.VALID_FROM IS NULL OR o.VALID_TO IS NULL)
                      OR (%s BETWEEN o.VALID_FROM AND o.VALID_TO)
                  )
            """, (today,))
            offers_rows = cursor.fetchall()
            print(f"✅ Step 5: Found {len(offers_rows)} active offers")
        except Exception as e:
            print(f"⚠️ Step 5 WARNING: {e}")
            offers_rows = []

        offers = {}
        for row in offers_rows:
            try:
                cname = row.get('COURSE_NAME')
                cfee = float(row.get('COURSE_FEE') or 0)
                disc = float(row.get('DISCOUNT_PERCENTAGE') or 0)
                discounted_fee = cfee - (cfee * disc / 100)

                offers[cname] = {
                    "original_fee": int(cfee),
                    "discounted_fee": int(discounted_fee),
                    "discount_percentage": int(disc)
                }
            except Exception as e:
                print(f"⚠️ Offer processing error: {e}")

        # Festival
        print("📡 Step 6: Fetching today's festival...")
        festival_today = None
        greeting = None
        try:
            cursor.execute("""
                SELECT FESTIVAL_NAME 
                FROM nrm_festivals 
                WHERE FESTIVAL_DATE = %s
            """, (today,))
            fr = cursor.fetchone()
            if fr:
                festival_today = fr.get('FESTIVAL_NAME')
                greeting = f"Happy {festival_today}!"
            print(f"✅ Step 6: Festival={festival_today}")
        except:
            print("⚠️ Step 6 festival fetch failed")

        offers_normalized = {
            subject: offers.get(subject) for subject in resources_dict.keys()
        }

        cursor.close()
        conn.close()

        print("📡 Step 7: Rendering template (with caching)...")

        # ==================================================
        # ⭐⭐ HERE IS THE IMPORTANT FIX — ADD CACHING ⭐⭐
        # ==================================================
        html = render_template(
            'resources.html',
            username=username,
            user=username,
            usertype=usertype,
            role=usertype,
            resources=resources_dict,
            tech="Informatica",
            lang="telugu",
            profile_pic=profile_pic,
            festival_today=festival_today,
            greeting=greeting,
            useremail=useremail,
            location=location_name,
            reg_id=reg_id,
            offers=offers_normalized
        )

        from flask import make_response
        resp = make_response(html)

        # ⭐ Allow browser to cache so BACK button does NOT reload
        resp.headers['Cache-Control'] = 'public, max-age=3600'
        resp.headers['Expires'] = '3600'

        return resp
        # ==================================================

    except Exception as e:
        print(f"❌ RESOURCES ERROR: {e}")
        traceback.print_exc()
        
        if cursor:
            try: cursor.close()
            except: pass

        if conn:
            try: conn.close()
            except: pass

        flash("An error occurred loading resources.")
        return redirect(url_for('home'))


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
        user_type = request.form.get("usertype").strip().lower()  # student or public
        username = request.form.get("username").strip()
        email = request.form.get("email").strip()
        phone = request.form.get("phone").strip()
        location_text = request.form.get("location").strip()
        gothram = request.form.get("gothram").strip()
        password = request.form.get("password")
        confirm_password = request.form.get("confirm_password")

        # ✅ Phone validation
        if not phone.isdigit() or len(phone) != 10:
            flash("Phone number must be exactly 10 digits.", "danger")
            return redirect(url_for('register'))

        # ✅ Password validation
        if not is_password_valid(password):
            flash("Password must contain exactly one special character, at least one uppercase letter, and one number.", "danger")
            return redirect(url_for('register'))

        if password != confirm_password:
            flash("Passwords do not match.", "danger")
            return redirect(url_for('register'))

        try:
            conn = get_db_connection()
            cur = conn.cursor(snowflake.connector.DictCursor)

            # ✅ Ensure location exists in nrm_locations for ALL users
            cur.execute("SELECT id FROM nrm_locations WHERE location = %s", (location_text,))
            loc_row = cur.fetchone()
            if loc_row:
                location_id = loc_row['id']
            else:
                cur.execute("INSERT INTO nrm_locations (location) VALUES (%s)", (location_text,))
                conn.commit()
                location_id = cur.lastrowid

            # ✅ Student-specific admin pre-check
            if user_type == "student":
                cur.execute("""
                    SELECT 1
                    FROM nrm_registrations r
                    JOIN nrm_students s ON r.student_id = s.id
                    WHERE s.email = %s
                """, (email,))
                if not cur.fetchone():
                    flash("Only users registered by admin can register as students.", "danger")
                    return redirect(url_for('register'))

            # ✅ Check username duplication
            cur.execute("SELECT 1 FROM nrm_users WHERE username = %s", (username,))
            if cur.fetchone():
                flash("Username already exists.", "danger")
                return redirect(url_for('register'))

            # ✅ Check duplicate email/phone in nrm_users
            cur.execute("SELECT 1 FROM nrm_users WHERE email = %s", (email,))
            if cur.fetchone():
                flash("This email is already registered.", "danger")
                return redirect(url_for('register'))

            cur.execute("SELECT 1 FROM nrm_users WHERE phone = %s", (phone,))
            if cur.fetchone():
                flash("This phone number is already registered.", "danger")
                return redirect(url_for('register'))

            # ✅ Insert into nrm_students if student
            if user_type == "student":
                cur.execute("""
                    INSERT INTO nrm_students (first_name, last_name, email, phone, location_id, gothram, registration_source)
                    VALUES (%s, %s, %s, %s, %s, %s, 'public')
                """, (username, '', email, phone, location_id, gothram))

            # ✅ Insert into nrm_users (NO location_id here)
            cur.execute("""
                INSERT INTO nrm_users (username, email, phone, gothram, usertype)
                VALUES (%s, %s, %s, %s, %s)
            """, (username, email, phone, gothram, user_type))
            user_id = cur.lastrowid

            # ✅ Insert into nrm_logins
            cur.execute("""
                INSERT INTO nrm_logins (user_id, email, phone, password, usertype, is_active)
                VALUES (%s, %s, %s, %s, %s, 'N')
            """, (user_id, email, phone, password, user_type))

            conn.commit()
            flash("Registration successful. Please log in.", "success")
            return redirect(url_for('home'))

        except Exception as e:
            conn.rollback()
            return f"<h3>Database Error: {e}</h3>"

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
        c["display_name"] = COURSE_MAPPING.get(db_name_norm, c["course_name"])

    for o in offers:
        db_name_norm = normalize(o["course_name"])
        o["display_name"] = COURSE_MAPPING.get(db_name_norm, o["course_name"])

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
    cur.execute("SELECT id, course_name FROM nrm_courses")
    courses = cur.fetchall()

    report_data = []

    for course in courses:
        course_id = course['id']
        course_name = course['course_name']
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
        cursor.execute("SELECT id, course_name FROM nrm_courses ORDER BY course_name")
        courses = cursor.fetchall()

        for course in courses:
            course_id = course['id']
            course_name = course['course_name']

            # Get total students for this course
            cursor.execute("""
                SELECT COUNT(*) as total_students
                FROM nrm_registrations
                WHERE course_id = %s
            """, (course_id,))
            total_students = cursor.fetchone()['total_students']

            # Get students with feedback for this course
            cursor.execute("""
                SELECT COUNT(DISTINCT f.student_id) as feedback_count
                FROM nrm_feedback f
                JOIN nrm_registrations r ON f.student_id = r.student_id
                WHERE r.course_id = %s
            """, (course_id,))
            feedback_count = cursor.fetchone()['feedback_count']

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

    # ---------- POST REQUEST ----------
    # NOTE: HTML FORM = request.form (NOT request.get_json)
    fname = request.form.get("first_name", "").strip()
    lname = request.form.get("last_name", "").strip()
    email = request.form.get("email", "").strip()
    phone = request.form.get("phone", "").strip()
    location_val = request.form.get("location", "").strip()
    course_raw = request.form.get("course")
    language_raw = request.form.get("language")
    start_date_raw = request.form.get("start_date", "").strip()

    # ---------- FIELD VALIDATION ----------
    if not fname:
        flash("First name is required.", "danger")
        return redirect(url_for("admin_register"))

    if not lname:
        flash("Last name is required.", "danger")
        return redirect(url_for("admin_register"))

    if not email:
        flash("Email is required.", "danger")
        return redirect(url_for("admin_register"))

    # Email domain rule
    if not (email.endswith("@gmail.com") or email.endswith("@chakorahub.com")):
        flash("Email must end with @gmail.com or @chakorahub.com", "danger")
        return redirect(url_for("admin_register"))

    if not phone.isdigit() or len(phone) != 10:
        flash("Phone number must be exactly 10 digits.", "danger")
        return redirect(url_for("admin_register"))

    if not location_val:
        flash("Location is required.", "danger")
        return redirect(url_for("admin_register"))

    try:
        course_id = int(course_raw)
        language_id = int(language_raw)
    except:
        flash("Invalid course or language.", "danger")
        return redirect(url_for("admin_register"))

    if not start_date_raw:
        flash("Start date is required.", "danger")
        return redirect(url_for("admin_register"))

    try:
        start_date = datetime.strptime(start_date_raw, "%Y-%m-%d")
    except:
        flash("Invalid date format.", "danger")
        return redirect(url_for("admin_register"))

    # ---------- DATABASE INSERT ----------
    try:
        cursor = connection.cursor()

        # Duplicate checks
        cursor.execute("SELECT 1 FROM nrm_students WHERE email=%s", (email,))
        if cursor.fetchone():
            flash("Email already exists.", "danger")
            return redirect(url_for("admin_register"))

        cursor.execute("SELECT 1 FROM nrm_students WHERE phone=%s", (phone,))
        if cursor.fetchone():
            flash("Phone number already exists.", "danger")
            return redirect(url_for("admin_register"))

        # Course code
        cursor.execute("SELECT course_code FROM nrm_courses WHERE id=%s", (course_id,))
        row = cursor.fetchone()
        if not row:
            flash("Invalid course selected.", "danger")
            return redirect(url_for("admin_register"))

        course_code = row[0]

        # Active status ID
        cursor.execute("SELECT id FROM nrm_statuses WHERE status='Active' LIMIT 1")
        row = cursor.fetchone()
        if not row:
            flash("Active status missing in DB.", "danger")
            return redirect(url_for("admin_register"))

        active_id = row[0]

        # Generate registration ID
        cursor.execute("SELECT COUNT(*) FROM nrm_registrations WHERE course_id=%s", (course_id,))
        seq = cursor.fetchone()[0] + 1
        initials = fname[0].upper() + lname[0].upper()
        reg_id = f"{course_code}{initials}{str(seq).zfill(3)}{start_date.strftime('%d%m')}"

        # Insert student
        cursor.execute("""
            INSERT INTO nrm_students(first_name, last_name, email, phone, location, registration_source)
            VALUES (%s, %s, %s, %s, %s, 'admin')
        """, (fname, lname, email, phone, location_val))

        cursor.execute("SELECT id FROM nrm_students WHERE email=%s ORDER BY id DESC LIMIT 1", (email,))
        student_id = cursor.fetchone()[0]

        # Insert user
        cursor.execute("""
            INSERT INTO nrm_users(username, email, phone, profile_pic, usertype)
            VALUES (%s, %s, %s, 'default.jpg', 'student')
        """, (f"{fname} {lname}", email, phone))
        
        cursor.execute("SELECT id FROM nrm_users WHERE email=%s ORDER BY id DESC LIMIT 1", (email,))
        user_id = cursor.fetchone()[0]

        # Insert login
        hashed_pwd = generate_password_hash("changeme123")
        cursor.execute("""
            INSERT INTO nrm_logins(user_id, email, phone, password, is_active)
            VALUES (%s, %s, %s, %s, 'Y')
        """, (user_id, email, phone, hashed_pwd))

        # Insert registration
        cursor.execute("""
            INSERT INTO nrm_registrations
            (registration_id, student_id, course_id, language_id, start_date, status_id, created_dt)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (reg_id, student_id, course_id, language_id, start_date_raw, active_id, datetime.now()))

        connection.commit()

        flash(f"Admin Registration Successful! Registration ID: {reg_id}", "success")
        return redirect(url_for("admin_register"))

    except Exception as e:
        connection.rollback()
        flash(f"Error: {str(e)}", "danger")
        print("Admin Register Error:", e)
        return redirect(url_for("admin_register"))

    finally:
        try: cursor.close()
        except: pass
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

@app.route('/nrm_enquiries', methods=['GET'])
def nrm_enquiries():
    user_data = None
    if 'user' in session:
        conn = get_db_connection()
        cursor = conn.cursor(snowflake.connector.DictCursor)
        cursor.execute("SELECT username, email, phone FROM nrm_users WHERE email = %s", (session['user'],))
        user_data = cursor.fetchone()
        cursor.close()
        conn.close()
    return render_template('enquiry.html', user=user_data)

@app.route('/submit_nrm_enquiries', methods=['POST'])
def submit_nrm_enquiries():
    # Check if it's an AJAX request
    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
    
    print(f"🔍 DEBUG: Session user: {session.get('user')}")
    print(f"🔍 DEBUG: Is AJAX: {is_ajax}")
    
    # Get form data
    name = request.form.get('name', '').strip()
    email = request.form.get('email', '').strip()
    phone = request.form.get('phone', '').strip()
    enquiry_text = request.form.get('enquiry', '').strip()

    print(f"🔍 DEBUG: Form data - Name: {name}, Email: {email}, Phone: {phone}, Enquiry: {enquiry_text}")

    # Validate required fields
    if not all([name, email, phone, enquiry_text]):
        if is_ajax:
            return jsonify({'success': False, 'message': 'All fields are required.'})
        else:
            flash("All fields are required.", "error")
            return redirect(url_for('nrm_enquiries'))

    conn = get_db_connection()
    cursor = conn.cursor(snowflake.connector.DictCursor)

    try:
        user_id = None
        
        # Check if user is logged in
        if 'user' in session:
            user_email = session['user']
            
            # Get user ID from nrm_users table for logged-in users
            cursor.execute("SELECT * FROM nrm_users WHERE email = %s", (user_email,))
            user_row = cursor.fetchone()
            
            print(f"🔍 DEBUG: User row: {user_row}")
            
            if user_row:
                # Try to get the ID - with multiple fallbacks
                if 'ID' in user_row:  # Snowflake sometimes returns uppercase
                    user_id = user_row['ID']
                elif 'id' in user_row:
                    user_id = user_row['id']
                elif 'USER_ID' in user_row:
                    user_id = user_row['USER_ID']
                else:
                    # Try to get the first column value
                    user_id = list(user_row.values())[0]
                    print(f"🔍 DEBUG: Using first column value as user_id: {user_id}")

                print(f"🔍 DEBUG: Final user_id: {user_id}")

        # Insert enquiry into database (user_id can be NULL for non-logged-in users)
        cursor.execute("""
            INSERT INTO nrm_enquiries (student_id, name, email, phone, enquiry, created_at, is_guest_enquiry)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), %s)
        """, (user_id, name, email, phone, enquiry_text, user_id is None))
        conn.commit()

        print("✅ Enquiry saved to database successfully!")

        # Send enquiry to Zapier webhook
        payload = {
            "name": name,
            "email": email,
            "phone": phone,
            "enquiry": enquiry_text,
            "source": "guest" if user_id is None else "logged_in_user"
        }
        
        try:
            zapier_response = requests.post(ZAPIER_WEBHOOK_URL, json=payload, timeout=10)
            zapier_response.raise_for_status()
            print("✅ Enquiry sent to Zapier successfully!")
        except requests.exceptions.RequestException as zap_err:
            print(f"⚠ Zapier webhook failed: {zap_err}")

        # Return success response
        success_message = "Your enquiry was submitted successfully!"
        if is_ajax:
            return jsonify({'success': True, 'message': success_message})
        else:
            flash(success_message, "success")
            return redirect(url_for('nrm_enquiries'))
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        print(f"❌ TRACEBACK: {traceback.format_exc()}")
        
        error_message = f"Error submitting enquiry: {str(e)}"
        if is_ajax:
            return jsonify({'success': False, 'message': error_message})
        else:
            flash(error_message, "error")
            return redirect(url_for('nrm_enquiries'))
    finally:
        cursor.close()
        conn.close()


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
    email = session.get('user')  # logged-in user's email

    if not email:
        flash("Please login first.")
        return redirect(url_for('login'))  # redirect to your login page

    conn = get_db_connection()
    cursor = conn.cursor(snowflake.connector.DictCursor)

    # ✅ Fetch address from DB
    cursor.execute("SELECT address FROM nrm_students WHERE email = %s", (email,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    address = result['address'] if result and result['address'] else ''

    return render_template('profile.html', address=address)

@app.route('/save_address', methods=['POST'])
def save_address():
    if 'user' not in session:
        flash("Please login first.")
        return redirect(url_for('login'))

    email = session['user']  # logged-in user's email
    address = request.form.get('address', '')

    conn = get_db_connection()
    cursor = conn.cursor()

    # ✅ Update address in DB
    cursor.execute("UPDATE nrm_students SET address = %s WHERE email = %s", (address, email))
    conn.commit()

    cursor.close()
    conn.close()

    # ✅ Save to session also (optional for quick display)
    session['address'] = address

    flash("Address saved successfully!")
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

    # Step 1: Get festivals for the month
    cursor.execute("""
        SELECT festival_date, festival_name
        FROM nrm_festivals
        WHERE MONTH(festival_date) = %s AND YEAR(festival_date) = %s
    """, (month, year))
    festival_rows = cursor.fetchall()
    month_nrm_festivals = {
        row['festival_date'].strftime('%Y-%m-%d'): row['festival_name']
        for row in festival_rows
    }

    # Step 2: Get all slots
    cursor.execute("SELECT id, slot_label FROM nrm_time_slots ORDER BY id")
    all_slots_rows = cursor.fetchall()
    all_slots = [row['slot_label'] for row in all_slots_rows]

    # Step 3: Get bookings for the month
    cursor.execute("""
        SELECT
            s.session_date,
            t.slot_label
        FROM
            nrm_session_bookings s
        JOIN
            nrm_time_slots t ON s.time_slot_id = t.id
        WHERE
            MONTH(s.session_date) = %s AND YEAR(s.session_date) = %s
        ORDER BY s.session_date, t.slot_label
    """, (month, year))
    booking_rows = cursor.fetchall()

    # Step 4: Organize bookings by date
    booked_slots_dict = {}
    for b in booking_rows:
        key = b['session_date'].strftime('%Y-%m-%d')
        if key not in booked_slots_dict:
            booked_slots_dict[key] = set()
        booked_slots_dict[key].add(b['slot_label'])

    conn.close()

    # Step 5: Build calendar data with all slots and their status
    days_in_month = (datetime(year, month % 12 + 1, 1) - timedelta(days=1)).day
    calendar_data = {}
    for day in range(1, days_in_month + 1):
        date_str = f"{year}-{month:02}-{day:02}"
        festival = month_nrm_festivals.get(date_str)
        bookings_status = []

        for slot in all_slots:
            status = "Booked" if booked_slots_dict.get(date_str) and slot in booked_slots_dict[date_str] else "Not booked"
            bookings_status.append({"slot": slot, "status": status})

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

    if request.method == 'POST':
        reg_id = request.form.get('reg_id').strip()

        conn = get_db_connection()
        cursor = conn.cursor(snowflake.connector.DictCursor)

        query = """
            SELECT s.first_name, s.last_name, c.course_name, st.status
            FROM nrm_registrations r
            JOIN nrm_students s ON r.student_id = s.id
            JOIN nrm_courses c ON r.course_id = c.id
            LEFT JOIN nrm_statuses st ON r.status_id = st.id
            WHERE r.registration_id = %s
        """
        cursor.execute(query, (reg_id,))
        student = cursor.fetchone()

        cursor.close()
        conn.close()

    return render_template(
        'generate-certificate.html',
        reg_id=reg_id,
        student=student
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
    try:
        # Connect to Snowflake using RSA key authentication
        conn = get_db_connection()
        cursor = conn.cursor(snowflake.connector.DictCursor)
    except Exception as e:
        flash(f"❌ Database connection error: {e}", "danger")
        return render_template("billing.html")

    if request.method == 'POST':
        try:
            # Get form inputs
            phone = str(request.form.get('phone', '')).strip()
            course_name = str(request.form.get('course_name', '')).strip()
            upi = str(request.form.get('upi', '')).strip()
            payment_input = request.form.get('payment', 0)

            try:
                current_payment = float(payment_input)
            except (ValueError, TypeError):
                current_payment = 0

            # --- Get user_id from phone ---
            cursor.execute("SELECT id FROM nrm_users WHERE phone = %s", (phone,))
            user_row = cursor.fetchone()
            if not user_row:
                flash("❌ User with this phone number not found.", "danger")
                return redirect(url_for('billing'))

            user_id = user_row['id']

            # --- Get course_id from course_name ---
            cursor.execute("SELECT id FROM nrm_courses WHERE course_name = %s", (course_name,))
            course_row = cursor.fetchone()
            if not course_row:
                flash("❌ Course not found.", "danger")
                return redirect(url_for('billing'))

            course_id = course_row['id']

            # --- Calculate total paid so far ---
            fee = get_fee_by_course(course_name)
            cursor.execute("""
                SELECT SUM(amount) AS total_paid
                FROM nrm_billing_entries
                WHERE user_id = %s AND course_id = %s
            """, (user_id, course_id))
            result = cursor.fetchone()
            total_paid_so_far = float(result['total_paid']) if result['total_paid'] else 0
            new_total_paid = total_paid_so_far + current_payment
            balance = max(fee - new_total_paid, 0)

            # --- Correct status logic: Completed only if total_paid == fee ---
            if new_total_paid == fee:
                status_id = 1  # Completed
            else:
                status_id = 2  # Active / Partial

            # --- Insert billing entry ---
            cursor.execute("""
                INSERT INTO nrm_billing_entries
                (user_id, course_id, upi_id, amount, discount, status_id, billing_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (user_id, course_id, upi, current_payment, 0.00, status_id, datetime.now()))

            # --- Update registration status ---
            cursor.execute("""
                UPDATE nrm_registrations
                SET status_id = %s
                WHERE student_id = %s AND course_id = %s
            """, (status_id, user_id, course_id))

            conn.commit()
            flash(f"✅ Payment of {current_payment} recorded. Total Paid: {new_total_paid}/{fee}, Balance: {balance}", "success")

        except Exception as e:
            flash(f"❌ Error processing billing: {e}", "danger")

        return redirect(url_for('billing'))

    # Fetch courses for dropdown (optional)
    cursor.execute("SELECT course_name FROM nrm_courses ORDER BY course_name")
    courses = cursor.fetchall()
    return render_template("billin.html", courses=courses)

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





###emp_resources#####


@app.route('/employee-resources')
def employee_resources():
    # Check if user is logged in
    if not session.get('employee_id'):
        return redirect(url_for('home'))
    
    employee_id = session.get('employee_id')
    employee_name = session.get('employee_name', 'Employee')
    
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return render_template('employee-resources.html', 
                             Employee_name=employee_name,
                             reg_id=employee_id,
                             profile_pic=None,
                             festival_today=None)
    
    try:
        cursor = conn.cursor()
        
        # Get employee profile picture
        profile_pic_query = """
        SELECT PROFILE_PIC 
        FROM EMP_NRM_PERSONAL 
        WHERE EMPLOYEE_ID = %s
        """
        cursor.execute(profile_pic_query, (employee_id,))
        profile_result = cursor.fetchone()
        profile_pic = profile_result[0] if profile_result else None
        
        # Check if today is a festival
        today = datetime.now().strftime('%Y-%m-%d')
        festival_query = """
        SELECT FESTIVAL_NAME 
        FROM EMP_NRM_FESTIVALS 
        WHERE FESTIVAL_DATE = %s
        """
        cursor.execute(festival_query, (today,))
        festival_result = cursor.fetchone()
        festival_today = festival_result[0] if festival_result else None
        
        cursor.close()
        conn.close()
        
        return render_template('employee-resources.html',
                             Employee_name=employee_name,
                             reg_id=employee_id,
                             profile_pic=profile_pic,
                             festival_today=festival_today)
        
    except Exception as e:
        print("Employee resources error:", e)
        if conn:
            conn.close()
        return render_template('employee-resources.html',
                             Employee_name=employee_name,
                             reg_id=employee_id,
                             profile_pic=None,
                             festival_today=None)

@app.route('/employee-personal-details', methods=['GET', 'POST'])
def personal_details():
    if not session.get('employee_id'):
        return redirect(url_for('home'))
    
    employee_id = session.get('employee_id')
    
    if request.method == 'POST':
        # Handle form submission
        first_name = request.form.get('firstName')
        last_name = request.form.get('lastName')
        email = request.form.get('email')
        phone = request.form.get('phone')
        address = request.form.get('address')
        dob = request.form.get('dob')
        
        conn = get_db_connection()
        if conn:
            try:
                cursor = conn.cursor()
                
                # Check if personal details already exist for this employee
                check_query = """
                SELECT EMPLOYEE_ID FROM EMP_NRM_PERSONAL WHERE EMPLOYEE_ID = %s
                """
                cursor.execute(check_query, (employee_id,))
                existing_record = cursor.fetchone()
                
                if existing_record:
                    # Update existing record
                    update_query = """
                    UPDATE EMP_NRM_PERSONAL 
                    SET FIRST_NAME = %s, LAST_NAME = %s, EMAIL = %s, PHONE = %s, ADDRESS = %s, DOB = %s
                    WHERE EMPLOYEE_ID = %s
                    """
                    cursor.execute(update_query, (first_name, last_name, email, phone, address, dob, employee_id))
                else:
                    # Insert new record
                    insert_query = """
                    INSERT INTO EMP_NRM_PERSONAL (EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, ADDRESS, DOB)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (employee_id, first_name, last_name, email, phone, address, dob))
                
                conn.commit()
                cursor.close()
                conn.close()
                
                flash('Personal details saved successfully!', 'success')
                return redirect(url_for('personal_details'))
                
            except Exception as e:
                print("Error saving personal details:", e)
                flash('Error saving personal details. Please try again.', 'error')
                if conn:
                    conn.close()
    
    # GET request - display existing data
    conn = get_db_connection()
    personal_data = None
    
    if conn:
        try:
            cursor = conn.cursor()
            
            # Get personal details
            query = """
            SELECT p.FIRST_NAME, p.LAST_NAME, p.DOB, p.EMAIL, p.PHONE, p.ADDRESS, p.PROFILE_PIC,
                   e.EMPLOYEE_NAME, e.DEPARTMENT, e.JOIN_DATE, e.STATUS,
                   d.DEPT_NAME
            FROM EMP_NRM_PERSONAL p
            JOIN EMP_NRM_EMPLOYEES e ON p.EMPLOYEE_ID = e.EMP_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS d ON e.DEPARTMENT = d.DEPT_ID
            WHERE p.EMPLOYEE_ID = %s
            """
            cursor.execute(query, (employee_id,))
            personal_data = cursor.fetchone()
            
            cursor.close()
            conn.close()
        except Exception as e:
            print("Error fetching personal details:", e)
            if conn:
                conn.close()
    
    return render_template('employee-personal-details.html', 
                         personal_data=personal_data,
                         employee_id=employee_id)

@app.route('/salary-info')
def salary_info():
    if not session.get('employee_id'):
        return redirect(url_for('home'))
    
    employee_id = session.get('employee_id')
    print(f"🔍 Fetching salary info for employee: {employee_id}")
    
    conn = get_db_connection()
    
    if not conn:
        error_msg = "Database connection error. Please try again later."
        print(f"❌ {error_msg}")
        return render_template('employee-salary.html', 
                             salary_info=None,
                             salary_slips=[],
                             error=error_msg)
    
    try:
        cursor = conn.cursor()
        
        # First, let's check if the employee exists
        emp_check_query = """
        SELECT EMP_ID, EMPLOYEE_NAME, DEPARTMENT, STATUS 
        FROM EMP_NRM_EMPLOYEES 
        WHERE EMP_ID = %s
        """
        cursor.execute(emp_check_query, (employee_id,))
        employee_data = cursor.fetchone()
        
        if not employee_data:
            error_msg = "Employee not found in the system."
            print(f"❌ {error_msg}")
            cursor.close()
            conn.close()
            return render_template('employee-salary.html',
                                 salary_info=None,
                                 salary_slips=[],
                                 error=error_msg)
        
        print(f"✅ Employee found: {employee_data[1]}")
        
        # Query to get salary details
        salary_query = """
        SELECT BASIC, HRA, ALLOWANCES, DEDUCTIONS, NET_SALARY
        FROM EMP_NRM_SALARY
        WHERE EMPLOYEE_ID = %s
        """
        cursor.execute(salary_query, (employee_id,))
        salary_data = cursor.fetchone()
        
        if salary_data:
            print(f"✅ Salary data found for employee {employee_id}")
            # Format the data for the template
            salary_info = {
                'id': employee_data[0],
                'name': employee_data[1],
                'department': employee_data[2] if employee_data[2] else 'N/A',
                'basic': float(salary_data[0]) if salary_data[0] is not None else 0.0,
                'hra': float(salary_data[1]) if salary_data[1] is not None else 0.0,
                'allowances': float(salary_data[2]) if salary_data[2] is not None else 0.0,
                'deductions': float(salary_data[3]) if salary_data[3] is not None else 0.0,
                'net_salary': float(salary_data[4]) if salary_data[4] is not None else 0.0,
                'status': employee_data[3] if employee_data[3] else 'Active'
            }
            
            # Get salary slips if available
            slips_query = """
            SELECT SLIP_ID, MONTH, YEAR, FILE_PATH, GENERATED_AT
            FROM EMP_NRM_SALARY_SLIPS
            WHERE EMP_ID = %s
            ORDER BY YEAR DESC, MONTH DESC
            """
            cursor.execute(slips_query, (employee_id,))
            salary_slips = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return render_template('employee-salary.html', 
                                 salary_info=salary_info,
                                 salary_slips=salary_slips,
                                 error=None)
        else:
            print(f"⚠️ No salary data found for employee {employee_id}")
            # Get department name if available
            dept_query = """
            SELECT DEPT_NAME FROM EMP_NRM_DEPARTMENTS WHERE DEPT_ID = %s
            """
            cursor.execute(dept_query, (employee_data[2],))
            dept_result = cursor.fetchone()
            dept_name = dept_result[0] if dept_result else employee_data[2]
            
            # Create basic employee info without salary data
            employee_info = {
                'id': employee_data[0],
                'name': employee_data[1],
                'department': dept_name if dept_name else 'N/A',
                'status': employee_data[3] if employee_data[3] else 'Active'
            }
            
            cursor.close()
            conn.close()
            
            return render_template('employee-salary.html',
                                 salary_info=employee_info,
                                 salary_slips=[],
                                 error="Salary information not found for your employee ID.")
            
    except Exception as e:
        print(f"❌ Salary info error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        if conn:
            try:
                cursor.close()
                conn.close()
            except:
                pass
        
        return render_template('employee-salary.html',
                             salary_info=None,
                             salary_slips=[],
                             error=f"Error fetching salary information: {str(e)}")

@app.route('/leave-tracker', methods=['GET', 'POST'])
def leave_tracker():
    if not session.get('employee_id'):
        return redirect(url_for('home'))
    
    employee_id = session.get('employee_id')
    
    # Get current month and year for calendar display
    today = datetime.now()
    month = request.args.get('month', today.month, type=int)
    year = request.args.get('year', today.year, type=int)
    
    # Handle month navigation
    if month > 12:
        month = 1
        year += 1
    elif month < 1:
        month = 12
        year -= 1
    
    # Calculate previous and next months
    prev_month = month - 1
    prev_year = year
    if prev_month < 1:
        prev_month = 12
        prev_year = year - 1
        
    next_month = month + 1
    next_year = year
    if next_month > 12:
        next_month = 1
        next_year = year + 1
    
    conn = get_db_connection()
    
    if request.method == 'POST':
        # Handle leave application
        if conn:
            try:
                start_date = request.form.get('start_date')
                end_date = request.form.get('end_date')
                reason = request.form.get('reason')
                
                cursor = conn.cursor()
                
                # Insert leave application
                insert_query = """
                INSERT INTO EMP_NRM_LEAVE (EMP_ID, START_DATE, END_DATE, REASON, STATUS, APPLIED_AT)
                VALUES (%s, %s, %s, %s, 'Pending', CURRENT_TIMESTAMP())
                """
                cursor.execute(insert_query, (employee_id, start_date, end_date, reason))
                conn.commit()
                cursor.close()
                
                flash('Leave application submitted successfully!', 'success')
                return redirect(url_for('leave_tracker'))
                
            except Exception as e:
                print("Error applying for leave:", e)
                flash('Error applying for leave. Please try again.', 'error')
                if conn:
                    conn.close()
    
    # GET request - display leave data and calendar
    leave_data = []
    festivals = []
    calendar_data = []
    
    if conn:
        try:
            cursor = conn.cursor()
            
            # Get employee's leave data
            leave_query = """
            SELECT LEAVE_ID, START_DATE, END_DATE, REASON, STATUS, APPLIED_AT
            FROM EMP_NRM_LEAVE
            WHERE EMP_ID = %s
            ORDER BY APPLIED_AT DESC
            """
            cursor.execute(leave_query, (employee_id,))
            leave_data = cursor.fetchall()
            
            # Get festivals for the current month
            festival_query = """
            SELECT FESTIVAL_NAME, FESTIVAL_DATE
            FROM EMP_NRM_FESTIVALS
            WHERE EXTRACT(YEAR FROM FESTIVAL_DATE) = %s 
            AND EXTRACT(MONTH FROM FESTIVAL_DATE) = %s
            ORDER BY FESTIVAL_DATE
            """
            cursor.execute(festival_query, (year, month))
            festivals = cursor.fetchall()
            
            # Create calendar data
            cal = calendar.Calendar()
            month_days = cal.monthdayscalendar(year, month)
            
            # Convert to our format
            weeks = []
            for week in month_days:
                week_data = []
                for day in week:
                    if day == 0:
                        week_data.append({'date': None, 'type': 'empty'})
                    else:
                        current_date = datetime(year, month, day).date()
                        day_type = 'normal'
                        festival_name = None
                        leave_info = None
                        
                        # Check if it's a festival
                        for festival in festivals:
                            if festival[1].date() == current_date:
                                festival_name = festival[0]
                                day_type = 'festival'
                                break
                        
                        # Check if employee has leave on this day
                        for leave in leave_data:
                            leave_start = leave[1].date() if leave[1] else None
                            leave_end = leave[2].date() if leave[2] else None
                            if leave_start and leave_end and leave_start <= current_date <= leave_end:
                                leave_info = leave[3]  # Reason
                                if day_type == 'festival':
                                    day_type = 'both'
                                else:
                                    day_type = 'leave'
                                break
                        
                        week_data.append({
                            'date': current_date,
                            'type': day_type,
                            'festival': festival_name,
                            'leave': leave_info
                        })
                weeks.append(week_data)
            
            calendar_data = weeks
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print("Leave tracker error:", e)
            if conn:
                conn.close()
    
    month_name = calendar.month_name[month]
    
    return render_template('emp-leave.html',
                         leave_data=leave_data,
                         calendar_data=calendar_data,
                         month=month,
                         year=year,
                         month_name=month_name,
                         prev_month=prev_month,
                         prev_year=prev_year,
                         next_month=next_month,
                         next_year=next_year,
                         employee_id=employee_id,
                         employee_name=session.get('employee_name', 'Employee'))

@app.route('/id-card')
def id_card():
    if not session.get('employee_id'):
        return redirect(url_for('home'))
    
    employee_id = session.get('employee_id')
    conn = get_db_connection()
    
    if not conn:
        flash('Database connection error. Please try again later.', 'error')
        return render_template('employee-idcard.html', 
                             id_card_data=None,
                             personal_data=None,
                             employee_data=None,
                             error="Database connection failed")
    
    try:
        cursor = conn.cursor()
        
        # Get ID card data
        id_query = """
        SELECT ID_NUMBER, ISSUE_DATE, EXPIRY_DATE
        FROM EMP_NRM_IDCARD
        WHERE EMPLOYEE_ID = %s
        """
        cursor.execute(id_query, (employee_id,))
        id_card_data = cursor.fetchone()
        
        # Get personal data
        personal_query = """
        SELECT FIRST_NAME, LAST_NAME, DOB, EMAIL, PHONE, ADDRESS, PROFILE_PIC
        FROM EMP_NRM_PERSONAL
        WHERE EMPLOYEE_ID = %s
        """
        cursor.execute(personal_query, (employee_id,))
        personal_data = cursor.fetchone()
        
        # Get employee data
        employee_query = """
        SELECT e.EMP_ID, e.EMPLOYEE_NAME, e.DEPARTMENT, e.JOIN_DATE, e.STATUS,
               d.DEPT_NAME, des.TITLE as DESIGNATION
        FROM EMP_NRM_EMPLOYEES e
        LEFT JOIN EMP_NRM_DEPARTMENTS d ON e.DEPARTMENT = d.DEPT_ID
        LEFT JOIN EMP_NRM_JOB_WORK jw ON e.EMP_ID = jw.EMPLOYEE_ID
        LEFT JOIN EMP_NRM_DESIGNATIONS des ON jw.DESIGNATION_ID = des.DESIGNATION_ID
        WHERE e.EMP_ID = %s
        """
        cursor.execute(employee_query, (employee_id,))
        employee_data = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return render_template('employee-idcard.html',
                             id_card_data=id_card_data,
                             personal_data=personal_data,
                             employee_data=employee_data,
                             error=None)
        
    except Exception as e:
        print(f"❌ ID card error: {str(e)}")
        if conn:
            conn.close()
        
        return render_template('employee-idcard.html',
                             id_card_data=None,
                             personal_data=None,
                             employee_data=None,
                             error=f"Error fetching ID card information: {str(e)}")

@app.route('/employee-queries', methods=['GET', 'POST'])
def employee_queries():
    if not session.get('employee_id'):
        return redirect(url_for('home'))
    
    employee_id = session.get('employee_id')
    
    if request.method == 'POST':
        # Handle query submission
        query_text = request.form.get('query_text')
        
        conn = get_db_connection()
        if conn:
            try:
                cursor = conn.cursor()
                insert_query = """
                INSERT INTO EMP_NRM_QUERIES (EMPLOYEE_ID, QUERY_TEXT, STATUS, CREATED_AT)
                VALUES (%s, %s, 'Pending', CURRENT_TIMESTAMP())
                """
                cursor.execute(insert_query, (employee_id, query_text))
                conn.commit()
                cursor.close()
                conn.close()
                
                flash('Query submitted successfully!', 'success')
                return redirect(url_for('employee_queries'))
                
            except Exception as e:
                print("Error submitting query:", e)
                flash('Error submitting query. Please try again.', 'error')
                if conn:
                    conn.close()
    
    # GET request - display existing queries
    conn = get_db_connection()
    queries_data = None
    
    if conn:
        try:
            cursor = conn.cursor()
            query = """
            SELECT QUERY_ID, QUERY_TEXT, STATUS, CREATED_AT
            FROM EMP_NRM_QUERIES
            WHERE EMPLOYEE_ID = %s
            ORDER BY CREATED_AT DESC
            """
            cursor.execute(query, (employee_id,))
            queries_data = cursor.fetchall()
            cursor.close()
            conn.close()
        except Exception as e:
            print("Queries error:", e)
            if conn:
                conn.close()
    
    return render_template('employee-queries.html', queries_data=queries_data)

@app.route('/Emp-logout')
def Emp_logout():
    session.clear()
    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('home'))

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
        SELECT e.EMP_ID, e.EMPLOYEE_NAME, e.DEPARTMENT,
               d.DEPT_NAME, des.TITLE as DESIGNATION
        FROM EMP_NRM_EMPLOYEES e
        LEFT JOIN EMP_NRM_DEPARTMENTS d ON e.DEPARTMENT = d.DEPT_ID
        LEFT JOIN EMP_NRM_JOB_WORK jw ON e.EMP_ID = jw.EMPLOYEE_ID
        LEFT JOIN EMP_NRM_DESIGNATIONS des ON jw.DESIGNATION_ID = des.DESIGNATION_ID
        WHERE e.EMP_ID = %s
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

#============================================
#Employee_appraisal
#============================================
# Add these routes to your existing app.py

@app.route('/employee-appraisal')
def employee_appraisal():
    """Main appraisal portal page"""
    if 'employee_id' not in session:
        return redirect(url_for('employee_home'))
    
    return render_template('employee-appraisal.html')

# ==========================================================
# GOALS MANAGEMENT
# ==========================================================

@app.route('/api/appraisal/goals', methods=['GET', 'POST'])
def appraisal_goals():
    """Handle goal setting operations"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    employee_id = session['employee_id']
    conn = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        if request.method == 'GET':
            # Get existing goals for the employee
            cursor.execute("""
                SELECT goal_id, goal_description, target_date, status, 
                       created_date, updated_date
                FROM EMP_NRM_APPRAISAL_GOALS 
                WHERE employee_id = %s 
                ORDER BY created_date DESC
            """, (employee_id,))
            goals = cursor.fetchall()
            
            return jsonify({
                'success': True,
                'goals': goals
            })
            
        elif request.method == 'POST':
            # Add new goal
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
            
            # Get the newly created goal
            cursor.execute("""
                SELECT goal_id, goal_description, target_date, status, 
                       created_date, updated_date
                FROM EMP_NRM_APPRAISAL_GOALS 
                WHERE goal_id = %s
            """, (cursor.lastrowid,))
            new_goal = cursor.fetchone()
            
            return jsonify({
                'success': True,
                'message': 'Goal added successfully',
                'goal': new_goal
            })
            
    except Exception as e:
        print(f"❌ Appraisal goals error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/appraisal/goals/<int:goal_id>', methods=['PUT', 'DELETE'])
def manage_goal(goal_id):
    """Update or delete a specific goal"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    employee_id = session['employee_id']
    conn = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        # Verify the goal belongs to the employee
        cursor.execute("SELECT employee_id FROM EMP_NRM_APPRAISAL_GOALS WHERE goal_id = %s", (goal_id,))
        goal = cursor.fetchone()
        
        if not goal or goal['employee_id'] != employee_id:
            return jsonify({'error': 'Goal not found or access denied'}), 404
        
        if request.method == 'PUT':
            # Update goal
            data = request.json
            goal_description = data.get('goal_description')
            target_date = data.get('target_date')
            status = data.get('status')
            
            update_fields = []
            params = []
            
            if goal_description:
                update_fields.append("goal_description = %s")
                params.append(goal_description)
            if target_date:
                update_fields.append("target_date = %s")
                params.append(target_date)
            if status:
                update_fields.append("status = %s")
                params.append(status)
            
            if not update_fields:
                return jsonify({'error': 'No fields to update'}), 400
            
            update_fields.append("updated_date = CURRENT_TIMESTAMP()")
            params.append(goal_id)
            
            query = f"UPDATE EMP_NRM_APPRAISAL_GOALS SET {', '.join(update_fields)} WHERE goal_id = %s"
            cursor.execute(query, params)
            conn.commit()
            
            return jsonify({'success': True, 'message': 'Goal updated successfully'})
            
        elif request.method == 'DELETE':
            # Delete goal
            cursor.execute("DELETE FROM EMP_NRM_APPRAISAL_GOALS WHERE goal_id = %s", (goal_id,))
            conn.commit()
            
            return jsonify({'success': True, 'message': 'Goal deleted successfully'})
            
    except Exception as e:
        print(f"❌ Manage goal error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ==========================================================
# FEEDBACK MANAGEMENT
# ==========================================================

@app.route('/api/appraisal/feedback', methods=['GET', 'POST'])
def appraisal_feedback():
    """Handle manager feedback operations"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    employee_id = session['employee_id']
    conn = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        if request.method == 'GET':
            # Get feedback for the employee
            cursor.execute("""
                SELECT f.feedback_id, f.feedback_text, f.manager_id, 
                       e.employee_name as manager_name, f.created_date, f.feedback_type
                FROM EMP_NRM_APPRAISAL_FEEDBACK f
                LEFT JOIN EMP_NRM_EMPLOYEES e ON f.manager_id = e.emp_id
                WHERE f.employee_id = %s 
                ORDER BY f.created_date DESC
            """, (employee_id,))
            feedbacks = cursor.fetchall()
            
            # Check if user is manager
            cursor.execute("SELECT usertype FROM EMP_NRM_EMPLOYEES WHERE emp_id = %s", (employee_id,))
            employee = cursor.fetchone()
            is_manager = employee and employee['usertype'] == 'manager'
            
            return jsonify({
                'success': True,
                'feedbacks': feedbacks,
                'is_manager': is_manager
            })
            
        elif request.method == 'POST':
            # Check if user is a manager
            cursor.execute("SELECT usertype FROM EMP_NRM_EMPLOYEES WHERE emp_id = %s", (employee_id,))
            employee = cursor.fetchone()
            
            if not employee or employee['usertype'] != 'manager':
                return jsonify({'error': 'Only managers can submit feedback'}), 403
            
            # Add manager feedback
            data = request.json
            target_employee_id = data.get('employee_id')
            feedback_text = data.get('feedback_text')
            feedback_type = data.get('feedback_type', 'general')
            
            if not target_employee_id or not feedback_text:
                return jsonify({'error': 'Employee ID and feedback text are required'}), 400
            
            cursor.execute("""
                INSERT INTO EMP_NRM_APPRAISAL_FEEDBACK 
                (employee_id, manager_id, feedback_text, feedback_type)
                VALUES (%s, %s, %s, %s)
            """, (target_employee_id, employee_id, feedback_text, feedback_type))
            
            conn.commit()
            
            return jsonify({
                'success': True,
                'message': 'Feedback submitted successfully'
            })
            
    except Exception as e:
        print(f"❌ Appraisal feedback error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ==========================================================
# TRAINING MANAGEMENT
# ==========================================================

@app.route('/api/appraisal/trainings', methods=['GET', 'POST'])
def appraisal_trainings():
    """Handle training and development operations"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    employee_id = session['employee_id']
    conn = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        if request.method == 'GET':
            # Get existing trainings for the employee
            cursor.execute("""
                SELECT training_id, training_name, completion_date, status, 
                       skills_acquired, created_date, updated_date
                FROM EMP_NRM_APPRAISAL_TRAININGS 
                WHERE employee_id = %s 
                ORDER BY created_date DESC
            """, (employee_id,))
            trainings = cursor.fetchall()
            
            return jsonify({
                'success': True,
                'trainings': trainings
            })
            
        elif request.method == 'POST':
            # Add new training
            data = request.json
            training_name = data.get('training_name')
            completion_date = data.get('completion_date')
            status = data.get('status', 'Completed')
            skills_acquired = data.get('skills_acquired', '')
            
            if not training_name or not completion_date:
                return jsonify({'error': 'Training name and completion date are required'}), 400
            
            cursor.execute("""
                INSERT INTO EMP_NRM_APPRAISAL_TRAININGS 
                (employee_id, training_name, completion_date, status, skills_acquired)
                VALUES (%s, %s, %s, %s, %s)
            """, (employee_id, training_name, completion_date, status, skills_acquired))
            
            conn.commit()
            
            # Get the newly created training
            cursor.execute("""
                SELECT training_id, training_name, completion_date, status, 
                       skills_acquired, created_date, updated_date
                FROM EMP_NRM_APPRAISAL_TRAININGS 
                WHERE training_id = %s
            """, (cursor.lastrowid,))
            new_training = cursor.fetchone()
            
            return jsonify({
                'success': True,
                'message': 'Training added successfully',
                'training': new_training
            })
            
    except Exception as e:
        print(f"❌ Appraisal trainings error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/appraisal/trainings/<int:training_id>', methods=['PUT', 'DELETE'])
def manage_training(training_id):
    """Update or delete a specific training"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    employee_id = session['employee_id']
    conn = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        # Verify the training belongs to the employee
        cursor.execute("SELECT employee_id FROM EMP_NRM_APPRAISAL_TRAININGS WHERE training_id = %s", (training_id,))
        training = cursor.fetchone()
        
        if not training or training['employee_id'] != employee_id:
            return jsonify({'error': 'Training not found or access denied'}), 404
        
        if request.method == 'PUT':
            # Update training
            data = request.json
            training_name = data.get('training_name')
            completion_date = data.get('completion_date')
            status = data.get('status')
            skills_acquired = data.get('skills_acquired')
            
            update_fields = []
            params = []
            
            if training_name:
                update_fields.append("training_name = %s")
                params.append(training_name)
            if completion_date:
                update_fields.append("completion_date = %s")
                params.append(completion_date)
            if status:
                update_fields.append("status = %s")
                params.append(status)
            if skills_acquired is not None:
                update_fields.append("skills_acquired = %s")
                params.append(skills_acquired)
            
            if not update_fields:
                return jsonify({'error': 'No fields to update'}), 400
            
            update_fields.append("updated_date = CURRENT_TIMESTAMP()")
            params.append(training_id)
            
            query = f"UPDATE EMP_NRM_APPRAISAL_TRAININGS SET {', '.join(update_fields)} WHERE training_id = %s"
            cursor.execute(query, params)
            conn.commit()
            
            return jsonify({'success': True, 'message': 'Training updated successfully'})
            
        elif request.method == 'DELETE':
            # Delete training
            cursor.execute("DELETE FROM EMP_NRM_APPRAISAL_TRAININGS WHERE training_id = %s", (training_id,))
            conn.commit()
            
            return jsonify({'success': True, 'message': 'Training deleted successfully'})
            
    except Exception as e:
        print(f"❌ Manage training error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ==========================================================
# APPRAISAL SUMMARY
# ==========================================================

@app.route('/api/appraisal/summary')
def appraisal_summary():
    """Get appraisal summary for the employee"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    employee_id = session['employee_id']
    conn = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        # Get employee details
        cursor.execute("""
            SELECT e.emp_id, e.employee_name, e.department, d.dept_name,
                   jw.designation_id, des.title as designation,
                   a.final_rating, a.comments, a.appraisal_date
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_DEPARTMENTS d ON e.department = d.dept_id
            LEFT JOIN EMP_NRM_JOB_WORK jw ON e.emp_id = jw.employee_id
            LEFT JOIN EMP_NRM_DESIGNATIONS des ON jw.designation_id = des.designation_id
            LEFT JOIN EMP_NRM_APPRAISAL_SUMMARY a ON e.emp_id = a.employee_id
            WHERE e.emp_id = %s
            ORDER BY a.appraisal_date DESC
            LIMIT 1
        """, (employee_id,))
        
        appraisal_data = cursor.fetchone()
        
        # Get goals count
        cursor.execute("SELECT COUNT(*) as goals_count FROM EMP_NRM_APPRAISAL_GOALS WHERE employee_id = %s", (employee_id,))
        goals_count = cursor.fetchone()['goals_count']
        
        # Get completed trainings count
        cursor.execute("SELECT COUNT(*) as trainings_count FROM EMP_NRM_APPRAISAL_TRAININGS WHERE employee_id = %s AND status = 'Completed'", (employee_id,))
        trainings_count = cursor.fetchone()['trainings_count']
        
        # Get feedback count
        cursor.execute("SELECT COUNT(*) as feedback_count FROM EMP_NRM_APPRAISAL_FEEDBACK WHERE employee_id = %s", (employee_id,))
        feedback_count = cursor.fetchone()['feedback_count']
        
        summary = {
            'employee_info': {
                'name': appraisal_data['employee_name'] if appraisal_data else '',
                'department': appraisal_data['dept_name'] if appraisal_data else '',
                'designation': appraisal_data['designation'] if appraisal_data else ''
            },
            'appraisal_data': appraisal_data,
            'stats': {
                'goals_count': goals_count,
                'trainings_count': trainings_count,
                'feedback_count': feedback_count
            }
        }
        
        return jsonify({
            'success': True,
            'summary': summary
        })
        
    except Exception as e:
        print(f"❌ Appraisal summary error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/appraisal/submit', methods=['POST'])
def submit_appraisal():
    """Submit final appraisal"""
    if 'employee_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    employee_id = session['employee_id']
    conn = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        data = request.json
        final_rating = data.get('final_rating')
        comments = data.get('comments', '')
        
        if not final_rating:
            return jsonify({'error': 'Final rating is required'}), 400
        
        # Check if appraisal already exists
        cursor.execute("SELECT * FROM EMP_NRM_APPRAISAL_SUMMARY WHERE employee_id = %s", (employee_id,))
        existing_appraisal = cursor.fetchone()
        
        if existing_appraisal:
            # Update existing appraisal
            cursor.execute("""
                UPDATE EMP_NRM_APPRAISAL_SUMMARY 
                SET final_rating = %s, comments = %s, appraisal_date = CURRENT_TIMESTAMP()
                WHERE employee_id = %s
            """, (final_rating, comments, employee_id))
        else:
            # Insert new appraisal
            cursor.execute("""
                INSERT INTO EMP_NRM_APPRAISAL_SUMMARY 
                (employee_id, final_rating, comments)
                VALUES (%s, %s, %s)
            """, (employee_id, final_rating, comments))
        
        conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'Appraisal submitted successfully'
        })
        
    except Exception as e:
        print(f"❌ Submit appraisal error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

#===================================
#Employee-appraisal
#===================================
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
# Replace this with your actual API Gateway/Lambda invoke URL
LAMBDA_URL = "https://m8e0rv8b4i.execute-api.eu-north-1.amazonaws.com/default/brs-upload"

@app.route("/industry/brs", methods=["GET", "POST"])
def industry_brs():
    # GET -> show form
    if request.method == "GET":
        return render_template("submit_brs.html")

    # POST -> build JSON payload and call Lambda
    try:
        form = request.form
        file = request.files.get("file")
        if not file:
            return render_template("submit_brs.html", message="Please upload a PDF file.", success=False)

        # read + encode file (base64)
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
            "version_no": form.get("version_no"),
            "version_notes": form.get("version_notes"),
            "business_objective": form.get("business_objective"),
            "scope": form.get("scope"),
            "start_date": form.get("start_date"),
            "end_date": form.get("end_date"),
            "contact_email": form.get("contact_email"),
            "contact_phone": form.get("contact_phone"),
            "filename": file.filename,
            "filedata": encoded_file
        }

        # call Lambda (API Gateway) - expects JSON
        resp = requests.post(LAMBDA_URL, json=payload, timeout=30)
        try:
            result = resp.json()
        except Exception:
            result = {"message": f"Lambda responded with status {resp.status_code}", "raw": resp.text}

        if resp.status_code == 200:
            msg = result.get("message", "Submitted successfully")
            return render_template("submit_brs.html", message=msg, success=True)
        else:
            err = result.get("error") or result.get("message") or result
            return render_template("submit_brs.html", message=f"Upload failed: {err}", success=False)

    except requests.exceptions.RequestException as e:
        return render_template("submit_brs.html", message=f"Request error: {str(e)}", success=False)
    except Exception as e:
        # show error on the page (in dev). In prod log it and show friendly message.
        current_app.logger.exception("BRS submit error")
        return render_template("submit_brs.html", message=f"Internal error: {str(e)}", success=False)
# --------------------------------------------------------------
# RENDER APPLICATION FORM PAGE
# --------------------------------------------------------------
@app.route("/applicationform")
def application_form():
    return render_template("applicationform.html")


# --------------------------------------------------------------
# HANDLE FORM SUBMISSION
# --------------------------------------------------------------
@app.route("/submit-application", methods=["POST"])
def submit_application():
    full_name = request.form.get("full_name")
    email = request.form.get("email")
    phone = request.form.get("phone")
    organisation = request.form.get("organisation")
    collaboration_type = request.form.get("collaboration_type")
    project_title = request.form.get("project_title")
    description = request.form.get("description")
    start_date = request.form.get("start_date")

    # FIXED: use existing function
    conn = get_db_connection()
    if conn is None:
        flash("Snowflake connection failed!", "error")
        return redirect(url_for("application_form"))

    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO INDUSTRY_APPLICATIONS 
            (FULL_NAME, EMAIL, PHONE, ORGANISATION, COLLABORATION_TYPE, PROJECT_TITLE, DESCRIPTION, START_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            full_name, email, phone, organisation, collaboration_type,
            project_title, description, start_date if start_date else None
        ))

        conn.commit()
        cursor.close()
        conn.close()

        flash("Application submitted successfully!", "success")
        return redirect(url_for("application_form"))

    except Exception as e:
        flash(f"Error submitting application: {e}", "error")
        return redirect(url_for("application_form"))

#-------------singoff---------------#
@app.route("/signoff-form")
def signoff_form():
    return render_template("organizationsingoff.html")
@app.route("/org-signoff", methods=["POST"])
def org_signoff():
    org_name = request.form.get("org_name")
    authorized_person = request.form.get("authorized_person")
    email = request.form.get("email")
    phone = request.form.get("phone")
    brs_id = request.form.get("brs_id")
    approval_notes = request.form.get("approval_notes")
    approval_status = request.form.get("approval_status")

    conn = get_db_connection()  # your existing Snowflake RSA connection
    if conn is None:
        return render_template(
            "organizationsingoff.html",
            message="❌ Failed to connect to Snowflake",
            message_type="error"
        )

    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO INDUSTRY_ORG_SIGNOFF 
            (ORG_NAME, AUTHORIZED_PERSON, EMAIL, PHONE, BRS_ID, APPROVAL_NOTES, APPROVAL_STATUS)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            org_name, authorized_person, email, phone, brs_id,
            approval_notes, approval_status
        ))

        conn.commit()
        cursor.close()
        conn.close()

        return render_template(
            "organizationsingoff.html",
            message="✔ Organization Sign-Off submitted successfully!",
            message_type="success"
        )

    except Exception as e:
        return render_template(
            "organizationsingoff.html",
            message=f"❌ Error submitting signoff: {e}",
            message_type="error"
        )


@app.route("/track-project-status")
def track_project_status_page():
    return render_template("track-project-status.html")

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080, debug=True)