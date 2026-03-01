# employee_service.py
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from werkzeug.security import check_password_hash
from datetime import datetime
import calendar
import os

# ==========================================
# DATABASE CONNECTION
# ==========================================
def get_db_connection():
    try:
        with open("rsa_key.p8", "rb") as key_file:
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
        
        conn = snowflake.connector.connect(
            user='ChakoraHub',
            account='gpguymt-ta88699',
            private_key=pkb,
            warehouse='COMPUTE_WH',
            database='"VSRSUBHASH$CHAKORA_DB"',
            schema="CHAKORA"
        )
        print("✅ Employee Service: Connected to Snowflake")
        return conn
        
    except Exception as e:
        print(f"❌ Employee Service DB Error: {e}")
        return None

# ==========================================
# FASTAPI APP
# ==========================================
app = FastAPI(title="Employee Microservice", version="1.0")

# Add CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# MODELS
# ==========================================
class EmployeeLogin(BaseModel):
    employee_id: str
    password: str

class PersonalDetailsUpdate(BaseModel):
    first_name: str
    last_name: str
    email: str
    phone: str
    address: Optional[str] = None
    dob: Optional[str] = None

class LeaveApplication(BaseModel):
    start_date: str
    end_date: str
    reason: str

class QuerySubmission(BaseModel):
    query_text: str

# ==========================================
# AUTH DEPENDENCY
# ==========================================
def get_current_employee(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    # Extract employee_id from token (simplified - you should use JWT in production)
    employee_id = authorization.replace("Bearer ", "")
    return employee_id

# ==========================================
# ROUTES
# ==========================================

# Health check
@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "employee"}

# Employee Login
@app.post("/employee/login")
def employee_login(data: EmployeeLogin):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        # Fetch employee login
        cursor.execute("""
            SELECT EMPLOYEE_ID, PASSWORD
            FROM EMP_NRM_LOGINS
            WHERE EMPLOYEE_ID = %s
            LIMIT 1
        """, (data.employee_id,))
        
        row = cursor.fetchone()
        
        if not row:
            raise HTTPException(400, "Employee ID not found")
        
        db_password = row["PASSWORD"] or ""
        
        # Validate password
        if db_password.startswith("scrypt:"):
            valid = check_password_hash(db_password, data.password)
        else:
            valid = (db_password == data.password)
        
        if not valid:
            raise HTTPException(401, "Invalid password")
        
        # Fetch employee name
        cursor.execute("""
            SELECT EMPLOYEE_NAME
            FROM EMP_NRM_EMPLOYEES
            WHERE EMPLOYEE_ID = %s
        """, (data.employee_id,))
        
        emp_info = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return {
            "message": "Login successful",
            "employee_id": data.employee_id,
            "employee_name": emp_info["EMPLOYEE_NAME"] if emp_info else "Employee",
            "token": data.employee_id  # In production, generate JWT token
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Login error: {e}")
        raise HTTPException(500, f"Login error: {str(e)}")
    finally:
        if conn:
            conn.close()

# Get Employee Resources (Dashboard)
@app.get("/resources")
def get_employee_resources(employee_id: str = Depends(get_current_employee)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        # Get employee info
        cursor.execute("""
            SELECT EMPLOYEE_NAME
            FROM EMP_NRM_EMPLOYEES
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        emp = cursor.fetchone()
        
        # Get profile picture
        cursor.execute("""
            SELECT PROFILE_PIC
            FROM EMP_NRM_PERSONAL
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        profile = cursor.fetchone()
        
        # Check today's festival
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("""
            SELECT FESTIVAL_NAME
            FROM EMP_NRM_FESTIVALS
            WHERE FESTIVAL_DATE = %s
        """, (today,))
        festival = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return {
            "employee_id": employee_id,
            "employee_name": emp["EMPLOYEE_NAME"] if emp else "Employee",
            "profile_pic": profile["PROFILE_PIC"] if profile and profile["PROFILE_PIC"] else None,
            "festival_today": festival["FESTIVAL_NAME"] if festival else None
        }
        
    except Exception as e:
        print(f"Resources error: {e}")
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        if conn:
            conn.close()

# Get Personal Details
@app.get("/personal-details")
def get_personal_details(employee_id: str = Depends(get_current_employee)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        cursor.execute("""
            SELECT FIRST_NAME, LAST_NAME, EMAIL, PHONE, ADDRESS, DOB
            FROM EMP_NRM_PERSONAL
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        
        personal_data = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return {
            "personal_data": personal_data
        }
        
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        if conn:
            conn.close()

# Update Personal Details
@app.post("/personal-details")
def update_personal_details(
    data: PersonalDetailsUpdate,
    employee_id: str = Depends(get_current_employee)
):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    
    try:
        cursor = conn.cursor()
        
        # Check if record exists
        cursor.execute("""
            SELECT EMPLOYEE_ID FROM EMP_NRM_PERSONAL WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        
        if cursor.fetchone():
            # Update
            cursor.execute("""
                UPDATE EMP_NRM_PERSONAL
                SET FIRST_NAME = %s, LAST_NAME = %s, EMAIL = %s, 
                    PHONE = %s, ADDRESS = %s, DOB = %s
                WHERE EMPLOYEE_ID = %s
            """, (data.first_name, data.last_name, data.email, 
                  data.phone, data.address, data.dob, employee_id))
        else:
            # Insert
            cursor.execute("""
                INSERT INTO EMP_NRM_PERSONAL 
                (EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, ADDRESS, DOB)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (employee_id, data.first_name, data.last_name, 
                  data.email, data.phone, data.address, data.dob))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"message": "Personal details updated successfully"}
        
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        if conn:
            conn.close()

# Get Salary Info
@app.get("/salary")
def get_salary_info(employee_id: str = Depends(get_current_employee)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        # Get employee basic info
        cursor.execute("""
            SELECT EMPLOYEE_NAME, DEPARTMENT
            FROM EMP_NRM_EMPLOYEES
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        emp = cursor.fetchone()
        
        # Get salary details
        cursor.execute("""
            SELECT BASIC, HRA, ALLOWANCES, DEDUCTIONS, NET_SALARY
            FROM EMP_NRM_SALARY
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        salary = cursor.fetchone()
        
        # Get salary slips
        cursor.execute("""
            SELECT SLIP_ID, MONTH, YEAR, FILE_PATH, GENERATED_AT
            FROM EMP_NRM_SALARY_SLIPS
            WHERE EMP_ID = %s
            ORDER BY YEAR DESC, MONTH DESC
        """, (employee_id,))
        slips = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        salary_info = None
        if salary:
            salary_info = {
                "name": emp["EMPLOYEE_NAME"] if emp else "N/A",
                "department": emp["DEPARTMENT"] if emp else "N/A",
                "basic": float(salary["BASIC"]) if salary["BASIC"] else 0,
                "hra": float(salary["HRA"]) if salary["HRA"] else 0,
                "allowances": float(salary["ALLOWANCES"]) if salary["ALLOWANCES"] else 0,
                "deductions": float(salary["DEDUCTIONS"]) if salary["DEDUCTIONS"] else 0,
                "net_salary": float(salary["NET_SALARY"]) if salary["NET_SALARY"] else 0
            }
        
        return {
            "salary_info": salary_info,
            "salary_slips": slips
        }
        
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        if conn:
            conn.close()

# Get Leave Data
@app.get("/leave")
def get_leave_data(
    employee_id: str = Depends(get_current_employee),
    month: int = datetime.now().month,
    year: int = datetime.now().year
):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        # Get leave records
        cursor.execute("""
            SELECT LEAVE_ID, START_DATE, END_DATE, REASON, STATUS, APPLIED_AT
            FROM EMP_NRM_LEAVE
            WHERE EMP_ID = %s
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
        
        # Generate calendar
        cal = calendar.Calendar()
        month_days = cal.monthdayscalendar(year, month)
        
        return {
            "leave_data": leave_data,
            "festivals": festivals,
            "calendar": month_days,
            "month_name": calendar.month_name[month]
        }
        
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        if conn:
            conn.close()

# Apply for Leave
@app.post("/leave/apply")
def apply_leave(
    data: LeaveApplication,
    employee_id: str = Depends(get_current_employee)
):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO EMP_NRM_LEAVE 
            (EMP_ID, START_DATE, END_DATE, REASON, STATUS, APPLIED_AT)
            VALUES (%s, %s, %s, %s, 'Pending', CURRENT_TIMESTAMP())
        """, (employee_id, data.start_date, data.end_date, data.reason))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"message": "Leave application submitted successfully"}
        
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        if conn:
            conn.close()

# Get ID Card Data
@app.get("/id-card")
def get_id_card(employee_id: str = Depends(get_current_employee)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        # Get ID card info
        cursor.execute("""
            SELECT ID_NUMBER, ISSUE_DATE, EXPIRY_DATE
            FROM EMP_NRM_IDCARD
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        id_card = cursor.fetchone()
        
        # Get personal data
        cursor.execute("""
            SELECT FIRST_NAME, LAST_NAME, EMAIL, PHONE, PROFILE_PIC
            FROM EMP_NRM_PERSONAL
            WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        personal = cursor.fetchone()
        
        # Get employee data
        cursor.execute("""
            SELECT e.EMPLOYEE_NAME, d.DEPT_NAME, des.TITLE as DESIGNATION
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_DEPARTMENTS d ON e.DEPARTMENT = d.DEPT_ID
            LEFT JOIN EMP_NRM_JOB_WORK jw ON e.EMP_ID = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS des ON jw.DESIGNATION_ID = des.DESIGNATION_ID
            WHERE e.EMPLOYEE_ID = %s
        """, (employee_id,))
        employee = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return {
            "id_card_data": id_card,
            "personal_data": personal,
            "employee_data": employee
        }
        
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        if conn:
            conn.close()

# Get Queries
@app.get("/queries")
def get_queries(employee_id: str = Depends(get_current_employee)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    
    try:
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        cursor.execute("""
            SELECT QUERY_ID, QUERY_TEXT, STATUS, CREATED_AT
            FROM EMP_NRM_QUERIES
            WHERE EMPLOYEE_ID = %s
            ORDER BY CREATED_AT DESC
        """, (employee_id,))
        
        queries = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return {"queries": queries}
        
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        if conn:
            conn.close()

# Submit Query
@app.post("/queries")
def submit_query(
    data: QuerySubmission,
    employee_id: str = Depends(get_current_employee)
):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO EMP_NRM_QUERIES 
            (EMPLOYEE_ID, QUERY_TEXT, STATUS, CREATED_AT)
            VALUES (%s, %s, 'Pending', CURRENT_TIMESTAMP())
        """, (employee_id, data.query_text))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"message": "Query submitted successfully"}
        
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        if conn:
            conn.close()

# ==========================================
# RUN SERVER
# ==========================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)