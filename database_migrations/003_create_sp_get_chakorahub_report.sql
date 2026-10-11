-- =============================================================================
-- Migration: 003_create_sp_get_chakorahub_report.sql
-- Description: Centralized Oracle PL/SQL Stored Procedure for ChakoraHub Reports
-- Supported Reports:
--   1. STUDENT      (NRM_STUDENTS, NRM_USERS, NRM_BATCH_ALLOCATION, NRM_BATCH_SCHEDULE, NRM_COURSES)
--   2. EMPLOYEE     (EMP_NRM_PERSONAL, EMP_NRM_JOB_WORK, EMP_NRM_SALARY, EMP_NRM_DEPARTMENTS, EMP_NRM_DESIGNATIONS)
--   3. CLIENT       (NRM_BILLING, BILLING_TRANSACTIONS)
--   4. APPLICANT    (NRM_APPLICATIONS, NRM_APPLICATION_STATUSES)
--   5. INTERN       (NRM_INTERNSHIP_APPLICATIONS)
--   6. COURSE       (NRM_COURSES, NRM_VIDEO_SESSIONS, NRM_COURSE_FILES)
--   7. ACTIVE_USERS (NRM_USERS, NRM_LOGINS)
--   8. BATCH        (NRM_BATCH_SCHEDULE, NRM_COURSES, NRM_BATCH_ALLOCATION)
-- =============================================================================

CREATE OR REPLACE PROCEDURE SP_GET_CHAKORAHUB_REPORT (
    p_report_type    IN  VARCHAR2,         -- 'STUDENT', 'EMPLOYEE', 'CLIENT', 'APPLICANT', 'INTERN', 'COURSE', 'ACTIVE_USERS', 'BATCH'
    p_from_date      IN  DATE     DEFAULT NULL,
    p_to_date        IN  DATE     DEFAULT NULL,
    p_status         IN  VARCHAR2 DEFAULT NULL,
    p_search_keyword IN  VARCHAR2 DEFAULT NULL,
    p_out_cursor     OUT SYS_REFCURSOR,
    p_status_code    OUT NUMBER,
    p_status_message OUT VARCHAR2
)
AS
    v_search VARCHAR2(200);
BEGIN
    p_status_code := 200;
    p_status_message := 'SUCCESS';
    v_search := '%' || UPPER(TRIM(p_search_keyword)) || '%';

    CASE UPPER(TRIM(p_report_type))

        -- -------------------------------------------------------------
        -- 1. STUDENT REPORT
        -- -------------------------------------------------------------
        WHEN 'STUDENT' THEN
            OPEN p_out_cursor FOR
                SELECT 
                    s.ID AS STUDENT_ID,
                    s.FIRST_NAME,
                    s.LAST_NAME,
                    u.EMAIL,
                    u.PHONE,
                    s.LOCATION,
                    s.EMPLOYED,
                    NVL(c.COURSE_NAME, 'Not Assigned') AS COURSE_NAME,
                    NVL(bs.BATCH_TYPE, 'N/A') AS BATCH_TYPE,
                    bs.START_DATE AS BATCH_START_DATE,
                    NVL(bs.STATUS, 'UNASSIGNED') AS BATCH_STATUS,
                    u.CREATED_AT AS REGISTERED_AT
                FROM NRM_STUDENTS s
                JOIN NRM_USERS u ON s.USER_ID = u.ID
                LEFT JOIN NRM_BATCH_ALLOCATION ba ON TO_CHAR(s.ID) = ba.REGISTRATION_ID OR TO_CHAR(u.ID) = ba.REGISTRATION_ID
                LEFT JOIN NRM_BATCH_SCHEDULE bs ON ba.BATCH_ID = bs.ID
                LEFT JOIN NRM_COURSES c ON bs.COURSE_ID = c.ID
                WHERE (p_from_date IS NULL OR TRUNC(u.CREATED_AT) >= TRUNC(p_from_date))
                  AND (p_to_date IS NULL OR TRUNC(u.CREATED_AT) <= TRUNC(p_to_date))
                  AND (p_status IS NULL OR UPPER(NVL(bs.STATUS, 'UNASSIGNED')) = UPPER(p_status))
                  AND (p_search_keyword IS NULL OR 
                       UPPER(s.FIRST_NAME || ' ' || s.LAST_NAME || ' ' || u.EMAIL || ' ' || u.PHONE || ' ' || s.LOCATION) LIKE v_search)
                ORDER BY u.CREATED_AT DESC;

        -- -------------------------------------------------------------
        -- 2. EMPLOYEE REPORT
        -- -------------------------------------------------------------
        WHEN 'EMPLOYEE' THEN
            OPEN p_out_cursor FOR
                SELECT 
                    p.EMPLOYEE_ID,
                    p.FIRST_NAME,
                    p.LAST_NAME,
                    p.EMAIL,
                    p.PHONE,
                    NVL(d.DEPT_NAME, 'Unassigned') AS DEPARTMENT,
                    NVL(des.TITLE, 'Unassigned') AS DESIGNATION,
                    des.EMP_LEVEL,
                    NVL(s.BASIC, 0) AS BASIC_SALARY,
                    NVL(s.HRA, 0) AS HRA,
                    NVL(s.ALLOWANCES, 0) AS ALLOWANCES,
                    NVL(s.NET_SALARY, 0) AS NET_SALARY,
                    j.CREATED_AT AS JOINING_DATE
                FROM EMP_NRM_PERSONAL p
                LEFT JOIN EMP_NRM_JOB_WORK j ON p.EMPLOYEE_ID = j.EMPLOYEE_ID
                LEFT JOIN EMP_NRM_SALARY s ON p.EMPLOYEE_ID = s.EMPLOYEE_ID
                LEFT JOIN EMP_NRM_DEPARTMENTS d ON j.DEPT_ID = d.DEPT_ID
                LEFT JOIN EMP_NRM_DESIGNATIONS des ON j.DESIGNATION_ID = des.DESIGNATION_ID
                WHERE (p_from_date IS NULL OR TRUNC(j.CREATED_AT) >= TRUNC(p_from_date))
                  AND (p_to_date IS NULL OR TRUNC(j.CREATED_AT) <= TRUNC(p_to_date))
                  AND (p_search_keyword IS NULL OR 
                       UPPER(p.EMPLOYEE_ID || ' ' || p.FIRST_NAME || ' ' || p.LAST_NAME || ' ' || p.EMAIL || ' ' || d.DEPT_NAME || ' ' || des.TITLE) LIKE v_search)
                ORDER BY p.EMPLOYEE_ID ASC;

        -- -------------------------------------------------------------
        -- 3. CLIENT REPORT
        -- -------------------------------------------------------------
        WHEN 'CLIENT' THEN
            OPEN p_out_cursor FOR
                SELECT 
                    b.CUSTOMER_ID,
                    b.NAME AS CLIENT_NAME,
                    NVL(b.ORG_NAME, 'Individual') AS COMPANY_NAME,
                    b.EMAIL,
                    b.PHONE,
                    b.CUSTOMER_TYPE,
                    bt.TRANSACTION_ID,
                    bt.BILLING_TYPE,
                    bt.BILLING_CATEGORY,
                    bt.AMOUNT,
                    NVL(bt.STATUS, 'PENDING') AS PAYMENT_STATUS,
                    b.CREATED_AT AS ONBOARDED_AT
                FROM NRM_BILLING b
                LEFT JOIN BILLING_TRANSACTIONS bt ON b.CUSTOMER_ID = bt.CUSTOMER_ID
                WHERE (p_from_date IS NULL OR TRUNC(b.CREATED_AT) >= TRUNC(p_from_date))
                  AND (p_to_date IS NULL OR TRUNC(b.CREATED_AT) <= TRUNC(p_to_date))
                  AND (p_status IS NULL OR UPPER(NVL(bt.STATUS, 'PENDING')) = UPPER(p_status))
                  AND (p_search_keyword IS NULL OR 
                       UPPER(b.NAME || ' ' || b.ORG_NAME || ' ' || b.EMAIL || ' ' || b.PHONE) LIKE v_search)
                ORDER BY b.CREATED_AT DESC;

        -- -------------------------------------------------------------
        -- 4. APPLICANTS REPORT
        -- -------------------------------------------------------------
        WHEN 'APPLICANT' THEN
            OPEN p_out_cursor FOR
                SELECT 
                    a.ID AS APPLICANT_ID,
                    a.APPLICATION_ID,
                    a.APPLICANT_NAME,
                    a.EMAIL,
                    a.PHONE,
                    NVL(st.STATUS_NAME, 'Applied') AS APPLICATION_STATUS,
                    NVL(a.SOURCE, 'Direct Website') AS APPLICATION_SOURCE,
                    a.APPLIED_DATE,
                    a.LAST_UPDATED
                FROM NRM_APPLICATIONS a
                LEFT JOIN NRM_APPLICATION_STATUSES st ON a.CURRENT_STATUS_ID = st.ID
                WHERE (p_from_date IS NULL OR TRUNC(a.APPLIED_DATE) >= TRUNC(p_from_date))
                  AND (p_to_date IS NULL OR TRUNC(a.APPLIED_DATE) <= TRUNC(p_to_date))
                  AND (p_status IS NULL OR UPPER(NVL(st.STATUS_NAME, '')) = UPPER(p_status))
                  AND (p_search_keyword IS NULL OR 
                       UPPER(a.APPLICATION_ID || ' ' || a.APPLICANT_NAME || ' ' || a.EMAIL || ' ' || a.PHONE || ' ' || st.STATUS_NAME) LIKE v_search)
                ORDER BY a.APPLIED_DATE DESC;

        -- -------------------------------------------------------------
        -- 5. INTERN REPORT
        -- -------------------------------------------------------------
        WHEN 'INTERN' THEN
            OPEN p_out_cursor FOR
                SELECT 
                    i.INTERN_ID,
                    i.FULL_NAME AS INTERN_NAME,
                    i.EMAIL,
                    i.MOBILE,
                    i.COLLEGE_NAME,
                    i.BRANCH,
                    NVL(i.INTERNSHIP_DOMAIN, 'General') AS DOMAIN,
                    NVL(i.INTERNSHIP_DURATION, '3 Months') AS DURATION,
                    NVL(i.INTERN_MODE, 'Online') AS INTERN_MODE,
                    i.START_DATE,
                    NVL(i.STATUS, 'PENDING') AS STATUS,
                    i.SUBMITTED_AT
                FROM NRM_INTERNSHIP_APPLICATIONS i
                WHERE (p_from_date IS NULL OR TRUNC(i.SUBMITTED_AT) >= TRUNC(p_from_date))
                  AND (p_to_date IS NULL OR TRUNC(i.SUBMITTED_AT) <= TRUNC(p_to_date))
                  AND (p_status IS NULL OR UPPER(NVL(i.STATUS, 'PENDING')) = UPPER(p_status))
                  AND (p_search_keyword IS NULL OR 
                       UPPER(i.INTERN_ID || ' ' || i.FULL_NAME || ' ' || i.EMAIL || ' ' || i.MOBILE || ' ' || i.COLLEGE_NAME || ' ' || i.INTERNSHIP_DOMAIN) LIKE v_search)
                ORDER BY i.SUBMITTED_AT DESC;

        -- -------------------------------------------------------------
        -- 6. COURSES REPORT
        -- -------------------------------------------------------------
        WHEN 'COURSE' THEN
            OPEN p_out_cursor FOR
                SELECT 
                    c.ID AS COURSE_ID,
                    c.COURSE_NAME,
                    c.COURSE_CODE,
                    (SELECT COUNT(*) FROM NRM_VIDEO_SESSIONS v WHERE v.COURSE_ID = c.ID) AS TOTAL_VIDEOS,
                    (SELECT COUNT(*) FROM NRM_COURSE_FILES f WHERE f.COURSE_ID = c.ID AND UPPER(f.FILE_TYPE) = 'PPT' AND f.IS_ACTIVE = 1) AS TOTAL_PPTS,
                    (SELECT COUNT(*) FROM NRM_COURSE_FILES f WHERE f.COURSE_ID = c.ID AND UPPER(f.FILE_TYPE) = 'CODE' AND f.IS_ACTIVE = 1) AS TOTAL_CODE_FILES,
                    (SELECT COUNT(*) FROM NRM_COURSE_FILES f WHERE f.COURSE_ID = c.ID AND UPPER(f.FILE_TYPE) = 'INTERVIEW_QUESTIONS' AND f.IS_ACTIVE = 1) AS TOTAL_IQ_FILES,
                    (SELECT COUNT(*) FROM NRM_BATCH_SCHEDULE bs WHERE bs.COURSE_ID = c.ID) AS TOTAL_BATCHES
                FROM NRM_COURSES c
                WHERE (p_search_keyword IS NULL OR 
                       UPPER(c.COURSE_NAME || ' ' || c.COURSE_CODE) LIKE v_search)
                ORDER BY c.ID ASC;

        -- -------------------------------------------------------------
        -- 7. ACTIVE USERS REPORT
        -- -------------------------------------------------------------
        WHEN 'ACTIVE_USERS' THEN
            OPEN p_out_cursor FOR
                SELECT 
                    u.ID AS USER_ID,
                    u.USERNAME,
                    u.EMAIL,
                    u.PHONE,
                    NVL(u.USERTYPE, 'STUDENT') AS USER_ROLE,
                    NVL(l.IS_ACTIVE, 'Y') AS ACCOUNT_ACTIVE,
                    l.LAST_LOGIN,
                    l.LOGOUT_TIME,
                    NVL(l.FAILED_LOGIN_ATTEMPTS, 0) AS FAILED_ATTEMPTS,
                    NVL(l.ACCOUNT_LOCKED, 'N') AS IS_LOCKED,
                    u.CREATED_AT AS SIGNUP_DATE
                FROM NRM_USERS u
                LEFT JOIN NRM_LOGINS l ON u.ID = l.USER_ID
                WHERE (p_from_date IS NULL OR TRUNC(l.LAST_LOGIN) >= TRUNC(p_from_date))
                  AND (p_to_date IS NULL OR TRUNC(l.LAST_LOGIN) <= TRUNC(p_to_date))
                  AND (p_status IS NULL OR UPPER(NVL(u.USERTYPE, 'STUDENT')) = UPPER(p_status))
                  AND (p_search_keyword IS NULL OR 
                       UPPER(u.USERNAME || ' ' || u.EMAIL || ' ' || u.PHONE || ' ' || u.USERTYPE) LIKE v_search)
                ORDER BY l.LAST_LOGIN DESC NULLS LAST;

        -- -------------------------------------------------------------
        -- 8. BATCHES REPORT
        -- -------------------------------------------------------------
        WHEN 'BATCH' THEN
            OPEN p_out_cursor FOR
                SELECT 
                    b.ID AS BATCH_ID,
                    c.COURSE_NAME,
                    NVL(b.BATCH_TYPE, 'Regular') AS BATCH_TYPE,
                    NVL(b.LANGUAGE, 'English') AS LANGUAGE,
                    b.START_DATE,
                    b.END_DATE,
                    NVL(b.STATUS, 'ACTIVE') AS STATUS,
                    COUNT(ba.ID) AS TOTAL_ENROLLED,
                    b.CREATED_AT
                FROM NRM_BATCH_SCHEDULE b
                LEFT JOIN NRM_COURSES c ON b.COURSE_ID = c.ID
                LEFT JOIN NRM_BATCH_ALLOCATION ba ON b.ID = ba.BATCH_ID
                WHERE (p_from_date IS NULL OR TRUNC(b.START_DATE) >= TRUNC(p_from_date))
                  AND (p_to_date IS NULL OR TRUNC(b.START_DATE) <= TRUNC(p_to_date))
                  AND (p_status IS NULL OR UPPER(NVL(b.STATUS, 'ACTIVE')) = UPPER(p_status))
                  AND (p_search_keyword IS NULL OR 
                       UPPER(c.COURSE_NAME || ' ' || b.BATCH_TYPE || ' ' || b.LANGUAGE || ' ' || b.STATUS) LIKE v_search)
                GROUP BY b.ID, c.COURSE_NAME, b.BATCH_TYPE, b.LANGUAGE, b.START_DATE, b.END_DATE, b.STATUS, b.CREATED_AT
                ORDER BY b.START_DATE DESC;

        ELSE
            p_status_code := 400;
            p_status_message := 'Invalid report type requested: ' || p_report_type || '. Allowed: STUDENT, EMPLOYEE, CLIENT, APPLICANT, INTERN, COURSE, ACTIVE_USERS, BATCH';
            OPEN p_out_cursor FOR SELECT NULL AS RESULT FROM DUAL WHERE 1=0;
    END CASE;

EXCEPTION
    WHEN OTHERS THEN
        p_status_code := 500;
        p_status_message := 'PLSQL Exception: ' || SQLERRM;
        OPEN p_out_cursor FOR SELECT NULL AS RESULT FROM DUAL WHERE 1=0;
END SP_GET_CHAKORAHUB_REPORT;
/
