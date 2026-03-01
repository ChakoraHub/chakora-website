# meeting_proxy.py
import os
import requests
from flask import (
    Flask, request, jsonify, make_response,
    render_template, session, send_from_directory, redirect, url_for
)
from datetime import timedelta

# --------------------------------
# CONFIG
# --------------------------------
MEETING_API_BASE = os.getenv("MEETING_API_BASE", "http://localhost:9000")

app = Flask(__name__)
app.secret_key = "temporary123"    # keep / change as per your main app
app.permanent_session_lifetime = timedelta(days=7)


# --------------------------------
# HELPER: forward Authorization
# --------------------------------
def forward_auth_headers():
    auth_header = request.headers.get("Authorization")
    headers = {"Content-Type": "application/json"}
    if auth_header:
        headers["Authorization"] = auth_header
    return headers


# --------------------------------
# PAGE ROUTES
# --------------------------------
@app.route("/meeting")
def meeting_home():
    """
    Render the meeting booking page.
    Works BOTH when user logged into website or not.
    """
    try:
        username = session.get("username", "Guest")
        user_type = session.get("user_type", "new")  # 'new' or 'existing'

        return render_template(
            "meeting.html",
            username=username,
            user_type=user_type,
        )
    except Exception as e:
        return f"Error loading meeting page: {str(e)}", 500


@app.route("/meeting/admin")
@app.route("/meeting/admin/")
def meeting_admin_page():
    """
    Serve admin.html (same as before).
    """
    try:
        return send_from_directory("templates", "admin.html")
    except Exception as e:
        return f"Error loading admin page: {str(e)}", 500


# --------------------------------
# PROXY APIs (used by meeting.html / admin.html)
# --------------------------------

# GET /meeting/api/slots?date=YYYY-MM-DD
@app.route("/meeting/api/slots", methods=["GET"])
def proxy_get_slots():
    date = request.args.get("date")
    if not date:
        return jsonify({"error": "date parameter required"}), 400

    try:
        resp = requests.get(
            f"{MEETING_API_BASE}/meeting-slots",
            params={"date": date},
            timeout=10,
        )
        return make_response(resp.text, resp.status_code,
                             {"Content-Type": "application/json"})
    except requests.Timeout:
        return jsonify({"error": "Meeting service timeout"}), 504
    except Exception as e:
        return jsonify({"error": f"Meeting service error: {str(e)}"}), 500


# POST /meeting/api/book
@app.route("/meeting/api/book", methods=["POST"])
def proxy_post_book():
    try:
        payload = request.get_json()
        if not payload:
            return jsonify({"error": "Invalid JSON payload"}), 400

        headers = forward_auth_headers()
        resp = requests.post(
            f"{MEETING_API_BASE}/meeting-book",
            json=payload,
            headers=headers,
            timeout=10,
        )
        return make_response(resp.text, resp.status_code,
                             {"Content-Type": "application/json"})
    except requests.Timeout:
        return jsonify({"error": "Meeting service timeout"}), 504
    except Exception as e:
        return jsonify({"error": f"Meeting service error: {str(e)}"}), 500


# GET /meeting/api/mybookings
@app.route("/meeting/api/mybookings", methods=["GET"])
def proxy_my_bookings():
    try:
        headers = forward_auth_headers()
        resp = requests.get(
            f"{MEETING_API_BASE}/meeting-mybookings",
            headers=headers,
            timeout=10,
        )
        return make_response(resp.text, resp.status_code,
                             {"Content-Type": "application/json"})
    except requests.Timeout:
        return jsonify({"error": "Meeting service timeout"}), 504
    except Exception as e:
        return jsonify({"error": f"Meeting service error: {str(e)}"}), 500


# DELETE /meeting/api/cancel?booking_id=...
@app.route("/meeting/api/cancel", methods=["DELETE"])
def proxy_cancel_booking():
    booking_id = request.args.get("booking_id")
    if not booking_id:
        return jsonify({"error": "booking_id required"}), 400

    try:
        headers = forward_auth_headers()
        resp = requests.delete(
            f"{MEETING_API_BASE}/meeting-cancel",
            params={"booking_id": booking_id},
            headers=headers,
            timeout=10,
        )
        return make_response(resp.text, resp.status_code,
                             {"Content-Type": "application/json"})
    except requests.Timeout:
        return jsonify({"error": "Meeting service timeout"}), 504
    except Exception as e:
        return jsonify({"error": f"Meeting service error: {str(e)}"}), 500


# GET /meeting/api/admin/bookings?status=PENDING
@app.route("/meeting/api/admin/bookings", methods=["GET"])
def proxy_admin_bookings():
    status_filter = request.args.get("status")

    try:
        headers = forward_auth_headers()
        params = {}
        if status_filter:
            params["status"] = status_filter

        resp = requests.get(
            f"{MEETING_API_BASE}/admin-bookings",
            headers=headers,
            params=params,
            timeout=10,
        )
        return make_response(resp.text, resp.status_code,
                             {"Content-Type": "application/json"})
    except requests.Timeout:
        return jsonify({"error": "Meeting service timeout"}), 504
    except Exception as e:
        return jsonify({"error": f"Meeting service error: {str(e)}"}), 500


# PUT /meeting/api/admin/approve
@app.route("/meeting/api/admin/approve", methods=["PUT"])
def proxy_admin_approve():
    try:
        payload = request.get_json()
        if not payload:
            return jsonify({"error": "Invalid JSON payload"}), 400

        headers = forward_auth_headers()
        resp = requests.put(
            f"{MEETING_API_BASE}/admin-approve",
            json=payload,
            headers=headers,
            timeout=10,
        )
        return make_response(resp.text, resp.status_code,
                             {"Content-Type": "application/json"})
    except requests.Timeout:
        return jsonify({"error": "Meeting service timeout"}), 504
    except Exception as e:
        return jsonify({"error": f"Meeting service error: {str(e)}"}), 500


# --------------------------------
# LOCAL DEV ENTRY POINT
# --------------------------------
if __name__ == "__main__":
    # Only for local testing of the proxy layer
    app.run(host="0.0.0.0", port=5000, debug=True)
