
import os
import hmac
import secrets
from datetime import datetime, timedelta, timezone
from functools import wraps
from urllib.parse import urlparse, urljoin

from flask import (
    Blueprint, Flask, abort, flash, jsonify, redirect,
    render_template_string, request, session, url_for
)
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash


db = SQLAlchemy()
auth_bp = Blueprint("auth", __name__)


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="user")
    status = db.Column(db.String(20), nullable=False, default="pending")
    created_at = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    last_login = db.Column(db.DateTime)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


def get_user():
    user_id = session.get("user_id")
    return db.session.get(User, user_id) if user_id else None


def csrf_token():
    if "_csrf_token" not in session:
        session["_csrf_token"] = secrets.token_urlsafe(32)
    return session["_csrf_token"]


def check_csrf():
    sent = request.form.get("csrf_token", "")
    saved = session.get("_csrf_token", "")
    if not sent or not saved or not hmac.compare_digest(sent, saved):
        abort(400, "Security token invalid. Please refresh and try again.")


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user = get_user()
        if not user or user.role != "admin" or user.status != "approved":
            return redirect(url_for("auth.login"))
        return fn(*args, **kwargs)
    return wrapper


STYLE = """
<style>
*{box-sizing:border-box}
body{margin:0;background:#eef2f7;color:#172033;font-family:Arial,sans-serif}
.wrap{max-width:1050px;margin:35px auto;padding:16px}
.card{background:white;border:1px solid #dce3ed;border-radius:15px;padding:25px;margin:15px 0;box-shadow:0 5px 20px #0000000b}
h1{color:#12345a;margin-top:0}
p{color:#526176;line-height:1.5}
input{width:100%;padding:12px;border:1px solid #cbd5e1;border-radius:8px;margin:7px 0 15px;font-size:16px}
button,.btn{border:0;border-radius:8px;padding:10px 14px;background:#1d4ed8;color:white;font-weight:bold;cursor:pointer;text-decoration:none;display:inline-block;margin:3px}
.green{background:#047857}.red{background:#b91c1c}.gray{background:#475569}
.msg{padding:12px;background:#eff6ff;border-radius:8px;margin:10px 0}
.tablewrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;min-width:650px}
th,td{text-align:left;padding:11px;border-bottom:1px solid #e2e8f0}
th{background:#f8fafc}
.small{font-size:13px;color:#64748b}
</style>
"""

LOGIN_PAGE = STYLE + """
<div class="wrap"><div class="card" style="max-width:470px;margin:8vh auto">
<h1>MarkeTGyan</h1><p>Stock Analysis Agent</p>
{% for m in get_flashed_messages() %}<div class="msg">{{m}}</div>{% endfor %}
<form method="post">
<input type="hidden" name="csrf_token" value="{{token}}">
<label>Username</label><input name="username" required autocomplete="username">
<label>Password</label><input name="password" type="password" required autocomplete="current-password">
<button type="submit">Login</button>
</form>
<p>नया अकाउंट? <a href="{{url_for('auth.register')}}">Register करें</a></p>
<p class="small">Registration के बाद Admin Approval आवश्यक है।</p>
</div></div>
"""

REGISTER_PAGE = STYLE + """
<div class="wrap"><div class="card" style="max-width:470px;margin:8vh auto">
<h1>नया अकाउंट बनाएँ</h1>
{% for m in get_flashed_messages() %}<div class="msg">{{m}}</div>{% endfor %}
<form method="post">
<input type="hidden" name="csrf_token" value="{{token}}">
<label>Username (कम-से-कम 4 अक्षर)</label>
<input name="username" minlength="4" maxlength="80" required>
<label>Password (कम-से-कम 8 अक्षर)</label>
<input name="password" type="password" minlength="8" required>
<label>Confirm Password</label>
<input name="confirm" type="password" minlength="8" required>
<button type="submit">Register</button>
</form>
<p><a href="{{url_for('auth.login')}}">Login पर वापस जाएँ</a></p>
</div></div>
"""

ADMIN_PAGE = STYLE + """
<div class="wrap"><div class="card">
<h1>MarkeTGyan Admin Panel</h1>
<p>Admin: <b>{{admin.username}}</b> |
<a href="{{url_for('auth.logout')}}">Logout</a> |
<a href="/">Open Stock Analysis</a></p>
{% for m in get_flashed_messages() %}<div class="msg">{{m}}</div>{% endfor %}
<div class="tablewrap"><table>
<tr><th>Username</th><th>Status</th><th>Registered</th><th>Last Login</th><th>Actions</th></tr>
{% for u in users %}
<tr>
<td>{{u.username}}</td><td>{{u.status}}</td>
<td>{{u.created_at.strftime('%d-%m-%Y %H:%M') if u.created_at else '-'}}</td>
<td>{{u.last_login.strftime('%d-%m-%Y %H:%M') if u.last_login else '-'}}</td>
<td>
{% if u.role != 'admin' %}
<form method="post" action="{{url_for('auth.user_action', user_id=u.id)}}">
<input type="hidden" name="csrf_token" value="{{token}}">
{% if u.status != 'approved' %}<button class="green" name="action" value="approve">Approve</button>{% endif %}
{% if u.status != 'rejected' %}<button class="red" name="action" value="reject">Reject</button>{% endif %}
{% if u.status != 'disabled' %}<button class="gray" name="action" value="disable">Disable</button>{% endif %}
{% if u.status in ['disabled','rejected'] %}<button name="action" value="pending">Set Pending</button>{% endif %}
<button class="red" name="action" value="delete" onclick="return confirm('यूजर को हटाएँ?')">Delete</button>
</form>
{% else %}Admin{% endif %}
</td></tr>
{% else %}
<tr><td colspan="5">अभी कोई यूजर नहीं है।</td></tr>
{% endfor %}
</table></div>
<p class="small">Pending और Rejected यूजर मुख्य एप्लिकेशन में प्रवेश नहीं कर सकते।</p>
</div></div>
"""


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        check_csrf()
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = User.query.filter_by(username=username).first()

        if not user or not user.check_password(password):
            flash("Username या Password गलत है।")
        elif user.status != "approved":
            flash("आपका अकाउंट अभी Admin Approval की प्रतीक्षा में है।")
        else:
            session.clear()
            session["user_id"] = user.id
            user.last_login = datetime.now(timezone.utc)
            db.session.commit()
            return redirect(url_for("auth.admin") if user.role == "admin" else "/")

    return render_template_string(LOGIN_PAGE, token=csrf_token())


@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        check_csrf()
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm", "")

        if len(username) < 4:
            flash("Username कम-से-कम 4 अक्षर का होना चाहिए।")
        elif len(password) < 8:
            flash("Password कम-से-कम 8 अक्षर का होना चाहिए।")
        elif password != confirm:
            flash("दोनों Password मेल नहीं खाते।")
        elif User.query.filter_by(username=username).first():
            flash("यह Username पहले से मौजूद है।")
        else:
            user = User(username=username, status="pending", role="user")
            user.set_password(password)
            db.session.add(user)
            db.session.commit()
            flash("Registration सफल। Admin Approval के बाद Login कर सकेंगे।")
            return redirect(url_for("auth.login"))

    return render_template_string(REGISTER_PAGE, token=csrf_token())


@auth_bp.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))


@auth_bp.get("/admin")
@admin_required
def admin():
    users = User.query.order_by(User.created_at.desc()).all()
    return render_template_string(
        ADMIN_PAGE, users=users, admin=get_user(), token=csrf_token()
    )


@auth_bp.post("/admin/user/<int:user_id>")
@admin_required
def user_action(user_id):
    check_csrf()
    user = db.session.get(User, user_id)
    action = request.form.get("action", "")

    if not user or user.role == "admin":
        flash("यह कार्रवाई संभव नहीं है।")
    elif action == "delete":
        db.session.delete(user)
        db.session.commit()
        flash("यूजर रिकॉर्ड हटा दिया गया।")
    elif action in ("approve", "reject", "disable", "pending"):
        user.status = {
            "approve": "approved",
            "reject": "rejected",
            "disable": "disabled",
            "pending": "pending"
        }[action]
        db.session.commit()
        flash("यूजर की स्थिति अपडेट हो गई।")
    else:
        flash("अमान्य कार्रवाई।")

    return redirect(url_for("auth.admin"))


def init_auth(app: Flask):
    database_url = os.environ.get("DATABASE_URL", "").strip()

    if database_url.startswith("postgres://"):
        database_url = "postgresql://" + database_url[len("postgres://"):]

    if database_url.startswith("postgresql://"):
        database_url = database_url.replace(
            "postgresql://", "postgresql+psycopg://", 1
        )

    if not database_url:
        database_url = "sqlite:///" + os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "markeTGyan_users.db"
        )

    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY") or secrets.token_hex(32)
    app.config["SQLALCHEMY_DATABASE_URI"] = database_url
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = os.environ.get("COOKIE_SECURE", "true").lower() == "true"
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=12)

    db.init_app(app)
    app.register_blueprint(auth_bp)

    @app.before_request
    def login_gate():
        endpoint = request.endpoint or ""

        if endpoint in ("auth.login", "auth.register", "health") or endpoint.startswith("static"):
            return None

        user = get_user()

        if not user or user.status != "approved":
            if request.path.startswith("/api/"):
                return jsonify({"error": "Login आवश्यक है।"}), 401
            return redirect(url_for("auth.login"))

        if user.role == "admin" and endpoint == "home":
            return redirect(url_for("auth.admin"))

    @app.get("/health")
    def health():
        return jsonify({"status": "ok"}), 200

    with app.app_context():
        db.create_all()

        admin_username = os.environ.get("ADMIN_USERNAME", "").strip()
        admin_password = os.environ.get("ADMIN_PASSWORD", "")

        if admin_username and admin_password:
            existing_admin = User.query.filter_by(role="admin").first()
            if not existing_admin:
                admin = User(
                    username=admin_username,
                    role="admin",
                    status="approved"
                )
                admin.set_password(admin_password)
                db.session.add(admin)
                db.session.commit()
