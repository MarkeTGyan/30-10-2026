import os
import hmac
import secrets
from datetime import datetime, timedelta, timezone
from functools import wraps

from flask import (
    Blueprint, Flask, abort, flash, jsonify, redirect,
    render_template_string, request, session, url_for
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import inspect, text
from werkzeug.security import generate_password_hash, check_password_hash


db = SQLAlchemy()
auth_bp = Blueprint("auth", __name__)

MAX_LOGIN_ATTEMPTS = 5
LOCK_MINUTES = 15


def utcnow():
    return datetime.now(timezone.utc)


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="user")
    status = db.Column(db.String(20), nullable=False, default="pending")
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow)
    last_login = db.Column(db.DateTime)
    is_hidden = db.Column(db.Boolean, nullable=False, default=False)
    failed_login_attempts = db.Column(db.Integer, nullable=False, default=0)
    locked_until = db.Column(db.DateTime)
    must_change_password = db.Column(db.Boolean, nullable=False, default=False)
    password_changed_at = db.Column(db.DateTime)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
        self.password_changed_at = utcnow()

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class LoginEvent(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True, index=True)
    username = db.Column(db.String(80), nullable=False, index=True)
    event_type = db.Column(db.String(30), nullable=False, index=True)
    event_at = db.Column(db.DateTime, nullable=False, default=utcnow, index=True)
    ip_address = db.Column(db.String(64))
    user_agent = db.Column(db.String(255))
    session_key = db.Column(db.String(100), index=True)


class AuditLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    admin_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    admin_username = db.Column(db.String(80), nullable=False)
    target_username = db.Column(db.String(80))
    action = db.Column(db.String(50), nullable=False)
    details = db.Column(db.String(500))
    created_at = db.Column(db.DateTime, nullable=False, default=utcnow, index=True)


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


def client_ip():
    # Do not trust forwarded headers unless the deployment explicitly configures a trusted proxy.
    return (request.remote_addr or "")[:64]


def record_login_event(username, event_type, user_id=None, session_key=None):
    db.session.add(LoginEvent(
        user_id=user_id,
        username=(username or "unknown")[:80],
        event_type=event_type,
        event_at=utcnow(),
        ip_address=client_ip(),
        user_agent=(request.user_agent.string or "")[:255],
        session_key=session_key
    ))


def write_audit(admin, action, target=None, details=""):
    db.session.add(AuditLog(
        admin_id=admin.id,
        admin_username=admin.username,
        target_username=target.username if target else None,
        action=action[:50],
        details=(details or "")[:500]
    ))


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
*{box-sizing:border-box}body{margin:0;background:#eef2f7;color:#172033;font-family:Arial,sans-serif}
.wrap{max-width:1200px;margin:28px auto;padding:14px}.card{background:white;border:1px solid #dce3ed;border-radius:15px;padding:22px;margin:14px 0;box-shadow:0 5px 20px #0000000b}
h1,h2{color:#12345a;margin-top:0}p{color:#526176;line-height:1.5}
input,select{padding:10px;border:1px solid #cbd5e1;border-radius:8px;margin:5px 0 10px;font-size:14px;max-width:100%}
input[type=text],input[type=password]{width:100%}
button,.btn{border:0;border-radius:8px;padding:9px 12px;background:#1d4ed8;color:white;font-weight:bold;cursor:pointer;text-decoration:none;display:inline-block;margin:2px;font-size:13px}
.green{background:#047857}.red{background:#b91c1c}.gray{background:#475569}.orange{background:#b45309}.purple{background:#6d28d9}
.msg{padding:12px;background:#eff6ff;border-radius:8px;margin:10px 0}.tablewrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;min-width:820px}th,td{text-align:left;padding:10px;border-bottom:1px solid #e2e8f0;vertical-align:top}
th{background:#f8fafc}.small{font-size:12px;color:#64748b}.actions form{display:inline-block;margin:1px}
.pill{padding:3px 7px;border-radius:20px;background:#e2e8f0;font-size:12px}.muted{color:#64748b}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px}
</style>
"""

LOGIN_PAGE = STYLE + """
<div class="wrap"><div class="card" style="max-width:470px;margin:8vh auto">
<h1>MarkeTGyan</h1><p>Stock Analysis Agent</p>
{% for m in get_flashed_messages() %}<div class="msg">{{m}}</div>{% endfor %}
<form method="post">
<input type="hidden" name="csrf_token" value="{{token}}">
<label>Username</label><input name="username" required autocomplete="username" maxlength="80">
<label>Password</label><input name="password" type="password" required autocomplete="current-password">
<button type="submit">Secure Login</button>
</form>
<p>नया अकाउंट? <a href="{{url_for('auth.register')}}">Register करें</a></p>
<p class="small">Registration के बाद Admin Approval आवश्यक है। अधिकतम 5 गलत प्रयासों के बाद 15 मिनट का अस्थायी लॉक लगेगा।</p>
</div></div>
"""

REGISTER_PAGE = STYLE + """
<div class="wrap"><div class="card" style="max-width:470px;margin:8vh auto">
<h1>नया अकाउंट बनाएँ</h1>
{% for m in get_flashed_messages() %}<div class="msg">{{m}}</div>{% endfor %}
<form method="post">
<input type="hidden" name="csrf_token" value="{{token}}">
<label>Username (कम-से-कम 4 अक्षर)</label><input name="username" minlength="4" maxlength="80" required autocomplete="username">
<label>Password (कम-से-कम 10 अक्षर)</label><input name="password" type="password" minlength="10" required autocomplete="new-password">
<label>Confirm Password</label><input name="confirm" type="password" minlength="10" required autocomplete="new-password">
<button type="submit">Register</button>
</form><p><a href="{{url_for('auth.login')}}">Login पर वापस जाएँ</a></p>
</div></div>
"""

CHANGE_PASSWORD_PAGE = STYLE + """
<div class="wrap"><div class="card" style="max-width:500px;margin:8vh auto">
<h1>Password बदलें</h1>
{% for m in get_flashed_messages() %}<div class="msg">{{m}}</div>{% endfor %}
<form method="post">
<input type="hidden" name="csrf_token" value="{{token}}">
<label>Current Password</label><input name="current_password" type="password" required autocomplete="current-password">
<label>New Password (कम-से-कम 10 अक्षर)</label><input name="new_password" type="password" minlength="10" required autocomplete="new-password">
<label>Confirm New Password</label><input name="confirm_password" type="password" minlength="10" required autocomplete="new-password">
<button type="submit">Update Password</button>
</form>
<p><a href="{{url_for('auth.logout')}}">Logout</a></p>
</div></div>
"""

ADMIN_PAGE = STYLE + """
<div class="wrap"><div class="card">
<h1>MarkeTGyan Admin Control Panel</h1>
<p>Admin: <b>{{admin.username}}</b> |
<a href="/">Open Stock Analysis</a>
<form method="post" action="{{url_for('auth.logout')}}" style="display:inline">
<input type="hidden" name="csrf_token" value="{{token}}"><button class="gray">Logout</button></form>
</p>
{% for m in get_flashed_messages() %}<div class="msg">{{m}}</div>{% endfor %}
<div class="grid">
<div class="card"><b>{{counts.pending}}</b><p>Pending Approval</p></div>
<div class="card"><b>{{counts.approved}}</b><p>Approved Users</p></div>
<div class="card"><b>{{counts.disabled}}</b><p>Disabled / Rejected</p></div>
<div class="card"><b>{{counts.hidden}}</b><p>Hidden Users</p></div>
</div>
<p>
<a class="btn" href="{{url_for('auth.admin')}}">All Users</a>
<a class="btn gray" href="{{url_for('auth.admin', view='hidden')}}">Hidden Users</a>
<a class="btn purple" href="{{url_for('auth.audit_history')}}">Admin Audit Log</a>
</p>
<h2>{{'Hidden Users' if view == 'hidden' else 'User Management'}}</h2>
<div class="tablewrap"><table>
<tr><th>Username</th><th>Status</th><th>Registered</th><th>Last Login</th><th>Security</th><th>Actions</th></tr>
{% for u in users %}
<tr>
<td><b>{{u.username}}</b><br><span class="small">ID: {{u.id}} | {{u.role}}</span></td>
<td><span class="pill">{{u.status}}</span>{% if u.is_hidden %}<br><span class="small">Hidden</span>{% endif %}</td>
<td>{{u.created_at.strftime('%d-%m-%Y %H:%M') if u.created_at else '-'}}</td>
<td>{{u.last_login.strftime('%d-%m-%Y %H:%M') if u.last_login else '-'}}</td>
<td class="small">Failed attempts: {{u.failed_login_attempts}}<br>Lock: {{u.locked_until.strftime('%d-%m %H:%M') if u.locked_until else 'No'}}</td>
<td class="actions">
{% if u.role != 'admin' %}
<form method="post" action="{{url_for('auth.user_action', user_id=u.id)}}">
<input type="hidden" name="csrf_token" value="{{token}}">
{% if u.status != 'approved' %}<button class="green" name="action" value="approve">Approve</button>{% endif %}
{% if u.status != 'rejected' %}<button class="red" name="action" value="reject">Reject</button>{% endif %}
{% if u.status != 'disabled' %}<button class="gray" name="action" value="disable">Disable</button>{% endif %}
{% if u.status in ['disabled','rejected'] %}<button name="action" value="pending">Set Pending</button>{% endif %}
{% if u.is_hidden %}<button class="purple" name="action" value="unhide">Unhide</button>{% else %}<button class="purple" name="action" value="hide">Hide</button>{% endif %}
<button class="orange" name="action" value="unlock">Unlock</button>
<button class="red" name="action" value="delete" onclick="return confirm('यूजर को स्थायी रूप से हटाएँ?')">Delete</button>
</form>
<form method="post" action="{{url_for('auth.admin_reset_password', user_id=u.id)}}">
<input type="hidden" name="csrf_token" value="{{token}}">
<input name="temporary_password" type="password" minlength="10" placeholder="New temporary password" required style="width:170px">
<button class="orange" onclick="return confirm('इस यूजर का Password Reset करें?')">Reset Password</button>
</form>
<a class="btn" href="{{url_for('auth.login_history', user_id=u.id)}}">Login History</a>
{% else %}Protected Admin Account{% endif %}
</td></tr>
{% else %}<tr><td colspan="6">इस सूची में कोई यूजर नहीं है।</td></tr>{% endfor %}
</table></div>
<p class="small">Hide/Unhide केवल Admin सूची की दृश्यता बदलता है, यूजर की Login अनुमति नहीं। Pending, Rejected और Disabled यूजर Login नहीं कर सकते।</p>
</div></div>
"""

HISTORY_PAGE = STYLE + """
<div class="wrap"><div class="card">
<h1>{{heading}}</h1><p><a href="{{url_for('auth.admin')}}">← Admin Panel</a></p>
<div class="tablewrap"><table><tr><th>Date / Time (UTC)</th><th>Username</th><th>Event</th><th>IP Address</th><th>Device / Browser</th></tr>
{% for e in events %}<tr><td>{{e.event_at.strftime('%d-%m-%Y %H:%M:%S') if e.event_at else '-'}}</td><td>{{e.username}}</td><td>{{e.event_type}}</td><td>{{e.ip_address or '-'}}</td><td>{{e.user_agent or '-'}}</td></tr>
{% else %}<tr><td colspan="5">कोई History उपलब्ध नहीं है।</td></tr>{% endfor %}
</table></div></div></div>
"""

AUDIT_PAGE = STYLE + """
<div class="wrap"><div class="card"><h1>Admin Audit Log</h1><p><a href="{{url_for('auth.admin')}}">← Admin Panel</a></p>
<div class="tablewrap"><table><tr><th>Date / Time (UTC)</th><th>Admin</th><th>Target User</th><th>Action</th><th>Details</th></tr>
{% for e in events %}<tr><td>{{e.created_at.strftime('%d-%m-%Y %H:%M:%S') if e.created_at else '-'}}</td><td>{{e.admin_username}}</td><td>{{e.target_username or '-'}}</td><td>{{e.action}}</td><td>{{e.details or '-'}}</td></tr>
{% else %}<tr><td colspan="5">कोई Audit Record नहीं है।</td></tr>{% endfor %}
</table></div></div></div>
"""


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        check_csrf()
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = User.query.filter_by(username=username).first()
        now = utcnow()

        if not user:
            record_login_event(username, "login_failed_unknown_user")
            db.session.commit()
            flash("Username या Password गलत है।")
        elif user.locked_until and user.locked_until.replace(tzinfo=timezone.utc) > now:
            record_login_event(username, "login_blocked_locked", user.id)
            db.session.commit()
            flash("बहुत अधिक गलत प्रयास हुए हैं। कुछ समय बाद पुनः प्रयास करें।")
        elif not user.check_password(password):
            user.failed_login_attempts = (user.failed_login_attempts or 0) + 1
            if user.failed_login_attempts >= MAX_LOGIN_ATTEMPTS:
                user.locked_until = now + timedelta(minutes=LOCK_MINUTES)
                user.failed_login_attempts = 0
            record_login_event(username, "login_failed", user.id)
            db.session.commit()
            flash("Username या Password गलत है।")
        elif user.status != "approved":
            record_login_event(username, "login_denied_" + user.status, user.id)
            db.session.commit()
            flash("आपका अकाउंट Admin Approval के बिना Login नहीं कर सकता।")
        else:
            user.failed_login_attempts = 0
            user.locked_until = None
            user.last_login = now
            session.clear()
            session["user_id"] = user.id
            session["session_key"] = secrets.token_urlsafe(24)
            session.permanent = True
            record_login_event(username, "login_success", user.id, session["session_key"])
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

        if len(username) < 4 or len(username) > 80:
            flash("Username 4 से 80 अक्षरों के बीच होना चाहिए।")
        elif len(password) < 10:
            flash("Password कम-से-कम 10 अक्षर का होना चाहिए।")
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


@auth_bp.route("/logout", methods=["GET", "POST"])
def logout():
    user = get_user()
    if request.method == "POST":
        check_csrf()
    if user:
        record_login_event(
            user.username, "logout", user.id, session.get("session_key")
        )
        db.session.commit()
    session.clear()
    return redirect(url_for("auth.login"))


@auth_bp.route("/change-password", methods=["GET", "POST"])
def change_password():
    user = get_user()
    if not user:
        return redirect(url_for("auth.login"))
    if request.method == "POST":
        check_csrf()
        current = request.form.get("current_password", "")
        new_password = request.form.get("new_password", "")
        confirm = request.form.get("confirm_password", "")
        if not user.check_password(current):
            flash("Current Password गलत है।")
        elif len(new_password) < 10:
            flash("नया Password कम-से-कम 10 अक्षर का होना चाहिए।")
        elif new_password != confirm:
            flash("दोनों नए Password मेल नहीं खाते।")
        elif current == new_password:
            flash("नया Password पुराने Password से अलग होना चाहिए।")
        else:
            user.set_password(new_password)
            user.must_change_password = False
            write_audit(user, "password_changed_by_user", user)
            db.session.commit()
            flash("Password सफलतापूर्वक बदल दिया गया।")
            return redirect(url_for("auth.admin") if user.role == "admin" else "/")
    return render_template_string(CHANGE_PASSWORD_PAGE, token=csrf_token())


@auth_bp.get("/admin")
@admin_required
def admin():
    view = request.args.get("view", "all")
    query = User.query.filter(User.role != "admin")
    if view == "hidden":
        query = query.filter(User.is_hidden.is_(True))
    else:
        query = query.filter(User.is_hidden.is_(False))
    users = query.order_by(User.created_at.desc()).all()
    counts = {
        "pending": User.query.filter_by(role="user", status="pending").count(),
        "approved": User.query.filter_by(role="user", status="approved").count(),
        "disabled": User.query.filter(User.role == "user", User.status.in_(["disabled", "rejected"])).count(),
        "hidden": User.query.filter_by(role="user", is_hidden=True).count()
    }
    return render_template_string(
        ADMIN_PAGE, users=users, admin=get_user(), token=csrf_token(),
        counts=counts, view=view
    )


@auth_bp.post("/admin/user/<int:user_id>")
@admin_required
def user_action(user_id):
    check_csrf()
    admin_user = get_user()
    user = db.session.get(User, user_id)
    action = request.form.get("action", "")

    if not user or user.role == "admin":
        flash("यह कार्रवाई संभव नहीं है।")
    elif action == "delete":
        target_name = user.username
        db.session.add(AuditLog(
            admin_id=admin_user.id, admin_username=admin_user.username,
            target_username=target_name, action="user_deleted",
            details="User record permanently deleted", created_at=utcnow()
        ))
        db.session.delete(user)
        db.session.commit()
        flash("यूजर रिकॉर्ड हटा दिया गया।")
        return redirect(url_for("auth.admin"))
    elif action in ("approve", "reject", "disable", "pending"):
        user.status = {
            "approve": "approved", "reject": "rejected",
            "disable": "disabled", "pending": "pending"
        }[action]
        write_audit(admin_user, "user_" + action, user, "Status changed to " + user.status)
        db.session.commit()
        flash("यूजर की स्थिति अपडेट हो गई।")
    elif action in ("hide", "unhide"):
        user.is_hidden = action == "hide"
        write_audit(admin_user, "user_" + action, user, "Admin list visibility changed")
        db.session.commit()
        flash("यूजर की सूची दृश्यता अपडेट हो गई।")
    elif action == "unlock":
        user.failed_login_attempts = 0
        user.locked_until = None
        write_audit(admin_user, "user_unlocked", user, "Login lock cleared")
        db.session.commit()
        flash("यूजर का Login Lock हटा दिया गया।")
    else:
        flash("अमान्य कार्रवाई।")

    return redirect(url_for("auth.admin", view=request.args.get("view", "all")))


@auth_bp.post("/admin/user/<int:user_id>/reset-password")
@admin_required
def admin_reset_password(user_id):
    check_csrf()
    admin_user = get_user()
    user = db.session.get(User, user_id)
    new_password = request.form.get("temporary_password", "")
    if not user or user.role == "admin":
        flash("इस यूजर का Password Reset नहीं किया जा सकता।")
    elif len(new_password) < 10:
        flash("Temporary Password कम-से-कम 10 अक्षर का होना चाहिए।")
    else:
        user.set_password(new_password)
        user.must_change_password = True
        user.failed_login_attempts = 0
        user.locked_until = None
        write_audit(admin_user, "admin_password_reset", user, "Temporary password set; change required at next login")
        db.session.commit()
        flash("Password Reset हो गया। यूजर को नया Temporary Password सुरक्षित तरीके से दें।")
    return redirect(url_for("auth.admin"))


@auth_bp.get("/admin/user/<int:user_id>/login-history")
@admin_required
def login_history(user_id):
    user = db.session.get(User, user_id)
    if not user:
        abort(404)
    events = LoginEvent.query.filter_by(user_id=user.id).order_by(LoginEvent.event_at.desc()).limit(300).all()
    return render_template_string(HISTORY_PAGE, heading="Login History: " + user.username, events=events)


@auth_bp.get("/admin/audit")
@admin_required
def audit_history():
    events = AuditLog.query.order_by(AuditLog.created_at.desc()).limit(500).all()
    return render_template_string(AUDIT_PAGE, events=events)


def add_missing_user_columns(app):
    """Small additive migration so existing user databases keep their records."""
    with app.app_context():
        inspector = inspect(db.engine)
        if "user" not in inspector.get_table_names():
            return
        existing = {column["name"] for column in inspector.get_columns("user")}
        additions = {
            "is_hidden": "BOOLEAN NOT NULL DEFAULT FALSE",
            "failed_login_attempts": "INTEGER NOT NULL DEFAULT 0",
            "locked_until": "TIMESTAMP NULL",
            "must_change_password": "BOOLEAN NOT NULL DEFAULT FALSE",
            "password_changed_at": "TIMESTAMP NULL"
        }
        with db.engine.begin() as connection:
            for name, definition in additions.items():
                if name not in existing:
                    connection.execute(text('ALTER TABLE "user" ADD COLUMN ' + name + " " + definition))


def init_auth(app: Flask):
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if database_url.startswith("postgres://"):
        database_url = "postgresql://" + database_url[len("postgres://"):]
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+psycopg://", 1)
    if not database_url:
        database_url = "sqlite:///" + os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "markeTGyan_users.db"
        )

    secret_key = os.environ.get("SECRET_KEY", "").strip()
    if not secret_key and os.environ.get("RENDER"):
        raise RuntimeError("Production deployment requires a stable SECRET_KEY environment variable.")
    app.config["SECRET_KEY"] = secret_key or secrets.token_hex(32)
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
            if user:
                session.clear()
            if request.path.startswith("/api/"):
                return jsonify({"error": "Login आवश्यक है।"}), 401
            return redirect(url_for("auth.login"))

        if user.must_change_password and endpoint != "auth.change_password" and endpoint != "auth.logout":
            return redirect(url_for("auth.change_password"))

        if user.role == "admin" and endpoint == "home":
            return redirect(url_for("auth.admin"))

    @app.get("/health")
    def health():
        return jsonify({"status": "ok"}), 200

    with app.app_context():
        db.create_all()
        add_missing_user_columns(app)

        admin_username = os.environ.get("ADMIN_USERNAME", "").strip()
        admin_password = os.environ.get("ADMIN_PASSWORD", "")
        if admin_username and admin_password:
            existing_admin = User.query.filter_by(role="admin").first()
            if not existing_admin:
                admin = User(username=admin_username, role="admin", status="approved")
                admin.set_password(admin_password)
                db.session.add(admin)
                db.session.commit()
