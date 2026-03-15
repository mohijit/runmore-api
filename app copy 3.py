import os
from datetime import datetime, date, timedelta
from functools import wraps

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, jsonify, request, render_template, redirect, url_for, flash, session
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()

# ---------- Models (global so UI + API can use them) ----------
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    weekly_goal_runs = db.Column(db.Integer, nullable=False, default=3)
    password_hash = db.Column(db.String(255), nullable=True)  # login password (hashed)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

class Run(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    run_date = db.Column(db.Date, nullable=False, index=True)
    distance_km = db.Column(db.Float, nullable=False)
    duration_min = db.Column(db.Integer, nullable=True)
    mood = db.Column(db.String(30), nullable=True)  # good/meh/tough
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

# ---------- Helper functions ----------
def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())  # Monday

def compute_streak(run_dates_set: set[date]) -> int:
    streak = 0
    cur = date.today()
    while cur in run_dates_set:
        streak += 1
        cur -= timedelta(days=1)
    return streak

def days_since_last_run(last_run_date: date | None) -> int | None:
    if not last_run_date:
        return None
    return (date.today() - last_run_date).days

def motivation_message(streak: int, days_since: int | None, weekly_done: int, weekly_goal: int) -> str:
    if days_since is None:
        return "Log your first run today — even 10 minutes counts. Start the streak."
    if streak >= 3:
        return f"{streak}-day streak. Don’t break it — keep it easy if you need."
    if days_since >= 3:
        return "No guilt. Just momentum: shoes on, 8–12 minutes easy, done."
    if weekly_done < weekly_goal:
        remaining = weekly_goal - weekly_done
        return f"You’re {remaining} run(s) away from your weekly goal. Book the next one for tomorrow."
    return "Weekly goal hit — a bonus run locks in the habit."

def next_run_suggestion(days_since: int | None, last_distance: float | None) -> dict:
    if days_since is None:
        return {"type": "starter", "plan": "10 min easy jog + 5 min walk", "when": "today"}
    if days_since >= 4:
        return {"type": "comeback", "plan": "12–20 min easy, conversational pace", "when": "today"}
    base = 3.0 if last_distance is None else max(2.0, min(last_distance * 1.1, last_distance + 1.0))
    return {"type": "easy", "plan": f"{base:.1f} km easy (or 20–30 min)", "when": "tomorrow"}

# ---------- App factory ----------
def create_app():
    app = Flask(__name__)
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")  # for sessions + flash

    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", "sqlite:///app.db")
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["API_KEY"] = os.getenv("API_KEY", "dev-change-me")
    app.config["JSON_SORT_KEYS"] = False

    db.init_app(app)

    with app.app_context():
        db.create_all()

    # ---------- API auth ----------
    def require_key(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            key = request.headers.get("X-API-KEY")
            if not key or key != app.config["API_KEY"]:
                return jsonify({"error": "unauthorized"}), 401
            return f(*args, **kwargs)
        return wrapper

    # ---------- Session auth helpers ----------
    def current_user():
        uid = session.get("user_id")
        if not uid:
            return None
        return User.query.get(uid)

    def login_required(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not current_user():
                return redirect(url_for("login_page"))
            return f(*args, **kwargs)
        return wrapper

    # ---------- Auth UI routes ----------
    @app.get("/login")
    def login_page():
        return render_template("login.html")

    @app.post("/login")
    def login_post():
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""

        if not username:
            flash("Username is required.", "error")
            return redirect(url_for("login_page"))

        user = User.query.filter_by(username=username).first()

        # New user -> create record, force password setup
        if not user:
            user = User(username=username, weekly_goal_runs=3, password_hash=None)
            db.session.add(user)
            db.session.commit()
            session["pending_user_id"] = user.id
            flash("Welcome! Set a password to finish creating your account.", "success")
            return redirect(url_for("set_password_page"))

        # Existing user with no password set (e.g., created before auth existed)
        if not user.password_hash:
            session["pending_user_id"] = user.id
            flash("Set your password to enable login.", "success")
            return redirect(url_for("set_password_page"))

        # Normal login
        if not check_password_hash(user.password_hash, password):
            flash("Incorrect password.", "error")
            return redirect(url_for("login_page"))

        session.pop("pending_user_id", None)
        session["user_id"] = user.id
        return redirect(url_for("user_page", username=user.username))

    @app.get("/set-password")
    def set_password_page():
        pending_id = session.get("pending_user_id")
        if not pending_id:
            flash("No account pending password setup.", "error")
            return redirect(url_for("login_page"))

        user = User.query.get(pending_id)
        if not user:
            session.pop("pending_user_id", None)
            flash("Account not found.", "error")
            return redirect(url_for("login_page"))

        return render_template("set_password.html", username=user.username)

    @app.post("/set-password")
    def set_password_post():
        pending_id = session.get("pending_user_id")
        if not pending_id:
            flash("No account pending password setup.", "error")
            return redirect(url_for("login_page"))

        user = User.query.get(pending_id)
        if not user:
            session.pop("pending_user_id", None)
            flash("Account not found.", "error")
            return redirect(url_for("login_page"))

        p1 = request.form.get("password") or ""
        p2 = request.form.get("password2") or ""

        if len(p1) < 8:
            flash("Password must be at least 8 characters.", "error")
            return redirect(url_for("set_password_page"))
        if p1 != p2:
            flash("Passwords do not match.", "error")
            return redirect(url_for("set_password_page"))

        user.password_hash = generate_password_hash(p1)
        db.session.commit()

        session.pop("pending_user_id", None)
        session["user_id"] = user.id
        flash("Password set ✅ You’re logged in.", "success")
        return redirect(url_for("user_page", username=user.username))

    @app.post("/logout")
    def logout():
        session.pop("user_id", None)
        session.pop("pending_user_id", None)
        flash("Logged out.", "success")
        return redirect(url_for("login_page"))

    # ---------- API routes ----------
    @app.get("/health")
    def health():
        return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})

    @app.post("/users")
    @require_key
    def api_create_user():
        data = request.get_json(silent=True) or {}
        username = (data.get("username") or "").strip()
        weekly_goal_runs = data.get("weekly_goal_runs", 3)

        if not username:
            return jsonify({"error": "username is required"}), 400
        if not isinstance(weekly_goal_runs, int) or weekly_goal_runs < 1 or weekly_goal_runs > 14:
            return jsonify({"error": "weekly_goal_runs must be an int between 1 and 14"}), 400
        if User.query.filter_by(username=username).first():
            return jsonify({"error": "username already exists"}), 409

        u = User(username=username, weekly_goal_runs=weekly_goal_runs)
        db.session.add(u)
        db.session.commit()
        return jsonify({"id": u.id, "username": u.username, "weekly_goal_runs": u.weekly_goal_runs}), 201

    @app.post("/users/<username>/runs")
    @require_key
    def api_log_run(username):
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({"error": "user not found"}), 404

        data = request.get_json(silent=True) or {}
        run_date_s = (data.get("date") or "").strip()
        distance_km = data.get("distance_km")
        duration_min = data.get("duration_min")
        mood = (data.get("mood") or "").strip() or None

        if not run_date_s:
            return jsonify({"error": "date is required (YYYY-MM-DD)"}), 400
        try:
            d = parse_date(run_date_s)
        except ValueError:
            return jsonify({"error": "date must be YYYY-MM-DD"}), 400

        if not isinstance(distance_km, (int, float)) or float(distance_km) <= 0 or float(distance_km) > 200:
            return jsonify({"error": "distance_km must be a number between 0 and 200"}), 400
        if duration_min is not None and (not isinstance(duration_min, int) or duration_min <= 0 or duration_min > 2000):
            return jsonify({"error": "duration_min must be an int > 0"}), 400
        if mood is not None and mood not in {"good", "meh", "tough"}:
            return jsonify({"error": "mood must be one of: good, meh, tough"}), 400

        r = Run(user_id=user.id, run_date=d, distance_km=float(distance_km), duration_min=duration_min, mood=mood)
        db.session.add(r)
        db.session.commit()
        return jsonify({"status": "logged", "run_id": r.id}), 201

    @app.get("/users/<username>/dashboard")
    @require_key
    def api_dashboard(username):
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({"error": "user not found"}), 404

        today = date.today()
        ws = week_start(today)
        we = ws + timedelta(days=7)

        runs = Run.query.filter(Run.user_id == user.id).order_by(Run.run_date.asc()).all()
        run_dates = {r.run_date for r in runs}
        streak = compute_streak(run_dates)

        week_runs = [r for r in runs if ws <= r.run_date < we]
        weekly_done = len(week_runs)
        weekly_km = sum(r.distance_km for r in week_runs)

        last_run = runs[-1] if runs else None
        last_run_date = last_run.run_date if last_run else None
        last_distance = last_run.distance_km if last_run else None
        dslr = days_since_last_run(last_run_date)

        msg = motivation_message(streak, dslr, weekly_done, user.weekly_goal_runs)
        suggestion = next_run_suggestion(dslr, last_distance)

        return jsonify({
            "user": {"username": user.username, "weekly_goal_runs": user.weekly_goal_runs},
            "streak_days": streak,
            "days_since_last_run": dslr,
            "this_week": {
                "week_start": ws.isoformat(),
                "runs": weekly_done,
                "goal": user.weekly_goal_runs,
                "distance_km": round(weekly_km, 2)
            },
            "motivation": msg,
            "next_run": suggestion
        })

    # ---------- Web UI routes ----------
    @app.get("/")
    @login_required
    def home_page():
        users = User.query.order_by(User.created_at.desc()).limit(20).all()
        me = current_user()
        return render_template("home.html", users=users, me=me)

    @app.post("/ui/create-user")
    @login_required
    def ui_create_user():
        username = (request.form.get("username") or "").strip()
        goal_raw = (request.form.get("weekly_goal_runs") or "").strip()

        if not username:
            flash("Username is required.", "error")
            return redirect(url_for("home_page"))

        try:
            goal = int(goal_raw) if goal_raw else 3
        except ValueError:
            flash("Weekly goal must be a number.", "error")
            return redirect(url_for("home_page"))

        if goal < 1 or goal > 14:
            flash("Weekly goal must be between 1 and 14.", "error")
            return redirect(url_for("home_page"))

        if User.query.filter_by(username=username).first():
            flash("That username already exists. Pick another.", "error")
            return redirect(url_for("home_page"))

        u = User(username=username, weekly_goal_runs=goal)
        db.session.add(u)
        db.session.commit()

        return redirect(url_for("user_page", username=username))

    @app.get("/u/<username>")
    @login_required
    def user_page(username):
        user = User.query.filter_by(username=username).first()
        if not user:
            return "User not found", 404

        # Optional: prevent viewing other users' pages
        me = current_user()
        if me and me.username != username:
            return "Forbidden", 403

        today = date.today()
        ws = week_start(today)
        we = ws + timedelta(days=7)

        runs = Run.query.filter(Run.user_id == user.id).order_by(Run.run_date.desc()).limit(20).all()
        all_runs_for_streak = Run.query.filter(Run.user_id == user.id).all()
        run_dates = {r.run_date for r in all_runs_for_streak}

        streak = compute_streak(run_dates)

        week_runs = Run.query.filter(
            Run.user_id == user.id,
            Run.run_date >= ws,
            Run.run_date < we
        ).all()

        weekly_done = len(week_runs)
        weekly_km = sum(r.distance_km for r in week_runs)

        last_run = all_runs_for_streak[-1] if all_runs_for_streak else None
        last_run_date = last_run.run_date if last_run else None
        last_distance = last_run.distance_km if last_run else None
        dslr = days_since_last_run(last_run_date)

        msg = motivation_message(streak, dslr, weekly_done, user.weekly_goal_runs)
        suggestion = next_run_suggestion(dslr, last_distance)

        return render_template(
            "user.html",
            user=user,
            runs=runs,
            streak=streak,
            weekly_done=weekly_done,
            weekly_goal=user.weekly_goal_runs,
            weekly_km=round(weekly_km, 2),
            motivation=msg,
            next_run=suggestion
        )

    @app.post("/ui/log-run/<username>")
    @login_required
    def ui_log_run(username):
        user = User.query.filter_by(username=username).first()
        if not user:
            return "User not found", 404

        # Optional: prevent logging runs for other users
        me = current_user()
        if me and me.username != username:
            return "Forbidden", 403

        run_date_s = (request.form.get("date") or "").strip()
        distance_s = (request.form.get("distance_km") or "").strip()
        duration_s = (request.form.get("duration_min") or "").strip()
        mood = (request.form.get("mood") or "").strip() or None

        try:
            d = parse_date(run_date_s)
        except Exception:
            flash("Date must be YYYY-MM-DD.", "error")
            return redirect(url_for("user_page", username=username))

        try:
            distance_km = float(distance_s)
        except Exception:
            flash("Distance must be a number.", "error")
            return redirect(url_for("user_page", username=username))

        duration_min = None
        if duration_s:
            try:
                duration_min = int(duration_s)
            except Exception:
                flash("Duration must be a whole number (minutes).", "error")
                return redirect(url_for("user_page", username=username))

        if distance_km <= 0 or distance_km > 200:
            flash("Distance must be between 0 and 200 km.", "error")
            return redirect(url_for("user_page", username=username))

        if duration_min is not None and (duration_min <= 0 or duration_min > 2000):
            flash("Duration must be a positive whole number.", "error")
            return redirect(url_for("user_page", username=username))

        if mood is not None and mood not in {"good", "meh", "tough"}:
            flash("Mood must be: good, meh, or tough.", "error")
            return redirect(url_for("user_page", username=username))

        r = Run(
            user_id=user.id,
            run_date=d,
            distance_km=distance_km,
            duration_min=duration_min,
            mood=mood
        )
        db.session.add(r)
        db.session.commit()

        flash("Run logged ✅", "success")
        return redirect(url_for("user_page", username=username))

    return app


app = create_app()

if __name__ == "__main__":
    app.run(debug=True)