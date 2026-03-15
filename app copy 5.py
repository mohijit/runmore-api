import os
from datetime import datetime, date, timedelta
from functools import wraps

from dotenv import load_dotenv
load_dotenv()

from flask import (
    Flask, jsonify, request, render_template,
    redirect, url_for, flash, session
)
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()


# =========================
# Models
# =========================
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    weekly_goal_runs = db.Column(db.Integer, nullable=False, default=3)
    password_hash = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class Run(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    run_date = db.Column(db.Date, nullable=False, index=True)
    distance_km = db.Column(db.Float, nullable=False)
    duration_min = db.Column(db.Integer, nullable=True)  # optional
    mood = db.Column(db.String(30), nullable=True)       # good/meh/tough
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class FreezeDay(db.Model):
    """
    A 'streak freeze' counts as if the user ran on that date, but they only get
    1 freeze per calendar month.
    """
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    frozen_date = db.Column(db.Date, nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("user_id", "frozen_date", name="uq_freeze_user_date"),
    )


# =========================
# Pure helpers
# =========================
def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())  # Monday


def month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def compute_streak(run_dates_set: set[date]) -> int:
    """Counts consecutive days ending today, using run_dates_set as 'did_run' dates."""
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


def pace_min_per_km(distance_km: float, duration_min: int) -> float | None:
    if not distance_km or not duration_min:
        return None
    if distance_km <= 0 or duration_min <= 0:
        return None
    return duration_min / distance_km


def format_pace(p: float | None) -> str:
    if p is None:
        return "—"
    mins = int(p)
    secs = int(round((p - mins) * 60))
    if secs == 60:
        mins += 1
        secs = 0
    return f"{mins}:{secs:02d} /km"


def intensity_level(distance_km: float) -> int:
    """
    For calendar dots (GitHub style).
    0 = no run
    1 = light
    2 = medium
    3 = heavy
    """
    if distance_km <= 0:
        return 0
    if distance_km <= 3:
        return 1
    if distance_km <= 7:
        return 2
    return 3


def build_nudge(days_since: int | None, streak: int, weekly_done: int, weekly_goal: int) -> str:
    # Smart nudges tuned to your requests
    if days_since is None:
        return "Start small: 8–12 minutes easy today. Your streak begins with one run."
    if days_since >= 3:
        return "You haven’t run in 3+ days — do 10 minutes easy today. Just restart the motion."
    if streak == 0 and days_since == 1:
        return "Yesterday was a miss — a short run today keeps the habit alive."
    if weekly_done < weekly_goal:
        remaining = weekly_goal - weekly_done
        return f"You’re {remaining} run(s) away from your weekly goal. Schedule one for tomorrow."
    return "Weekly goal hit — a bonus easy run locks in the habit."


def best_week_stats(runs: list[Run]) -> dict:
    """
    Best week by distance and by run count.
    Weeks start Monday.
    """
    if not runs:
        return {"week_start": None, "runs": 0, "distance_km": 0.0}

    # Group by week_start
    buckets: dict[date, list[Run]] = {}
    for r in runs:
        ws = week_start(r.run_date)
        buckets.setdefault(ws, []).append(r)

    best_ws = None
    best_score = (-1, -1.0)  # (runs, distance)
    best_runs = 0
    best_km = 0.0

    for ws, rs in buckets.items():
        cnt = len(rs)
        km = sum(x.distance_km for x in rs)
        score = (cnt, km)
        if score > best_score:
            best_score = score
            best_ws = ws
            best_runs = cnt
            best_km = km

    return {"week_start": best_ws, "runs": best_runs, "distance_km": round(best_km, 2)}


def freeze_available_this_month(user_id: int) -> bool:
    mk = month_key(date.today())
    existing = FreezeDay.query.filter(
        FreezeDay.user_id == user_id
    ).all()
    return not any(month_key(f.frozen_date) == mk for f in existing)


def frozen_dates_set(user_id: int) -> set[date]:
    frz = FreezeDay.query.filter(FreezeDay.user_id == user_id).all()
    return {f.frozen_date for f in frz}


def build_calendar_30(user_runs: list[Run], frozen: set[date]) -> dict:
    """
    Returns GitHub-like calendar data for last 30 days (including today),
    arranged in weeks (columns) x weekdays (rows).
    """
    today = date.today()
    start = today - timedelta(days=29)

    # map date -> max intensity for that day
    day_level: dict[date, int] = {}
    for r in user_runs:
        if start <= r.run_date <= today:
            day_level[r.run_date] = max(day_level.get(r.run_date, 0), intensity_level(r.distance_km))

    # Frozen days count as "ran" but use level 1 (light) visually
    for d in frozen:
        if start <= d <= today:
            day_level[d] = max(day_level.get(d, 0), 1)

    # Build list of days
    days = []
    cur = start
    while cur <= today:
        days.append({
            "date": cur.isoformat(),
            "dow": cur.weekday(),      # 0=Mon
            "level": day_level.get(cur, 0),
            "is_frozen": (cur in frozen) and (cur not in {r.run_date for r in user_runs}),
        })
        cur += timedelta(days=1)

    # Align into columns like GitHub: columns are weeks.
    # We'll create columns keyed by week_start date.
    cols: dict[date, dict[int, dict]] = {}
    for item in days:
        d = parse_date(item["date"])
        ws = week_start(d)
        cols.setdefault(ws, {})
        cols[ws][d.weekday()] = item  # place by weekday row

    # Ensure each week has rows 0..6 with placeholders if missing
    col_list = []
    for ws in sorted(cols.keys()):
        rows = []
        for dow in range(7):
            rows.append(cols[ws].get(dow, {"date": "", "dow": dow, "level": -1, "is_frozen": False}))
        col_list.append({"week_start": ws.isoformat(), "rows": rows})

    return {
        "start": start.isoformat(),
        "end": today.isoformat(),
        "columns": col_list,
    }


def build_dashboard_for_user(user: User) -> dict:
    # Pull data once
    runs_all = Run.query.filter(Run.user_id == user.id).order_by(Run.run_date.asc()).all()

    # Streak calculation uses both runs + freeze days
    frozen = frozen_dates_set(user.id)
    run_dates = {r.run_date for r in runs_all} | frozen
    streak = compute_streak(run_dates)

    # Weekly stats (this week)
    today = date.today()
    ws = week_start(today)
    we = ws + timedelta(days=7)

    week_runs = [r for r in runs_all if ws <= r.run_date < we]
    weekly_done = len(week_runs)
    weekly_km = round(sum(r.distance_km for r in week_runs), 2)

    last_run = runs_all[-1] if runs_all else None
    last_run_date = last_run.run_date if last_run else None
    dslr = days_since_last_run(last_run_date)

    # Smart nudge
    nudge = build_nudge(dslr, streak, weekly_done, user.weekly_goal_runs)

    # Next run suggestion (simple)
    if dslr is None:
        next_run = {"when": "today", "plan": "10 min easy jog + 5 min walk"}
    elif dslr >= 4:
        next_run = {"when": "today", "plan": "12–20 min easy, conversational pace"}
    else:
        next_run = {"when": "tomorrow", "plan": "20–30 min easy (or 3–5 km)"}

    # Personal bests
    longest = max((r.distance_km for r in runs_all), default=0.0)

    # Fastest pace = min(duration/distance) among runs with duration
    best_pace_val = None
    best_pace_run = None
    for r in runs_all:
        if r.duration_min is not None:
            p = pace_min_per_km(r.distance_km, r.duration_min)
            if p is not None and (best_pace_val is None or p < best_pace_val):
                best_pace_val = p
                best_pace_run = r

    best_week = best_week_stats(runs_all)

    # Calendar data
    cal = build_calendar_30(runs_all, frozen)

    # Freeze status
    freeze_available = freeze_available_this_month(user.id)
    # When freeze is used, it should apply to "yesterday" typically
    yesterday = date.today() - timedelta(days=1)
    can_use_freeze_today = freeze_available and (yesterday not in run_dates) and (date.today() not in run_dates)

    return {
        "user": {"username": user.username, "weekly_goal_runs": user.weekly_goal_runs},
        "streak_days": streak,
        "days_since_last_run": dslr,
        "this_week": {
            "week_start": ws.isoformat(),
            "runs": weekly_done,
            "goal": user.weekly_goal_runs,
            "distance_km": weekly_km,
        },
        "nudge": nudge,
        "next_run": next_run,
        "bests": {
            "longest_km": round(longest, 2),
            "fastest_pace": format_pace(best_pace_val),
            "fastest_pace_date": best_pace_run.run_date.isoformat() if best_pace_run else None,
            "best_week_start": best_week["week_start"].isoformat() if best_week["week_start"] else None,
            "best_week_runs": best_week["runs"],
            "best_week_km": best_week["distance_km"],
        },
        "calendar30": cal,
        "freeze": {
            "available_this_month": freeze_available,
            "can_use_today": can_use_freeze_today,
        }
    }


# =========================
# App factory
# =========================
def create_app():
    app = Flask(__name__)
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")

    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", "sqlite:///app.db")
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["API_KEY"] = os.getenv("API_KEY", "dev-change-me")
    app.config["JSON_SORT_KEYS"] = False

    db.init_app(app)
    with app.app_context():
        db.create_all()

    # -------------------------
    # Auth helpers
    # -------------------------
    def current_user() -> User | None:
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

    def ensure_own_user(username: str):
        me = current_user()
        if not me:
            return redirect(url_for("login_page"))
        if me.username != username:
            return "Forbidden", 403
        return None

    def require_key(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            key = request.headers.get("X-API-KEY")
            if not key or key != app.config["API_KEY"]:
                return jsonify({"error": "unauthorized"}), 401
            return f(*args, **kwargs)
        return wrapper

    # -------------------------
    # Auth pages
    # -------------------------
    @app.get("/login")
    def login_page():
        if current_user():
            return redirect(url_for("home_page"))
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

        # Existing user created before auth existed
        if not user.password_hash:
            session["pending_user_id"] = user.id
            flash("Set your password to enable login.", "success")
            return redirect(url_for("set_password_page"))

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

    # -------------------------
    # API routes (unchanged)
    # -------------------------
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
        return jsonify(build_dashboard_for_user(user))

    # -------------------------
    # UI routes
    # -------------------------
    @app.get("/")
    @login_required
    def home_page():
        me = current_user()
        return render_template("home.html", me=me)

    @app.get("/u/<username>")
    @login_required
    def user_page(username):
        guard = ensure_own_user(username)
        if guard:
            return guard

        user = User.query.filter_by(username=username).first()
        if not user:
            return "User not found", 404

        dash = build_dashboard_for_user(user)

        runs = Run.query.filter(Run.user_id == user.id).order_by(Run.run_date.desc()).limit(20).all()

        # Add computed pace strings for UI convenience
        runs_ui = []
        for r in runs:
            p = pace_min_per_km(r.distance_km, r.duration_min) if r.duration_min else None
            runs_ui.append({
                "id": r.id,
                "run_date": r.run_date,
                "distance_km": r.distance_km,
                "duration_min": r.duration_min,
                "mood": r.mood,
                "pace_str": format_pace(p),
            })

        return render_template(
            "user.html",
            me=current_user(),
            user=user,
            dash=dash,
            runs=runs_ui,
        )

    @app.post("/ui/log-run/<username>")
    @login_required
    def ui_log_run(username):
        guard = ensure_own_user(username)
        if guard:
            return guard

        user = User.query.filter_by(username=username).first()
        if not user:
            return "User not found", 404

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

        # Prevent duplicates for same date? (Optional) We'll allow multiple runs/day.
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

    # ---- Goal editing ----
    @app.post("/ui/update-goal/<username>")
    @login_required
    def ui_update_goal(username):
        guard = ensure_own_user(username)
        if guard:
            return guard

        user = User.query.filter_by(username=username).first()
        if not user:
            return "User not found", 404

        goal_raw = (request.form.get("weekly_goal_runs") or "").strip()
        try:
            goal = int(goal_raw)
        except Exception:
            flash("Weekly goal must be a number.", "error")
            return redirect(url_for("user_page", username=username))

        if goal < 1 or goal > 14:
            flash("Weekly goal must be between 1 and 14.", "error")
            return redirect(url_for("user_page", username=username))

        user.weekly_goal_runs = goal
        db.session.commit()
        flash("Weekly goal updated ✅", "success")
        return redirect(url_for("user_page", username=username))

    # ---- Streak freeze (1 per month) ----
    @app.post("/ui/use-freeze/<username>")
    @login_required
    def ui_use_freeze(username):
        guard = ensure_own_user(username)
        if guard:
            return guard

        user = User.query.filter_by(username=username).first()
        if not user:
            return "User not found", 404

        # We apply freeze to yesterday (classic streak-freeze behavior)
        y = date.today() - timedelta(days=1)

        # Already ran yesterday? then freeze not needed
        ran_y = Run.query.filter(Run.user_id == user.id, Run.run_date == y).first() is not None
        if ran_y:
            flash("You ran yesterday — no freeze needed.", "success")
            return redirect(url_for("user_page", username=username))

        # Already used this month?
        mk = month_key(date.today())
        used = FreezeDay.query.filter(FreezeDay.user_id == user.id).all()
        if any(month_key(f.frozen_date) == mk for f in used):
            flash("Freeze already used this month.", "error")
            return redirect(url_for("user_page", username=username))

        # Create freeze
        db.session.add(FreezeDay(user_id=user.id, frozen_date=y))
        db.session.commit()
        flash("Streak freeze used for yesterday ✅", "success")
        return redirect(url_for("user_page", username=username))

    # ---- Edit run ----
    @app.get("/ui/run/<int:run_id>/edit")
    @login_required
    def ui_edit_run_page(run_id: int):
        me = current_user()
        r = Run.query.get(run_id)
        if not r:
            return "Run not found", 404
        if not me:
            return redirect(url_for("login_page"))
        if r.user_id != me.id:
            return "Forbidden", 403

        return render_template("run_edit.html", me=me, run=r)

    @app.post("/ui/run/<int:run_id>/edit")
    @login_required
    def ui_edit_run_post(run_id: int):
        me = current_user()
        r = Run.query.get(run_id)
        if not r:
            return "Run not found", 404
        if not me or r.user_id != me.id:
            return "Forbidden", 403

        run_date_s = (request.form.get("date") or "").strip()
        distance_s = (request.form.get("distance_km") or "").strip()
        duration_s = (request.form.get("duration_min") or "").strip()
        mood = (request.form.get("mood") or "").strip() or None

        try:
            d = parse_date(run_date_s)
        except Exception:
            flash("Date must be YYYY-MM-DD.", "error")
            return redirect(url_for("ui_edit_run_page", run_id=run_id))

        try:
            distance_km = float(distance_s)
        except Exception:
            flash("Distance must be a number.", "error")
            return redirect(url_for("ui_edit_run_page", run_id=run_id))

        duration_min = None
        if duration_s:
            try:
                duration_min = int(duration_s)
            except Exception:
                flash("Duration must be a whole number (minutes).", "error")
                return redirect(url_for("ui_edit_run_page", run_id=run_id))

        if distance_km <= 0 or distance_km > 200:
            flash("Distance must be between 0 and 200 km.", "error")
            return redirect(url_for("ui_edit_run_page", run_id=run_id))

        if duration_min is not None and (duration_min <= 0 or duration_min > 2000):
            flash("Duration must be a positive whole number.", "error")
            return redirect(url_for("ui_edit_run_page", run_id=run_id))

        if mood is not None and mood not in {"good", "meh", "tough"}:
            flash("Mood must be: good, meh, or tough.", "error")
            return redirect(url_for("ui_edit_run_page", run_id=run_id))

        r.run_date = d
        r.distance_km = distance_km
        r.duration_min = duration_min
        r.mood = mood
        db.session.commit()

        flash("Run updated ✅", "success")
        return redirect(url_for("user_page", username=me.username))

    # ---- Delete run ----
    @app.post("/ui/run/<int:run_id>/delete")
    @login_required
    def ui_delete_run(run_id: int):
        me = current_user()
        r = Run.query.get(run_id)
        if not r:
            return "Run not found", 404
        if not me or r.user_id != me.id:
            return "Forbidden", 403

        db.session.delete(r)
        db.session.commit()
        flash("Run deleted ✅", "success")
        return redirect(url_for("user_page", username=me.username))

    return app


app = create_app()

if __name__ == "__main__":
    app.run(debug=True)