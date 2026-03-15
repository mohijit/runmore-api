import os
from datetime import datetime, date, timedelta
from functools import wraps

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", "sqlite:///app.db")
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["API_KEY"] = os.getenv("API_KEY", "dev-change-me")
    app.config["JSON_SORT_KEYS"] = False

    db.init_app(app)

    # --- Models ---
    class User(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        username = db.Column(db.String(80), unique=True, nullable=False, index=True)
        weekly_goal_runs = db.Column(db.Integer, nullable=False, default=3)  # runs/week
        created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    class Run(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
        run_date = db.Column(db.Date, nullable=False, index=True)
        distance_km = db.Column(db.Float, nullable=False)
        duration_min = db.Column(db.Integer, nullable=True)
        mood = db.Column(db.String(30), nullable=True)  # "good", "meh", "tough"
        created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    with app.app_context():
        db.create_all()

    # --- Auth ---
    def require_key(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            key = request.headers.get("X-API-KEY")
            if not key or key != app.config["API_KEY"]:
                return jsonify({"error": "unauthorized"}), 401
            return f(*args, **kwargs)
        return wrapper

    # --- Helpers ---
    def parse_date(s: str) -> date:
        # expects YYYY-MM-DD
        return datetime.strptime(s, "%Y-%m-%d").date()

    def week_start(d: date) -> date:
        return d - timedelta(days=d.weekday())  # Monday

    def compute_streak(run_dates_set: set[date]) -> int:
        # streak of consecutive days ending today where there is at least one run on each day
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
            return "No guilt. Just momentum: put on shoes, run 8–12 minutes, stop if you want."
        if weekly_done < weekly_goal:
            remaining = weekly_goal - weekly_done
            return f"You’re {remaining} run(s) away from your weekly goal. Schedule the next one for tomorrow."
        return "You’ve hit your weekly goal — bonus run = stronger habit."

    def next_run_suggestion(days_since: int | None, last_distance: float | None) -> dict:
        # simple, safe heuristic
        if days_since is None:
            return {"type": "starter", "plan": "10 min easy jog + 5 min walk", "when": "today"}
        if days_since >= 4:
            return {"type": "comeback", "plan": "12–20 min easy, keep it conversational", "when": "today"}
        # if recent, alternate easy/long-ish
        base = 3.0 if last_distance is None else max(2.0, min(last_distance * 1.1, last_distance + 1.0))
        return {"type": "easy", "plan": f"{base:.1f} km easy (or 20–30 min)", "when": "tomorrow"}

    # --- Routes ---
    @app.get("/health")
    def health():
        return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})
    
    @app.get("/")
    def home():
        return jsonify({
            "message": "RunMore API is running",
            "try": ["/health", "/users/<username>/dashboard"]
        })

    @app.post("/users")
    @require_key
    def create_user():
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
    def log_run(username):
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

        if not isinstance(distance_km, (int, float)) or distance_km <= 0 or distance_km > 200:
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
    def dashboard(username):
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

    @app.get("/users/<username>/nudge")
    @require_key
    def nudge(username):
        # Dedicated endpoint for notifications later
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

        last_run = runs[-1] if runs else None
        last_run_date = last_run.run_date if last_run else None
        dslr = days_since_last_run(last_run_date)

        msg = motivation_message(streak, dslr, weekly_done, user.weekly_goal_runs)
        return jsonify({"message": msg, "streak_days": streak, "days_since_last_run": dslr})

    return app

app = create_app()

print("=== REGISTERED ROUTES ===")
for r in app.url_map.iter_rules():
    print(r, r.methods)
print("=========================")

if __name__ == "__main__":
    app.run(debug=True)