from datetime import datetime, date, timedelta

from .models import (
    User,
    Run,
    FreezeDay,
    Challenge,
    ChallengeParticipant,
)

# =========================
def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())


def month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


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
    if distance_km <= 0:
        return 0
    if distance_km <= 3:
        return 1
    if distance_km <= 7:
        return 2
    return 3


def build_nudge(days_since: int | None, streak: int, weekly_done: int, weekly_goal: int) -> str:
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
    if not runs:
        return {"week_start": None, "runs": 0, "distance_km": 0.0}

    buckets: dict[date, list[Run]] = {}
    for r in runs:
        ws = week_start(r.run_date)
        buckets.setdefault(ws, []).append(r)

    best_ws = None
    best_score = (-1, -1.0)
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
    existing = FreezeDay.query.filter(FreezeDay.user_id == user_id).all()
    mk = month_key(date.today())
    return not any(month_key(f.frozen_date) == mk for f in existing)


def frozen_dates_set(user_id: int) -> set[date]:
    frz = FreezeDay.query.filter(FreezeDay.user_id == user_id).all()
    return {f.frozen_date for f in frz}


def build_calendar_30(user_runs: list[Run], frozen: set[date]) -> dict:
    today = date.today()
    start = today - timedelta(days=29)

    day_level: dict[date, int] = {}
    run_dates_set = {r.run_date for r in user_runs}

    for r in user_runs:
        if start <= r.run_date <= today:
            day_level[r.run_date] = max(day_level.get(r.run_date, 0), intensity_level(r.distance_km))

    for d in frozen:
        if start <= d <= today:
            day_level[d] = max(day_level.get(d, 0), 1)

    days = []
    cur = start
    while cur <= today:
        days.append({
            "date": cur.isoformat(),
            "dow": cur.weekday(),
            "level": day_level.get(cur, 0),
            "is_frozen": (cur in frozen) and (cur not in run_dates_set),
        })
        cur += timedelta(days=1)

    cols: dict[date, dict[int, dict]] = {}
    for item in days:
        d = parse_date(item["date"])
        ws = week_start(d)
        cols.setdefault(ws, {})
        cols[ws][d.weekday()] = item

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
    runs_all = Run.query.filter(Run.user_id == user.id).order_by(Run.run_date.asc()).all()

    frozen = frozen_dates_set(user.id)
    run_dates = {r.run_date for r in runs_all} | frozen
    streak = compute_streak(run_dates)

    today = date.today()
    ws = week_start(today)
    we = ws + timedelta(days=7)

    week_runs = [r for r in runs_all if ws <= r.run_date < we]
    weekly_done = len(week_runs)
    weekly_km = round(sum(r.distance_km for r in week_runs), 2)

    last_run = runs_all[-1] if runs_all else None
    last_run_date = last_run.run_date if last_run else None
    dslr = days_since_last_run(last_run_date)

    nudge = build_nudge(dslr, streak, weekly_done, user.weekly_goal_runs)

    if dslr is None:
        next_run = {"when": "today", "plan": "10 min easy jog + 5 min walk"}
    elif dslr >= 4:
        next_run = {"when": "today", "plan": "12–20 min easy, conversational pace"}
    else:
        next_run = {"when": "tomorrow", "plan": "20–30 min easy (or 3–5 km)"}

    longest = max((r.distance_km for r in runs_all), default=0.0)

    best_pace_val = None
    best_pace_run = None
    for r in runs_all:
        if r.duration_min is not None:
            p = pace_min_per_km(r.distance_km, r.duration_min)
            if p is not None and (best_pace_val is None or p < best_pace_val):
                best_pace_val = p
                best_pace_run = r

    best_week = best_week_stats(runs_all)
    cal = build_calendar_30(runs_all, frozen)

    freeze_available = freeze_available_this_month(user.id)
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


def build_home_analytics(user: User) -> dict:
    runs = Run.query.filter(Run.user_id == user.id).order_by(Run.run_date.asc()).all()
    frozen = frozen_dates_set(user.id)
    run_dates = {r.run_date for r in runs} | frozen
    streak = compute_streak(run_dates)

    total_runs = len(runs)
    total_km = round(sum(r.distance_km for r in runs), 2)
    avg_distance = round((total_km / total_runs), 2) if total_runs else 0.0

    longest = max((r.distance_km for r in runs), default=0.0)

    best_pace_val = None
    for r in runs:
        if r.duration_min is not None:
            p = pace_min_per_km(r.distance_km, r.duration_min)
            if p is not None and (best_pace_val is None or p < best_pace_val):
                best_pace_val = p

    best_week = best_week_stats(runs)

    today = date.today()
    weekly_blocks = []
    for i in range(3, -1, -1):
        ws = week_start(today) - timedelta(days=7 * i)
        we = ws + timedelta(days=7)
        week_runs = [r for r in runs if ws <= r.run_date < we]
        km = round(sum(r.distance_km for r in week_runs), 2)
        weekly_blocks.append({
            "label": ws.strftime("%d %b"),
            "runs": len(week_runs),
            "km": km
        })

    max_weekly_km = max([b["km"] for b in weekly_blocks], default=0)

    active_days_30 = len({r.run_date for r in runs if r.run_date >= today - timedelta(days=29)})

    return {
        "total_runs": total_runs,
        "total_km": total_km,
        "avg_distance": avg_distance,
        "current_streak": streak,
        "active_days_30": active_days_30,
        "longest_km": round(longest, 2),
        "fastest_pace": format_pace(best_pace_val),
        "best_week_runs": best_week["runs"],
        "best_week_km": best_week["distance_km"],
        "best_week_start": best_week["week_start"].isoformat() if best_week["week_start"] else None,
        "weekly_blocks": weekly_blocks,
        "max_weekly_km": max_weekly_km,
    }


def build_runs_ui(user_id: int, limit: int = 20) -> list[dict]:
    runs = Run.query.filter(Run.user_id == user_id).order_by(Run.run_date.desc()).limit(limit).all()
    rows = []
    for r in runs:
        p = pace_min_per_km(r.distance_km, r.duration_min) if r.duration_min else None
        rows.append({
            "id": r.id,
            "run_date": r.run_date,
            "distance_km": r.distance_km,
            "duration_min": r.duration_min,
            "mood": r.mood,
            "pace_str": format_pace(p),
        })
    return rows


def challenge_target_label(challenge: Challenge) -> str:
    if challenge.challenge_type == "single":
        return f"{challenge.target_distance_km:.2f} km under {format_pace(challenge.pace_target_min_per_km)}"
    return f"{challenge.target_distance_km:.2f} cumulative km per person"


def build_challenge_leaderboard(challenge: Challenge) -> list[dict]:
    participants = ChallengeParticipant.query.filter_by(challenge_id=challenge.id).all()
    rows = []

    for p in participants:
        user = User.query.get(p.user_id)
        eligible_runs = Run.query.filter(
            Run.user_id == p.user_id,
            Run.created_at >= p.accepted_at
        ).order_by(Run.created_at.asc()).all()

        if challenge.challenge_type == "single":
            best_run = None
            best_pace = None

            for r in eligible_runs:
                if r.distance_km >= challenge.target_distance_km and r.duration_min is not None:
                    pace = pace_min_per_km(r.distance_km, r.duration_min)
                    if pace is not None and pace <= challenge.pace_target_min_per_km:
                        if best_pace is None or pace < best_pace:
                            best_pace = pace
                            best_run = r

            rows.append({
                "username": user.username,
                "completed": best_run is not None,
                "metric_primary": best_pace if best_pace is not None else float("inf"),
                "display": format_pace(best_pace) if best_pace is not None else "Not completed",
                "detail": best_run.run_date.isoformat() if best_run else "",
                "progress_value": 1 if best_run else 0
            })

        else:
            total = round(sum(r.distance_km for r in eligible_runs), 2)
            completed = total >= challenge.target_distance_km
            rows.append({
                "username": user.username,
                "completed": completed,
                "metric_primary": -total,
                "display": f"{total:.2f} km",
                "detail": "Completed" if completed else "In progress",
                "progress_value": total
            })

    if challenge.challenge_type == "single":
        rows.sort(key=lambda x: (not x["completed"], x["metric_primary"], x["username"].lower()))
    else:
        rows.sort(key=lambda x: (x["metric_primary"], x["username"].lower()))

    for i, row in enumerate(rows, start=1):
        row["rank"] = i

    return rows


def get_challenge_progress_for_user(challenge: Challenge, user_id: int) -> dict:
    participant = ChallengeParticipant.query.filter_by(challenge_id=challenge.id, user_id=user_id).first()
    if not participant:
        return {"joined": False}

    runs = Run.query.filter(
        Run.user_id == user_id,
        Run.created_at >= participant.accepted_at
    ).all()

    if challenge.challenge_type == "single":
        qualifying = []
        for r in runs:
            if r.distance_km >= challenge.target_distance_km and r.duration_min is not None:
                pace = pace_min_per_km(r.distance_km, r.duration_min)
                if pace is not None and pace <= challenge.pace_target_min_per_km:
                    qualifying.append((pace, r))

        if not qualifying:
            return {"joined": True, "completed": False, "display": "No qualifying run yet"}

        best = min(qualifying, key=lambda x: x[0])
        return {
            "joined": True,
            "completed": True,
            "display": f"Best: {format_pace(best[0])} on {best[1].run_date.isoformat()}"
        }

    total = round(sum(r.distance_km for r in runs), 2)
    return {
        "joined": True,
        "completed": total >= challenge.target_distance_km,
        "display": f"{total:.2f} / {challenge.target_distance_km:.2f} km"
    }


