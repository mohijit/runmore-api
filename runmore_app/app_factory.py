import os
from datetime import datetime, timedelta, date
from functools import wraps

from dotenv import load_dotenv
from flask import (
    Flask, jsonify, request, render_template,
    redirect, url_for, flash, session
)
from sqlalchemy import or_, and_
from werkzeug.security import generate_password_hash, check_password_hash
from jinja2 import ChoiceLoader, FileSystemLoader

from .extensions import db
from .models import (
    User, Run, FreezeDay, FriendRequest, Friendship, Message,
    Challenge, ChallengeParticipant, ChallengeInvite,
)
from .helpers import (
    parse_date, week_start, month_key, compute_streak, days_since_last_run,
    pace_min_per_km, format_pace, intensity_level, build_nudge,
    best_week_stats, freeze_available_this_month, frozen_dates_set,
    build_calendar_30, build_dashboard_for_user, build_home_analytics,
    build_runs_ui, challenge_target_label, build_challenge_leaderboard,
    get_challenge_progress_for_user,
)

load_dotenv()

# =========================
def create_app():
    package_root = os.path.dirname(__file__)
    repo_root = os.path.abspath(os.path.join(package_root, ".."))

    template_candidates = [
        os.path.join(repo_root, "templates"),
        os.path.join(os.getcwd(), "templates"),
        os.path.join(package_root, "templates"),
    ]
    template_paths = []
    for path in template_candidates:
        if path not in template_paths and os.path.isdir(path):
            template_paths.append(path)

    app = Flask(
        __name__,
        template_folder=(template_paths[0] if template_paths else "templates"),
    )

    if len(template_paths) > 1:
        app.jinja_loader = ChoiceLoader([FileSystemLoader(path) for path in template_paths])
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

    def friendship_exists(user_id: int, friend_id: int) -> bool:
        return Friendship.query.filter_by(user_id=user_id, friend_id=friend_id).first() is not None

    def create_friendship_pair(user_a_id: int, user_b_id: int) -> None:
        if not friendship_exists(user_a_id, user_b_id):
            db.session.add(Friendship(user_id=user_a_id, friend_id=user_b_id))
        if not friendship_exists(user_b_id, user_a_id):
            db.session.add(Friendship(user_id=user_b_id, friend_id=user_a_id))

    def get_friends_for_user(user_id: int) -> list[User]:
        rows = Friendship.query.filter_by(user_id=user_id).all()
        ids = [r.friend_id for r in rows]
        if not ids:
            return []
        return User.query.filter(User.id.in_(ids)).order_by(User.username.asc()).all()

    def get_pending_received(user_id: int) -> list[FriendRequest]:
        return FriendRequest.query.filter_by(to_user_id=user_id, status="pending").order_by(FriendRequest.created_at.desc()).all()

    def get_pending_sent(user_id: int) -> list[FriendRequest]:
        return FriendRequest.query.filter_by(from_user_id=user_id, status="pending").order_by(FriendRequest.created_at.desc()).all()

    def get_relationship_status(me: User, target: User) -> str:
        if me.id == target.id:
            return "self"
        if friendship_exists(me.id, target.id):
            return "friend"

        outgoing = FriendRequest.query.filter_by(
            from_user_id=me.id,
            to_user_id=target.id,
            status="pending"
        ).first()
        if outgoing:
            return "outgoing_pending"

        incoming = FriendRequest.query.filter_by(
            from_user_id=target.id,
            to_user_id=me.id,
            status="pending"
        ).first()
        if incoming:
            return "incoming_pending"

        return "none"

    def get_challenge_invites_for_user(user_id: int) -> list[ChallengeInvite]:
        return ChallengeInvite.query.filter_by(invited_user_id=user_id, status="pending").order_by(ChallengeInvite.created_at.desc()).all()

    def get_message_conversations(user_id: int) -> list[dict]:
        messages = Message.query.filter(
            or_(Message.sender_id == user_id, Message.recipient_id == user_id)
        ).order_by(Message.created_at.desc()).all()

        seen = set()
        conversations = []

        for m in messages:
            partner_id = m.recipient_id if m.sender_id == user_id else m.sender_id
            if partner_id in seen:
                continue
            seen.add(partner_id)
            partner = User.query.get(partner_id)

            unread_count = Message.query.filter_by(
                sender_id=partner_id,
                recipient_id=user_id,
                is_read=False
            ).count()

            conversations.append({
                "partner": partner,
                "latest_body": m.body,
                "latest_at": m.created_at,
                "unread_count": unread_count
            })

        return conversations

    def is_challenge_participant(challenge_id: int, user_id: int) -> bool:
        return ChallengeParticipant.query.filter_by(challenge_id=challenge_id, user_id=user_id).first() is not None

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

        if not user:
            user = User(username=username, weekly_goal_runs=3, password_hash=None)
            db.session.add(user)
            db.session.commit()
            session["pending_user_id"] = user.id
            flash("Welcome! Set a password to finish creating your account.", "success")
            return redirect(url_for("set_password_page"))

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
    # API routes
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
    # Main pages
    # -------------------------
    @app.get("/")
    @login_required
    def home_page():
        me = current_user()
        analytics = build_home_analytics(me)
        friends = get_friends_for_user(me.id)
        pending_received = get_pending_received(me.id)
        pending_sent = get_pending_sent(me.id)

        from_users = {}
        to_users = {}
        for fr in pending_received:
            from_users[fr.id] = User.query.get(fr.from_user_id)
        for fr in pending_sent:
            to_users[fr.id] = User.query.get(fr.to_user_id)

        return render_template(
            "home.html",
            me=me,
            analytics=analytics,
            friends=friends,
            pending_received=pending_received,
            pending_sent=pending_sent,
            from_users=from_users,
            to_users=to_users
        )

    @app.get("/friends")
    @login_required
    def friends_page():
        me = current_user()
        friends = get_friends_for_user(me.id)
        pending_received = get_pending_received(me.id)
        pending_sent = get_pending_sent(me.id)

        from_users = {}
        to_users = {}
        for fr in pending_received:
            from_users[fr.id] = User.query.get(fr.from_user_id)
        for fr in pending_sent:
            to_users[fr.id] = User.query.get(fr.to_user_id)

        return render_template(
            "friends.html",
            me=me,
            friends=friends,
            pending_received=pending_received,
            pending_sent=pending_sent,
            from_users=from_users,
            to_users=to_users
        )

    @app.get("/people")
    @login_required
    def people_search_page():
        me = current_user()
        q = (request.args.get("q") or "").strip()
        results = []

        if q:
            results = User.query.filter(
                User.username.ilike(f"%{q}%"),
                User.id != me.id
            ).order_by(User.username.asc()).limit(30).all()

        relationship_map = {}
        for user in results:
            relationship_map[user.id] = get_relationship_status(me, user)

        return render_template(
            "people_search.html",
            me=me,
            q=q,
            results=results,
            relationship_map=relationship_map
        )

    @app.get("/profile/<username>")
    @login_required
    def profile_page(username):
        me = current_user()
        target = User.query.filter_by(username=username).first()
        if not target:
            return "User not found", 404

        relationship = get_relationship_status(me, target)

        if relationship in {"self", "friend"}:
            dash = build_dashboard_for_user(target)
            runs = build_runs_ui(target.id, limit=30)

            return render_template(
                "friend_profile.html",
                me=me,
                target=target,
                relationship=relationship,
                dash=dash,
                runs=runs,
                can_view_full=True,
                is_self=(relationship == "self")
            )

        return render_template(
            "friend_profile.html",
            me=me,
            target=target,
            relationship=relationship,
            dash=None,
            runs=[],
            can_view_full=False,
            is_self=False
        )

    # -------------------------
    # Friend requests
    # -------------------------
    @app.post("/ui/send-friend-request")
    @login_required
    def ui_send_friend_request():
        me = current_user()
        target_username = (request.form.get("target_username") or "").strip()

        if not target_username:
            flash("Enter a username to send a friend request.", "error")
            return redirect(request.referrer or url_for("home_page"))

        target = User.query.filter_by(username=target_username).first()
        if not target:
            flash("That user does not exist.", "error")
            return redirect(request.referrer or url_for("home_page"))

        if target.id == me.id:
            flash("You cannot send a friend request to yourself.", "error")
            return redirect(request.referrer or url_for("home_page"))

        if friendship_exists(me.id, target.id):
            flash("You are already friends.", "error")
            return redirect(request.referrer or url_for("home_page"))

        existing_pending = FriendRequest.query.filter(
            or_(
                and_(FriendRequest.from_user_id == me.id, FriendRequest.to_user_id == target.id),
                and_(FriendRequest.from_user_id == target.id, FriendRequest.to_user_id == me.id)
            ),
            FriendRequest.status == "pending"
        ).first()

        if existing_pending:
            flash("A pending friend request already exists between you and that user.", "error")
            return redirect(request.referrer or url_for("home_page"))

        db.session.add(FriendRequest(from_user_id=me.id, to_user_id=target.id, status="pending"))
        db.session.commit()
        flash(f"Friend request sent to {target.username} ✅", "success")
        return redirect(request.referrer or url_for("home_page"))

    @app.post("/ui/friend-request/<int:request_id>/accept")
    @login_required
    def ui_accept_friend_request(request_id: int):
        me = current_user()
        fr = FriendRequest.query.get(request_id)

        if not fr:
            flash("Friend request not found.", "error")
            return redirect(url_for("inbox_page"))

        if fr.to_user_id != me.id:
            return "Forbidden", 403

        if fr.status != "pending":
            flash("That friend request is no longer pending.", "error")
            return redirect(url_for("inbox_page"))

        fr.status = "accepted"
        create_friendship_pair(fr.from_user_id, fr.to_user_id)
        db.session.commit()

        sender = User.query.get(fr.from_user_id)
        flash(f"You are now friends with {sender.username} ✅", "success")
        return redirect(url_for("inbox_page"))

    @app.post("/ui/friend-request/<int:request_id>/decline")
    @login_required
    def ui_decline_friend_request(request_id: int):
        me = current_user()
        fr = FriendRequest.query.get(request_id)

        if not fr:
            flash("Friend request not found.", "error")
            return redirect(url_for("inbox_page"))

        if fr.to_user_id != me.id:
            return "Forbidden", 403

        if fr.status != "pending":
            flash("That friend request is no longer pending.", "error")
            return redirect(url_for("inbox_page"))

        fr.status = "declined"
        db.session.commit()
        flash("Friend request declined.", "success")
        return redirect(url_for("inbox_page"))

    # -------------------------
    # Inbox + messages
    # -------------------------
    @app.get("/inbox")
    @login_required
    def inbox_page():
        me = current_user()

        pending_received = get_pending_received(me.id)
        from_users = {fr.id: User.query.get(fr.from_user_id) for fr in pending_received}

        challenge_invites = get_challenge_invites_for_user(me.id)
        invite_context = {}
        for inv in challenge_invites:
            invite_context[inv.id] = {
                "challenge": Challenge.query.get(inv.challenge_id),
                "from_user": User.query.get(inv.invited_by_user_id)
            }

        conversations = get_message_conversations(me.id)

        return render_template(
            "inbox.html",
            me=me,
            pending_received=pending_received,
            from_users=from_users,
            challenge_invites=challenge_invites,
            invite_context=invite_context,
            conversations=conversations
        )

    @app.get("/messages/<username>")
    @login_required
    def conversation_page(username):
        me = current_user()
        friend = User.query.filter_by(username=username).first()

        if not friend:
            return "User not found", 404

        if not friendship_exists(me.id, friend.id):
            return "Forbidden", 403

        msgs = Message.query.filter(
            or_(
                and_(Message.sender_id == me.id, Message.recipient_id == friend.id),
                and_(Message.sender_id == friend.id, Message.recipient_id == me.id)
            )
        ).order_by(Message.created_at.asc()).all()

        unread = Message.query.filter_by(sender_id=friend.id, recipient_id=me.id, is_read=False).all()
        for m in unread:
            m.is_read = True
        db.session.commit()

        return render_template(
            "conversation.html",
            me=me,
            friend=friend,
            messages=msgs
        )

    @app.post("/messages/<username>/send")
    @login_required
    def send_message(username):
        me = current_user()
        friend = User.query.filter_by(username=username).first()

        if not friend:
            return "User not found", 404

        if not friendship_exists(me.id, friend.id):
            return "Forbidden", 403

        body = (request.form.get("body") or "").strip()
        if not body:
            flash("Message cannot be empty.", "error")
            return redirect(url_for("conversation_page", username=username))

        db.session.add(Message(sender_id=me.id, recipient_id=friend.id, body=body))
        db.session.commit()
        flash("Message sent ✅", "success")
        return redirect(url_for("conversation_page", username=username))

    # -------------------------
    # Challenges
    # -------------------------
    @app.get("/challenges")
    @login_required
    def challenges_page():
        me = current_user()
        friends = get_friends_for_user(me.id)

        my_participations = ChallengeParticipant.query.filter_by(user_id=me.id).all()
        participating_ids = [p.challenge_id for p in my_participations]
        challenges = Challenge.query.filter(Challenge.id.in_(participating_ids)).order_by(Challenge.created_at.desc()).all() if participating_ids else []

        challenge_cards = []
        for c in challenges:
            progress = get_challenge_progress_for_user(c, me.id)
            challenge_cards.append({
                "challenge": c,
                "target_label": challenge_target_label(c),
                "progress": progress
            })

        return render_template(
            "challenges.html",
            me=me,
            friends=friends,
            challenge_cards=challenge_cards
        )

    @app.post("/challenges/create")
    @login_required
    def create_challenge():
        me = current_user()

        challenge_type = (request.form.get("challenge_type") or "").strip()
        target_distance_raw = (request.form.get("target_distance_km") or "").strip()
        pace_target_raw = (request.form.get("pace_target_min_per_km") or "").strip()
        description = (request.form.get("description") or "").strip() or None
        invited_ids = request.form.getlist("friend_ids")

        if challenge_type not in {"single", "cumulative"}:
            flash("Choose a valid challenge type.", "error")
            return redirect(url_for("challenges_page"))

        try:
            target_distance_km = float(target_distance_raw)
        except Exception:
            flash("Target distance must be a number.", "error")
            return redirect(url_for("challenges_page"))

        if target_distance_km <= 0:
            flash("Target distance must be greater than 0.", "error")
            return redirect(url_for("challenges_page"))

        pace_target_min_per_km = None
        if challenge_type == "single":
            try:
                pace_target_min_per_km = float(pace_target_raw)
            except Exception:
                flash("Single-run challenges need a pace target (minutes per km).", "error")
                return redirect(url_for("challenges_page"))
            if pace_target_min_per_km <= 0:
                flash("Pace target must be greater than 0.", "error")
                return redirect(url_for("challenges_page"))

        challenge = Challenge(
            creator_id=me.id,
            challenge_type=challenge_type,
            target_distance_km=target_distance_km,
            pace_target_min_per_km=pace_target_min_per_km,
            description=description
        )
        db.session.add(challenge)
        db.session.flush()

        db.session.add(ChallengeParticipant(
            challenge_id=challenge.id,
            user_id=me.id,
            accepted_at=challenge.created_at or datetime.utcnow(),
            joined_via_invite=False
        ))

        valid_friend_ids = {f.id for f in get_friends_for_user(me.id)}
        invited_count = 0

        for raw_id in invited_ids:
            try:
                friend_id = int(raw_id)
            except Exception:
                continue
            if friend_id not in valid_friend_ids:
                continue
            if friend_id == me.id:
                continue

            existing = ChallengeInvite.query.filter_by(challenge_id=challenge.id, invited_user_id=friend_id).first()
            if existing:
                continue

            db.session.add(ChallengeInvite(
                challenge_id=challenge.id,
                invited_user_id=friend_id,
                invited_by_user_id=me.id,
                status="pending"
            ))
            invited_count += 1

        db.session.commit()
        flash(f"Challenge created ✅ Invited {invited_count} friend(s).", "success")
        return redirect(url_for("challenge_detail_page", challenge_id=challenge.id))

    @app.get("/challenges/<int:challenge_id>")
    @login_required
    def challenge_detail_page(challenge_id: int):
        me = current_user()
        challenge = Challenge.query.get(challenge_id)
        if not challenge:
            return "Challenge not found", 404

        if not is_challenge_participant(challenge.id, me.id):
            return "Forbidden", 403

        creator = User.query.get(challenge.creator_id)
        leaderboard = build_challenge_leaderboard(challenge)
        participants = ChallengeParticipant.query.filter_by(challenge_id=challenge.id).all()
        participant_users = [User.query.get(p.user_id) for p in participants]

        return render_template(
            "challenge_detail.html",
            me=me,
            challenge=challenge,
            creator=creator,
            target_label=challenge_target_label(challenge),
            leaderboard=leaderboard,
            participant_users=participant_users
        )

    @app.post("/challenge-invites/<int:invite_id>/accept")
    @login_required
    def accept_challenge_invite(invite_id: int):
        me = current_user()
        invite = ChallengeInvite.query.get(invite_id)

        if not invite:
            flash("Challenge invite not found.", "error")
            return redirect(url_for("inbox_page"))

        if invite.invited_user_id != me.id:
            return "Forbidden", 403

        if invite.status != "pending":
            flash("That challenge invite is no longer pending.", "error")
            return redirect(url_for("inbox_page"))

        challenge = Challenge.query.get(invite.challenge_id)
        if not challenge:
            flash("Challenge not found.", "error")
            return redirect(url_for("inbox_page"))

        if not is_challenge_participant(challenge.id, me.id):
            db.session.add(ChallengeParticipant(
                challenge_id=challenge.id,
                user_id=me.id,
                accepted_at=datetime.utcnow(),
                joined_via_invite=True
            ))

        invite.status = "accepted"
        db.session.commit()

        flash("Challenge accepted ✅", "success")
        return redirect(url_for("challenge_detail_page", challenge_id=challenge.id))

    @app.post("/challenge-invites/<int:invite_id>/decline")
    @login_required
    def decline_challenge_invite(invite_id: int):
        me = current_user()
        invite = ChallengeInvite.query.get(invite_id)

        if not invite:
            flash("Challenge invite not found.", "error")
            return redirect(url_for("inbox_page"))

        if invite.invited_user_id != me.id:
            return "Forbidden", 403

        if invite.status != "pending":
            flash("That challenge invite is no longer pending.", "error")
            return redirect(url_for("inbox_page"))

        invite.status = "declined"
        db.session.commit()
        flash("Challenge invite declined.", "success")
        return redirect(url_for("inbox_page"))

    # -------------------------
    # Personal dashboard
    # -------------------------
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
        runs_ui = build_runs_ui(user.id, 20)

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

    @app.post("/ui/use-freeze/<username>")
    @login_required
    def ui_use_freeze(username):
        guard = ensure_own_user(username)
        if guard:
            return guard

        user = User.query.filter_by(username=username).first()
        if not user:
            return "User not found", 404

        y = date.today() - timedelta(days=1)
        ran_y = Run.query.filter(Run.user_id == user.id, Run.run_date == y).first() is not None
        if ran_y:
            flash("You ran yesterday — no freeze needed.", "success")
            return redirect(url_for("user_page", username=username))

        mk = month_key(date.today())
        used = FreezeDay.query.filter(FreezeDay.user_id == user.id).all()
        if any(month_key(f.frozen_date) == mk for f in used):
            flash("Freeze already used this month.", "error")
            return redirect(url_for("user_page", username=username))

        db.session.add(FreezeDay(user_id=user.id, frozen_date=y))
        db.session.commit()
        flash("Streak freeze used for yesterday ✅", "success")
        return redirect(url_for("user_page", username=username))

    @app.get("/ui/run/<int:run_id>/edit")
    @login_required
    def ui_edit_run_page(run_id: int):
        me = current_user()
        r = Run.query.get(run_id)
        if not r:
            return "Run not found", 404
        if not me or r.user_id != me.id:
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
