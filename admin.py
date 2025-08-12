from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from functools import wraps
import database as db

# Create a 'Blueprint' for the admin section. This helps organize routes.
admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

# --- Admin Authentication Decorator ---
def admin_required(f):
    """A decorator to ensure a user is a logged-in admin."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Prefer full Supabase session if present
        access_token = session.get('access_token')
        if access_token and session.get('refresh_token'):
            sb = db.get_supabase_client()
            if not sb:
                flash("Backend not configured. Please set SUPABASE_URL and SUPABASE_KEY.", "error")
                return redirect(url_for('dashboard'))
            try:
                sb.auth.set_session(access_token, session.get('refresh_token'))
                user = sb.auth.get_user()
                if not user:
                    raise Exception("User not found")
                profile = sb.table('profiles').select('is_admin').eq('id', user.user.id).single().execute().data
                if not profile or not profile.get('is_admin'):
                    flash("You do not have permission to access this page.", "error")
                    return redirect(url_for('dashboard'))
                return f(sb, *args, **kwargs)
            except Exception as e:
                flash(f"Admin access error: {e}", "error")
                return redirect(url_for('dashboard'))

        # Fallback: use service client and app session identity (email/user_id)
        if not session.get('user_email'):
            return redirect(url_for('login'))

        sb_admin = db.get_supabase_client(service_role=True)
        if not sb_admin:
            flash("Admin backend not configured. Please set SUPABASE_URL and SUPABASE_SERVICE_KEY in your environment.", "error")
            return redirect(url_for('dashboard'))
        try:
            user_id = session.get('user_id')
            profile_query = sb_admin.table('profiles').select('id, is_admin')
            if user_id:
                profile_query = profile_query.eq('id', user_id)
            else:
                profile_query = profile_query.eq('email', session.get('user_email'))
            profile = profile_query.single().execute().data
            if not profile or not profile.get('is_admin'):
                flash("You do not have permission to access this page.", "error")
                return redirect(url_for('dashboard'))
        except Exception as e:
            flash(f"Admin access error: {e}", "error")
            return redirect(url_for('dashboard'))

        return f(sb_admin, *args, **kwargs)
    return decorated_function

# --- Admin Panel Routes ---
@admin_bp.route('/')
@admin_required
def dashboard(sb):
    """Main admin dashboard. Shows a list of all users."""
    all_users = db.admin_get_all_users()
    return render_template('admin_dashboard.html', users=all_users, selected_user=None)

@admin_bp.route('/cron_runs')
@admin_required
def cron_runs(sb):
    """Admin-only page: view last cron run summaries (counts per user)."""
    # Try a robust fetch that works even if ordering fails
    error_msg = None
    try:
        q = sb.table('cron_run_logs').select('*')
        try:
            q = q.order('run_at', desc=True)
        except Exception:
            pass
        rows = q.limit(500).execute().data or []
    except Exception as e:
        rows = []
        error_msg = f"Query error: {e}"

    try:
        from collections import defaultdict
        grouped = defaultdict(list)
        for r in rows:
            grouped[r.get('run_id')].append(r)
        runs = []
        for run_id, items in grouped.items():
            if not items:
                continue
            # Prefer the newest item's run_at/job
            items_sorted = sorted(items, key=lambda x: (x.get('user_id') or ''))
            job = (sorted(items, key=lambda x: str(x.get('run_at') or ''))[-1]).get('job')
            run_at = (sorted(items, key=lambda x: str(x.get('run_at') or ''))[-1]).get('run_at')
            total_users = len({i.get('user_id') for i in items if i.get('user_id')})
            processed_users = sum(1 for i in items if i.get('processed'))
            skipped_users = sum(1 for i in items if not i.get('processed'))
            total_notifications = sum(int(i.get('notifications_sent') or 0) for i in items)
            total_recipients = sum(int(i.get('recipients') or 0) for i in items)
            runs.append({
                'run_id': run_id,
                'run_at': run_at,
                'job': job,
                'total_users': total_users,
                'processed_users': processed_users,
                'skipped_users': skipped_users,
                'total_notifications': total_notifications,
                'total_recipients': total_recipients,
                'items': items_sorted[:50],
            })
        runs = sorted(runs, key=lambda x: str(x.get('run_at') or ''), reverse=True)[:10]
        if error_msg:
            flash(error_msg, 'warning')
        return render_template('admin_cron_runs.html', runs=runs)
    except Exception as e:
        flash(f"Error processing cron runs: {e}", 'error')
        return render_template('admin_cron_runs.html', runs=[])

@admin_bp.route('/user/<user_id>')
@admin_required
def view_user(sb, user_id):
    """Shows the scrips and recipients for a specific user."""
    all_users = db.admin_get_all_users()
    selected_user_data = db.admin_get_user_details(user_id)
    
    return render_template('admin_dashboard.html', 
                           users=all_users, 
                           selected_user=selected_user_data)

@admin_bp.route('/add_scrip', methods=['POST'])
@admin_required
def add_scrip(sb):
    user_id = request.form['user_id']
    bse_code = request.form['scrip_code']
    company_name = request.form['company_name']
    db.admin_add_scrip_for_user(user_id, bse_code, company_name)
    return redirect(url_for('admin.view_user', user_id=user_id))

@admin_bp.route('/delete_scrip', methods=['POST'])
@admin_required
def delete_scrip(sb):
    user_id = request.form['user_id']
    bse_code = request.form['scrip_code']
    db.admin_delete_scrip_for_user(user_id, bse_code)
    return redirect(url_for('admin.view_user', user_id=user_id))

@admin_bp.route('/add_recipient', methods=['POST'])
@admin_required
def add_recipient(sb):
    user_id = request.form['user_id']
    chat_id = request.form['chat_id']
    db.admin_add_recipient_for_user(user_id, chat_id)
    return redirect(url_for('admin.view_user', user_id=user_id))

@admin_bp.route('/delete_recipient', methods=['POST'])
@admin_required
def delete_recipient(sb):
    user_id = request.form['user_id']
    chat_id = request.form['chat_id']
    db.admin_delete_recipient_for_user(user_id, chat_id)
    return redirect(url_for('admin.view_user', user_id=user_id))

@admin_bp.route('/purge', methods=['POST'])
@admin_required
def purge_data(sb):
    """Purge all data except for the current admin user, guarded by a secret."""
    secret = request.form.get('secret', '')
    if secret != 'vadodara':
        flash('Invalid secret for purge operation.', 'error')
        return redirect(url_for('admin.dashboard'))

    current_user_id = session.get('user_id')
    if not current_user_id:
        # Try to resolve from email via profiles
        try:
            email = session.get('user_email')
            if email:
                profile = sb.table('profiles').select('id').eq('email', email).single().execute().data
                current_user_id = profile and profile.get('id')
        except Exception:
            current_user_id = None

    if not current_user_id:
        flash('Could not determine current admin user id.', 'error')
        return redirect(url_for('admin.dashboard'))

    try:
        # Keep only current admin's rows in core tables
        sb.table('seen_announcements').delete().neq('user_id', current_user_id).execute()
        sb.table('monitored_scrips').delete().neq('user_id', current_user_id).execute()
        sb.table('telegram_recipients').delete().neq('user_id', current_user_id).execute()
        flash('Purge complete. Kept only your data.', 'success')
    except Exception as e:
        flash(f'Purge failed: {e}', 'error')

    return redirect(url_for('admin.dashboard'))
