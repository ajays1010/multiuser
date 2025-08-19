import os
from dotenv import load_dotenv
from functools import wraps
load_dotenv()

from flask import Flask, request, render_template, redirect, url_for, session, flash, jsonify
# from supabase import create_client  # not used directly
import pandas as pd
import database as db
from firebase_admin import auth
from admin import admin_bp
import uuid
from sentiment_analyzer import get_sentiment_analysis_for_stock, create_sentiment_visualizations
from logging_config import github_logger
import logging
import traceback
import atexit

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "a-super-secret-key-for-local-testing")
app.register_blueprint(admin_bp)

# Initialize logging
github_logger.log_app_start()

# Error handling decorator
def log_errors(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            github_logger.log_error(e, f"Route: {request.endpoint}")
            raise
    return decorated_function

# Global error handlers
@app.errorhandler(404)
def not_found_error(error):
    return {'error': 'Not found', 'message': 'The requested URL was not found on the server.'}, 404

@app.errorhandler(500)
def internal_error(error):
    github_logger.log_error(error, "Internal Server Error")
    return {'error': 'Internal server error', 'timestamp': str(error)}, 500

@app.errorhandler(Exception)
def handle_exception(e):
    # Don't log 404 errors as exceptions
    if hasattr(e, 'code') and e.code == 404:
        return {'error': 'Not found', 'message': 'The requested URL was not found on the server.'}, 404
    
    github_logger.log_error(e, "Unhandled Exception")
    return {'error': 'Application error', 'details': str(e)}, 500

# Log memory usage periodically and push logs to GitHub
def cleanup_and_log():
    github_logger.log_memory_usage()
    github_logger.push_logs_to_github()

atexit.register(cleanup_and_log)

# Ensure Firebase Admin SDK is initialized when the app starts (works under Gunicorn too)
db.initialize_firebase()

# --- Load local company data into memory for searching ---
try:
    company_df = pd.read_csv('indian_stock_tickers.csv')
    company_df['BSE Code'] = company_df['BSE Code'].astype(str).fillna('')
except FileNotFoundError:
    print("[CRITICAL ERROR] The company list 'indian_stock_tickers.csv' was not found. Search will not work.")
    company_df = pd.DataFrame(columns=['BSE Code', 'Company Name'])

# --- Helper function to get an authenticated Supabase client ---
def get_authenticated_client():
    """
    Creates a Supabase client instance for the current user session.
    Prioritizes a full Supabase session, but falls back to a service role client
    if the user is logged in via a Flask session (e.g., email-only).
    """
    access_token = session.get('access_token')
    refresh_token = session.get('refresh_token')
    if access_token and refresh_token:
        sb = db.get_supabase_client()
        try:
            sb.auth.set_session(access_token, refresh_token)
            return sb
        except Exception as e:
            print(f"Session authentication error: {e}")
            # If session is invalid, clear it to force re-login
            session.pop('access_token', None)
            session.pop('refresh_token', None)

    # Fallback for users logged in without a full Supabase session
    if session.get('user_email'):
        return db.get_supabase_client(service_role=True)

    return None

# --- Decorator for Protected Routes ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        sb_client = get_authenticated_client()
        if sb_client is None:
            flash("You must be logged in to view this page.", "warning")
            return redirect(url_for('login'))
        # Pass the authenticated client to the decorated route function
        return f(sb_client, *args, **kwargs)
    return decorated_function

# --- Unified Authentication Logic ---
def _process_firebase_token():
    """Helper function to verify a Firebase token and set the user session."""
    id_token = request.json.get('token')
    if not id_token:
        return jsonify({"success": False, "error": "No token provided."}), 400

    try:
        decoded_token = auth.verify_id_token(id_token)
        user_result = db.find_or_create_supabase_user(decoded_token)

        if user_result.get('error'):
            return jsonify({"success": False, "error": user_result['error']}), 401

        # Set user data that is always present
        session['user_id'] = user_result.get('user_id')
        session['user_phone'] = user_result.get('phone')

        # Handle Supabase session data if it exists
        if session_data := user_result.get('session'):
            session['access_token'] = session_data.get('access_token')
            session['refresh_token'] = session_data.get('refresh_token')
            session['user_email'] = session_data.get('user', {}).get('email') or user_result.get('email')
        else:
            # Fallback for email if no full Supabase session
            session['user_email'] = user_result.get('email')

        # Final check to ensure a user context was established
        if session.get('user_email') or session.get('user_phone'):
            return jsonify({"success": True})
        else:
            return jsonify({"success": False, "error": "Authentication succeeded but no user context could be established."}), 500

    except Exception as e:
        # Catch specific Firebase auth errors if needed, otherwise generic
        return jsonify({"success": False, "error": f"An unexpected error occurred: {str(e)}"}), 500

# --- Authentication Routes ---
@app.route('/login')
def login():
    """Renders the new unified login page."""
    return render_template('login_unified.html')

@app.route('/cron/bse_announcements')
@app.route('/cron/hourly_spike_alerts')
@app.route('/cron/evening_summary')
@log_errors
def cron_bse_announcements():
    """Cron-compatible endpoint to send BSE announcements.
    Expects a secret key in query string (?key=...) to prevent abuse.
    Optionally accepts hours_back (default 1).

    This endpoint iterates over all users who have both monitored scrips and
    at least one Telegram recipient, and sends consolidated announcements.
    """
    key = request.args.get('key')
    expected = os.environ.get('CRON_SECRET_KEY')
    if not expected or key != expected:
        return "Unauthorized", 403

    # Always use service client for cron
    sb = db.get_supabase_client(service_role=True)
    if not sb:
        return "Supabase not configured", 500

    try:
        # Allow overriding hours_back via query param (default: 1 hour)
        try:
            hours_back = int(request.args.get('hours_back', '1'))
        except Exception:
            hours_back = 1

        # Fetch all scrips and recipients once
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id').execute().data or []

        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({'bse_code': r.get('bse_code'), 'company_name': r.get('company_name')})

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({'chat_id': r.get('chat_id')})

        totals = {"users_processed": 0, "notifications_sent": 0, "users_skipped": 0, "recipients": 0, "items": 0}
        errors = []

        import uuid
        run_id = str(uuid.uuid4())
        job_name = 'hourly_spike_alerts' if request.path.endswith('/hourly_spike_alerts') else 'bse_announcements'

        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if not scrips or not recipients:
                totals["users_skipped"] += 1
                try:
                    # Ensure user_id is a valid UUID
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': job_name,
                        'user_id': user_uuid,
                        'processed': False,
                        'notifications_sent': 0,
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    logging.error(f"Failed to log skipped cron run: {e}")
                continue
            try:
                # Decide which job to run based on path
                if request.path.endswith('/hourly_spike_alerts'):
                    sent = db.send_hourly_spike_alerts(sb, uid, scrips, recipients)
                elif request.path.endswith('/evening_summary'):
                    # Enforce evening run by default; allow override with force=true
                    force = request.args.get('force') == 'true'
                    is_open, open_dt, close_dt = db.ist_market_window()
                    from datetime import datetime
                    now = db.ist_now()
                    if (now <= close_dt) and not force:
                        # Skip if before or during market hours unless forced
                        sent = 0
                    else:
                        # Send price summary instead of announcements
                        sent = db.send_script_messages_to_telegram(sb, uid, scrips, recipients)
                else:
                    sent = db.send_bse_announcements_consolidated(sb, uid, scrips, recipients, hours_back=hours_back)
                totals["users_processed"] += 1
                totals["notifications_sent"] += sent
                totals["recipients"] += len(recipients)
                try:
                    # Ensure user_id is a valid UUID
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': job_name,
                        'user_id': user_uuid,
                        'processed': True,
                        'notifications_sent': int(sent),
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    logging.error(f"Failed to log cron run: {e}")
                # We do not know exact items here, but we can log via BSE_VERBOSE in the function
            except Exception as e:
                errors.append({"user_id": uid, "error": str(e)})

        return jsonify({"ok": True, **totals, "errors": errors})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/verify_phone_token', methods=['POST'])
def verify_phone_token():
    """Endpoint for verifying Firebase phone auth tokens."""
    return _process_firebase_token()

@app.route('/verify_google_token', methods=['POST'])
def verify_google_token():
    """Endpoint for verifying Firebase Google auth tokens."""
    return _process_firebase_token()

@app.route('/logout')
def logout():
    session.clear()
    flash("You have been successfully logged out.", "success")
    return redirect(url_for('login'))

# --- Main Application Routes (Protected) ---
@app.route('/health')
def health_check():
    """Lightweight health check endpoint for uptime monitoring.
    Returns 200 OK with minimal processing to keep the app alive.
    """
    from datetime import datetime
    try:
        # Quick DB connectivity check
        sb = db.get_supabase_client(service_role=True)
        if sb:
            # Very lightweight query
            sb.table('profiles').select('id', count='exact').limit(1).execute()
            db_status = 'connected'
        else:
            db_status = 'disconnected'
    except Exception as e:
        db_status = f'error: {str(e)[:50]}'
    
    return {
        'status': 'ok',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'service': 'bse-monitor',
        'database': db_status,
        'memory_mb': get_memory_usage()
    }, 200

@app.route('/debug/cron_auth')
def debug_cron_auth():
    """Debug endpoint to check cron authentication"""
    key = request.args.get('key')
    expected = os.environ.get('CRON_SECRET_KEY')
    
    return {
        'provided_key': key,
        'expected_key': expected,
        'keys_match': key == expected,
        'expected_exists': expected is not None,
        'provided_exists': key is not None,
        'expected_length': len(expected) if expected else 0,
        'provided_length': len(key) if key else 0
    }

@app.route('/debug/user_setup')
@login_required
def debug_user_setup(sb):
    """Debug endpoint to check user's setup"""
    user_id = session.get('user_id')
    
    # Get user's monitored scrips
    monitored_scrips = db.get_user_scrips(sb, user_id)
    
    # Get user's recipients
    recipients = db.get_user_recipients(sb, user_id)
    
    # Get user's category preferences
    category_prefs = db.get_user_category_prefs(sb, user_id)
    
    return {
        'user_id': user_id,
        'monitored_scrips': monitored_scrips,
        'recipients': recipients,
        'category_preferences': category_prefs,
        'scrip_count': len(monitored_scrips),
        'recipient_count': len(recipients),
        'category_count': len(category_prefs)
    }

@app.route('/debug/cron_logs')
def debug_cron_logs():
    """Debug endpoint to check recent cron job runs"""
    try:
        sb = db.get_supabase_client(service_role=True)
        if not sb:
            return {'error': 'Supabase not configured'}, 500
        
        # Get recent cron runs (last 50, ordered by id desc since created_at doesn't exist)
        result = sb.table('cron_run_logs').select('*').order('id', desc=True).limit(50).execute()
        
        return {
            'success': True,
            'total_runs': len(result.data),
            'recent_runs': result.data
        }
    except Exception as e:
        return {'error': str(e)}, 500

@app.route('/test/evening_summary')
def test_evening_summary():
    """Test endpoint to manually trigger evening summary without secret key"""
    try:
        sb = db.get_supabase_client(service_role=True)
        if not sb:
            return {'error': 'Supabase not configured'}, 500
        
        # Force run evening summary for all users
        from datetime import datetime
        import uuid
        
        run_id = str(uuid.uuid4())
        job_name = 'evening_summary_test'
        
        # Get all users with scrips and recipients
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id').execute().data or []
        
        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({'bse_code': r.get('bse_code'), 'company_name': r.get('company_name')})

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({'chat_id': r.get('chat_id')})

        users_processed = 0
        notifications_sent = 0
        users_skipped = 0
        errors = []

        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if not scrips or not recipients:
                users_skipped += 1
                continue
            try:
                # Send price summary instead of announcements
                sent = db.send_script_messages_to_telegram(sb, uid, scrips, recipients)
                users_processed += 1
                notifications_sent += sent
                
                # Log the run
                try:
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': job_name,
                        'user_id': user_uuid,
                        'processed': True,
                        'notifications_sent': int(sent),
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    errors.append(f"Failed to log for user {uid}: {e}")
                    
            except Exception as e:
                errors.append({"user_id": uid, "error": str(e)})
                users_skipped += 1

        return {
            'success': True,
            'run_id': run_id,
            'job': job_name,
            'timestamp': datetime.now().isoformat(),
            'totals': {
                'users_processed': users_processed,
                'users_skipped': users_skipped,
                'notifications_sent': notifications_sent,
                'errors': errors
            }
        }
    except Exception as e:
        return {'error': str(e)}, 500

def get_memory_usage():
    """Get current memory usage in MB"""
    try:
        import psutil
        import os
        process = psutil.Process(os.getpid())
        return round(process.memory_info().rss / 1024 / 1024, 2)
    except Exception:
        return 'unknown'

@app.route('/')
@login_required
def dashboard(sb):
    """Main dashboard showing monitored scrips and recipients."""
    user_id = session.get('user_id')
    monitored_scrips = db.get_user_scrips(sb, user_id)
    telegram_recipients = db.get_user_recipients(sb, user_id)
    
    category_prefs = db.get_user_category_prefs(sb, user_id)
    return render_template('dashboard.html', 
                           monitored_scrips=monitored_scrips,
                           telegram_recipients=telegram_recipients,
                           category_prefs=category_prefs,
                           user_email=session.get('user_email', ''),
                           user_phone=session.get('user_phone', ''))

@app.route('/search')
@login_required
def search(sb):
    """Endpoint for fuzzy searching company names and BSE codes."""
    query = request.args.get('query', '')
    if not query or len(query) < 2:
        return jsonify({"matches": []})
    
    mask = (company_df['Company Name'].str.contains(query, case=False, na=False)) | \
           (company_df['BSE Code'].str.startswith(query))
           
    matches = company_df[mask].head(10)
    return jsonify({"matches": matches.to_dict('records')})

@app.route('/send_script_messages', methods=['POST'])
@login_required
def send_script_messages(sb):
    """Triggers sending Telegram messages for all monitored scrips."""
    user_id = session.get('user_id')
    try:
        monitored_scrips = db.get_user_scrips(sb, user_id)
        telegram_recipients = db.get_user_recipients(sb, user_id)
        
        if not monitored_scrips:
            flash('No scrips to monitor. Please add scrips first.', 'info')
        elif not telegram_recipients:
            flash('No Telegram recipients found. Please add a recipient first.', 'info')
        else:
            messages_sent = db.send_script_messages_to_telegram(sb, user_id, monitored_scrips, telegram_recipients)
            if messages_sent > 0:
                flash(f'Successfully sent {messages_sent} message(s)!', 'success')
            else:
                flash('No messages were sent. Check scrips and recipients.', 'info')
            
    except Exception as e:
        flash(f'Error sending messages: {str(e)}', 'error')
        print(f"Error in send_script_messages: {e}")
    
    return redirect(url_for('dashboard'))

@app.route('/send_bse_announcements', methods=['POST'])
@login_required
def send_bse_announcements(sb):
    """Send consolidated BSE announcements for monitored scrips to Telegram recipients."""
    user_id = session.get('user_id')
    try:
        monitored_scrips = db.get_user_scrips(sb, user_id)
        telegram_recipients = db.get_user_recipients(sb, user_id)
        hours_back = 24
        try:
            hours_back = int(request.form.get('hours_back', 24))
        except Exception:
            hours_back = 24

        if not monitored_scrips:
            flash('No scrips to monitor. Please add scrips first.', 'info')
        elif not telegram_recipients:
            flash('No Telegram recipients found. Please add a recipient first.', 'info')
        else:
            sent = db.send_bse_announcements_consolidated(sb, user_id, monitored_scrips, telegram_recipients, hours_back=hours_back)
            if sent > 0:
                flash(f'Sent announcements summary to {sent} recipient(s).', 'success')
            else:
                flash('No new announcements found in the selected period.', 'warning')
    except Exception as e:
        flash(f'Error sending BSE announcements: {str(e)}', 'error')
        print(f"Error in send_bse_announcements: {e}")

    return redirect(url_for('dashboard'))

# --- Data Management Routes (Protected) ---
@app.route('/add_scrip', methods=['POST'])
@login_required
def add_scrip(sb):
    user_id = session.get('user_id')
    bse_code = request.form.get('scrip_code')
    company_name = request.form.get('company_name', '').strip()

    if not bse_code:
        flash('Scrip code is required.', 'error')
        return redirect(url_for('dashboard'))

    if not company_name:
        match = company_df[company_df['BSE Code'] == bse_code]
        if not match.empty:
            company_name = str(match.iloc[0]['Company Name'])
        else:
            flash('Scrip code not found. Please check the BSE code.', 'error')
            return redirect(url_for('dashboard'))

    db.add_user_scrip(sb, user_id, bse_code, company_name)
    flash(f'Added {company_name} to your watchlist.', 'success')
    return redirect(url_for('dashboard'))

@app.route('/delete_scrip', methods=['POST'])
@login_required
def delete_scrip(sb):
    user_id = session.get('user_id')
    bse_code = request.form['scrip_code']
    db.delete_user_scrip(sb, user_id, bse_code)
    flash(f'Scrip {bse_code} removed from your watchlist.', 'success')
    return redirect(url_for('dashboard'))

@app.route('/add_recipient', methods=['POST'])
@login_required
def add_recipient(sb):
    user_id = session.get('user_id')
    chat_id = request.form['chat_id']
    db.add_user_recipient(sb, user_id, chat_id)
    flash(f'Added recipient {chat_id}.', 'success')
    return redirect(url_for('dashboard'))

@app.route('/delete_recipient', methods=['POST'])
@login_required
def delete_recipient(sb):
    user_id = session.get('user_id')
    chat_id = request.form['chat_id']
    db.delete_user_recipient(sb, user_id, chat_id)
    flash(f'Recipient {chat_id} removed.', 'success')
    return redirect(url_for('dashboard'))

@app.route('/set_category_prefs', methods=['POST'])
@login_required
def set_category_prefs(sb):
    user_id = session.get('user_id')
    selected = request.form.getlist('categories')
    ok = db.set_user_category_prefs(sb, user_id, selected)
    if ok:
        flash('Category preferences saved.', 'success')
    else:
        flash('Failed to save preferences.', 'error')
    return redirect(url_for('dashboard'))

# --- Sentiment Analysis Routes (Protected) ---
@app.route('/sentiment_analysis')
@login_required
def sentiment_analysis(sb):
    """Renders the sentiment analysis dashboard."""
    user_id = session.get('user_id')
    monitored_scrips = db.get_user_scrips(sb, user_id)
    return render_template('sentiment_analysis.html', 
                         scrips=monitored_scrips,
                         user_email=session.get('user_email'))

@app.route('/analyze_sentiment', methods=['POST'])
@login_required
def analyze_sentiment(sb):
    """API endpoint to analyze sentiment for a specific stock."""
    try:
        data = request.get_json()
        stock_symbol = data.get('stock_symbol')
        company_name = data.get('company_name')
        hours_back = data.get('hours_back', 24)
        
        if not stock_symbol or not company_name:
            return jsonify({'error': 'Stock symbol and company name required'}), 400
        
        sentiment_result = get_sentiment_analysis_for_stock(stock_symbol, company_name, hours_back)
        visualizations = create_sentiment_visualizations(sentiment_result)
        
        return jsonify({
            'success': True,
            'sentiment_data': sentiment_result,
            'visualizations': visualizations
        })
    except Exception as e:
        print(f"Error in analyze_sentiment: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_sentiment_summary')
@login_required
def get_sentiment_summary(sb):
    """API endpoint for a quick sentiment summary of monitored scrips."""
    try:
        user_id = session.get('user_id')
        monitored_scrips = db.get_user_scrips(sb, user_id)
        
        summary_data = []
        for scrip in monitored_scrips[:5]:  # Limit to 5 for performance
            try:
                result = get_sentiment_analysis_for_stock(
                    scrip['bse_code'], scrip['company_name'], hours_back=6)
                summary_data.append({
                    'bse_code': scrip['bse_code'],
                    'company_name': scrip['company_name'],
                    'sentiment_score': result['average_sentiment'],
                    'mood': result['summary']['overall_mood'],
                    'confidence': result['summary']['confidence'],
                    'data_points': result['total_data_points']
                })
            except Exception as e:
                print(f"Error analyzing summary for {scrip.get('company_name', 'N/A')}: {e}")
                continue
        
        return jsonify({'success': True, 'summary_data': summary_data})
    except Exception as e:
        print(f"Error in get_sentiment_summary: {e}")
        return jsonify({'error': str(e)}), 500

# --- Health Check ---
@app.route('/health')
def health():
    return 'ok', 200

# --- Main Execution ---
if __name__ == '__main__':
    db.initialize_firebase()
    port = int(os.environ.get('PORT', os.environ.get('FLASK_RUN_PORT', 5000)))
    debug = os.environ.get('FLASK_DEBUG', '0') == '1'
    app.run(host='0.0.0.0', port=port, debug=debug)
