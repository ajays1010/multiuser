#!/usr/bin/env python3
"""
Cron runner for BSE announcements.

Runs the same logic as the /cron/bse_announcements endpoint but without HTTP,
so it can be invoked by Render Cron Jobs, Windows Task Scheduler, or crontab.

Exit codes:
  0 -> success
  1 -> configuration missing
  2 -> runtime error
"""
import os
import sys
from dotenv import load_dotenv
load_dotenv()

import database as db


def run(hours_back: int = 1) -> int:
    # Always use service client for batch jobs
    sb = db.get_supabase_client(service_role=True)
    if not sb:
        print("ERROR: Supabase not configured. Set SUPABASE_URL and SUPABASE_SERVICE_KEY.")
        return 1

    try:
        try:
            hours_back = int(hours_back)
        except Exception:
            hours_back = 1

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

        print(f"Starting BSE announcements run (hours_back={hours_back}) for {len(scrips_by_user)} users with scrips...")

        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if not scrips or not recipients:
                users_skipped += 1
                continue
            try:
                sent = db.send_bse_announcements_consolidated(sb, uid, scrips, recipients, hours_back=hours_back)
                users_processed += 1
                notifications_sent += sent
                print(f"  user {uid}: sent={sent}")
            except Exception as e:
                errors.append({"user_id": uid, "error": str(e)})
                print(f"  ERROR user {uid}: {e}")

        print("Run complete:")
        print({
            "users_processed": users_processed,
            "users_skipped": users_skipped,
            "notifications_sent": notifications_sent,
            "errors": errors,
        })
        return 0
    except Exception as e:
        print(f"FATAL: {e}")
        return 2


if __name__ == '__main__':
    hb = os.environ.get('BSE_CRON_HOURS_BACK', '1')
    if len(sys.argv) > 1:
        hb = sys.argv[1]
    sys.exit(run(hb))
