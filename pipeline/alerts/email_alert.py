"""
alerts/email_alert.py

Sends an HTML digest email when new 🔥 Must Tour listings appear.
Uses Gmail SMTP + App Password — no OAuth, no third-party service.

Setup:
  1. Enable 2-Factor Authentication on your Google account
  2. Google Account → Security → App Passwords → create one named "Apt Hunter"
  3. Store the 16-char password as ALERT_EMAIL_PASSWORD in GitHub secrets
"""

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

log = logging.getLogger(__name__)

FROM_EMAIL = os.environ.get("ALERT_EMAIL", "")
APP_PASSWORD = os.environ.get("ALERT_EMAIL_PASSWORD", "")
TO_EMAIL = os.environ.get("ALERT_EMAIL", "")  # same address — sends to yourself


def send_must_tour_alert(listings: list[dict]):
    if not FROM_EMAIL or not APP_PASSWORD:
        log.warning("Alert email credentials not set — skipping")
        return
    if not listings:
        return

    n = len(listings)
    subject = f"🔥 {n} Must-Tour Apt{'s' if n > 1 else ''} Just Listed — Act Now"
    html = _build_html(listings)

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = FROM_EMAIL
        msg["To"] = TO_EMAIL
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(FROM_EMAIL, APP_PASSWORD)
            server.sendmail(FROM_EMAIL, TO_EMAIL, msg.as_string())

        log.info(f"  Alert sent — {n} Must Tour listing(s)")
    except Exception as e:
        log.error(f"  Alert email failed: {e}")


def _build_html(listings: list[dict]) -> str:
    rows = ""
    for l in listings:
        price = f"${l.get('price', 0):,}"
        addr = l.get("address_normalized") or l.get("address") or "—"
        unit = l.get("unit", "")
        neigh = l.get("neighborhood", "")
        score = l.get("score_raw", 0)
        commute = l.get("commute_minutes", "?")
        sqft = l.get("sqft", "?")
        url = l.get("primary_url", "#")
        beds = l.get("bedrooms", "?")
        baths = l.get("bathrooms", "?")
        heat = l.get("heat", "")
        hot_badge = (
            '<span style="background:#fff3e0;color:#bf360c;font-size:11px;padding:2px 6px;border-radius:3px;margin-left:6px">🔥 Hot</span>'
            if heat == "🔥 Hot" else ""
        )
        amenities = ", ".join(filter(None, [
            "Laundry" if l.get("in_unit_laundry") else "",
            "Parking" if l.get("parking") else "",
            "Storage" if l.get("storage") else "",
            "Gym" if l.get("gym") else "",
        ])) or "—"

        rows += f"""
        <tr style="border-bottom:1px solid #f0f0f0">
          <td style="padding:14px 10px;vertical-align:top">
            <div style="font-weight:600;font-size:14px">{addr}{' #'+unit if unit else ''}{hot_badge}</div>
            <div style="color:#666;font-size:12px;margin-top:2px">{neigh}</div>
            <div style="color:#888;font-size:12px;margin-top:4px">+{amenities}</div>
          </td>
          <td style="padding:14px 10px;vertical-align:top;font-weight:700;color:#d84315;font-size:16px;white-space:nowrap">{price}/mo</td>
          <td style="padding:14px 10px;vertical-align:top;font-size:13px">{beds}bd / {baths}ba<br><span style="color:#888">{sqft if sqft != '?' else '?'} sqft</span></td>
          <td style="padding:14px 10px;vertical-align:top;font-size:13px">{commute} min<br><span style="color:#888">to Midtown</span></td>
          <td style="padding:14px 10px;vertical-align:top">
            <div style="background:#fbe9e7;color:#bf360c;font-size:15px;font-weight:700;padding:4px 10px;border-radius:6px;text-align:center">{score}</div>
            <div style="color:#aaa;font-size:10px;text-align:center">/100</div>
          </td>
          <td style="padding:14px 10px;vertical-align:top">
            <a href="{url}" style="color:#1565c0;font-size:13px;text-decoration:none;border:1px solid #1565c0;padding:5px 10px;border-radius:4px;white-space:nowrap">View →</a>
          </td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"/></head>
<body style="margin:0;padding:20px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f5">
  <div style="max-width:680px;margin:0 auto;background:white;border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08)">
    <div style="background:#bf360c;color:white;padding:22px 28px">
      <h1 style="margin:0;font-size:20px;font-weight:600">🔥 Must Tour — New Listings</h1>
      <p style="margin:6px 0 0;opacity:0.85;font-size:14px">{len(listings)} new listing{'s' if len(listings)>1 else ''} match all your must-have criteria</p>
    </div>
    <table style="width:100%;border-collapse:collapse">
      <thead>
        <tr style="background:#fafafa;border-bottom:2px solid #f0f0f0">
          <th style="padding:10px;text-align:left;font-size:11px;color:#999;font-weight:600;text-transform:uppercase">Address</th>
          <th style="padding:10px;text-align:left;font-size:11px;color:#999;font-weight:600;text-transform:uppercase">Price</th>
          <th style="padding:10px;text-align:left;font-size:11px;color:#999;font-weight:600;text-transform:uppercase">Size</th>
          <th style="padding:10px;text-align:left;font-size:11px;color:#999;font-weight:600;text-transform:uppercase">Commute</th>
          <th style="padding:10px;text-align:left;font-size:11px;color:#999;font-weight:600;text-transform:uppercase">Score</th>
          <th style="padding:10px;text-align:left;font-size:11px;color:#999;font-weight:600;text-transform:uppercase"></th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
    <div style="padding:16px 28px;background:#fafafa;border-top:1px solid #f0f0f0;font-size:12px;color:#aaa">
      Sent by Apt Hunter · <a href="#" style="color:#aaa">Open Dashboard</a>
    </div>
  </div>
</body></html>"""
