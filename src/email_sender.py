import resend

from pathlib import Path

from src.config import FROM_EMAIL, REPLY_TO_EMAIL, RESEND_API_KEY

INSIGHTS_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "insights_email.html"

_INSIGHTS_FALLBACK_HTML = """\
<html><body style="font-family:Arial,sans-serif;color:#222;max-width:600px;margin:auto;padding:24px;">
<h1 style="color:#112b50;">Hi {{business_name}}, {{headline}}</h1>
<p><strong>{{top_product_name}}</strong> - {{top_product_units}} units, {{top_product_revenue}} this week.
Total revenue: {{week_revenue}} {{delta_line}}</p>
<h3>Recommended next steps</h3>
<p><strong>{{step1_title}}</strong><br>{{step1_desc}}</p>
<p><strong>{{step2_title}}</strong><br>{{step2_desc}}</p>
<p><strong>{{step3_title}}</strong><br>{{step3_desc}}</p>
<p><a href="{{cta_url}}">{{cta_label}}</a></p>
<p style="color:#888;font-size:12px;">{{week_range}} &middot; Next report: {{next_report}} &middot; Powered by Clearpath</p>
</body></html>"""


def _flatten_report(report):
    steps = list(report.get("steps", []))
    while len(steps) < 3:
        steps.append({"title": "", "description": ""})
    delta = report.get("delta_pct")
    delta_line = "" if delta is None else f"{'▲' if delta >= 0 else '▼'} {abs(delta)}% vs last week"
    return {
        "business_name": report.get("business_name", ""),
        "week_range": report.get("week_range", ""),
        "next_report": report.get("next_report", ""),
        "headline": report.get("headline", ""),
        "top_product_name": report.get("top_product_name", ""),
        "top_product_units": str(report.get("top_product_units", "")),
        "top_product_revenue": report.get("top_product_revenue", ""),
        "week_revenue": report.get("week_revenue", ""),
        "delta_line": delta_line,
        "step1_title": steps[0]["title"], "step1_desc": steps[0]["description"],
        "step2_title": steps[1]["title"], "step2_desc": steps[1]["description"],
        "step3_title": steps[2]["title"], "step3_desc": steps[2]["description"],
        "cta_url": "https://app.clearpathdata.org",
        "cta_label": "Open your portal",
    }


def _render_insights_html(report):
    try:
        html = INSIGHTS_TEMPLATE_PATH.read_text(encoding="utf-8")
    except Exception:
        html = _INSIGHTS_FALLBACK_HTML
    for key, value in _flatten_report(report).items():
        html = html.replace("{{" + key + "}}", str(value))
    return html


def _insights_text(report):
    lines = [f"Hi {report.get('business_name', 'there')}, {report.get('headline', '')}", ""]
    lines.append(f"{report.get('top_product_name', '')}: {report.get('top_product_units', '')} units, "
                 f"{report.get('top_product_revenue', '')} this week.")
    lines.append(f"Total revenue this week: {report.get('week_revenue', '')}")
    lines.append("")
    lines.append("Recommended next steps:")
    for i, s in enumerate(report.get("steps", []), 1):
        lines.append(f"{i}. {s.get('title', '')}: {s.get('description', '')}")
    return "\n".join(lines)


def send_weekly_insights(client_email, report):
    """
    Send the weekly insights email. `report` is the dict built in main.py.
    Returns True on success, False on failure.
    """
    missing = []
    if not RESEND_API_KEY:
        missing.append("RESEND_API_KEY")
    if not FROM_EMAIL:
        missing.append("FROM_EMAIL")
    if not client_email:
        missing.append("REPORT_RECIPIENT_EMAIL")
    if missing:
        print(f"Email step skipped: missing env var(s): {', '.join(missing)}.")
        return False

    subject = f"Weekly Retail Insights for {report.get('business_name', 'your business')}"
    html_content = _render_insights_html(report)
    text_content = _insights_text(report)
    try:
        resend.api_key = RESEND_API_KEY
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": client_email,
            "subject": subject,
            "html": html_content,
            "text": text_content,
            "reply_to": REPLY_TO_EMAIL,
        })
        email_id = response.get("id") if isinstance(response, dict) else None
        print(f"Weekly insights email sent to {client_email}" + (f" (id: {email_id})." if email_id else "."))
        return True
    except Exception as e:
        print(f"Failed to send insights email: {e}")
        return False


def _build_error_email(client_name: str, error_message: str) -> str:
    client_name = client_name or "there"
    required = ", ".join(["date", "product_name", "category", "size", "quantity", "price"])
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ font-family: Arial, sans-serif; color: #222; max-width: 700px; margin: auto; padding: 24px; }}
    .header {{ background: #1a1a2e; color: #fff; padding: 20px 24px; border-radius: 6px 6px 0 0; }}
    .header h1 {{ margin: 0; font-size: 22px; }}
    .header p {{ margin: 4px 0 0; font-size: 14px; color: #aaa; }}
    .content {{ padding: 24px; border: 1px solid #e0e0e0; border-top: none; }}
    .notice {{ background: #fdf2f2; border-left: 4px solid #d64545; padding: 12px 16px;
               border-radius: 4px; color: #7a1f1f; margin: 0 0 16px; }}
    .content p {{ line-height: 1.6; }}
    code {{ background: #f3f3f3; padding: 2px 6px; border-radius: 4px; font-size: 13px; }}
    .footer {{ background: #f5f5f5; color: #888; font-size: 12px; text-align: center;
               padding: 12px; border: 1px solid #e0e0e0; border-top: none;
               border-radius: 0 0 6px 6px; }}
  </style>
</head>
<body>
  <div class="header">
    <h1>We couldn't process your file</h1>
    <p>Clearpath retail analytics</p>
  </div>
  <div class="content">
    <p>Hi {client_name},</p>
    <p>We received your upload, but we weren't able to generate your insights. Here's what went wrong:</p>
    <div class="notice">{error_message}</div>
    <p>Your CSV should have exactly these columns: <code>{required}</code>.</p>
    <p>Please fix the file and upload it again. If you keep seeing this, just reply to this email and we'll help.</p>
  </div>
  <div class="footer">Powered by Clearpath</div>
</body>
</html>"""


def send_csv_error(client_name: str, client_email: str, error_message: str) -> bool:
    """
    Email the client when their uploaded CSV could not be processed.
    Returns True on success, False on failure.
    """
    missing = []
    if not RESEND_API_KEY:
        missing.append("RESEND_API_KEY")
    if not FROM_EMAIL:
        missing.append("FROM_EMAIL")
    if not client_email:
        missing.append("REPORT_RECIPIENT_EMAIL")
    if missing:
        print(f"Error email skipped: missing env var(s): {', '.join(missing)}.")
        return False

    subject = "We couldn't process your Clearpath upload"
    html_content = _build_error_email(client_name, error_message)
    text_content = (
        f"Hi {client_name or 'there'},\n\n"
        "We received your upload, but we weren't able to generate your insights.\n\n"
        f"{error_message}\n\n"
        "Your CSV should have exactly these columns: "
        "date, product_name, category, size, quantity, price.\n\n"
        "Please fix the file and upload it again. If you keep seeing this, reply to this email and we'll help."
    )
    try:
        resend.api_key = RESEND_API_KEY
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": client_email,
            "subject": subject,
            "html": html_content,
            "text": text_content,
            "reply_to": REPLY_TO_EMAIL,
        })
        email_id = response.get("id") if isinstance(response, dict) else None
        if email_id:
            print(f"CSV error email sent to {client_email} (id: {email_id}).")
        else:
            print(f"CSV error email sent to {client_email}.")
        return True
    except Exception as e:
        print(f"Failed to send CSV error email: {e}")
        return False