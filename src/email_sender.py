import resend

from pathlib import Path

from src.config import FROM_EMAIL, REPLY_TO_EMAIL, RESEND_API_KEY

INSIGHTS_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "insights_email.html"
ERROR_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "error_email.html"

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

_ERROR_FALLBACK_HTML = """\
<html><body style="font-family:Arial,sans-serif;color:#222;max-width:600px;margin:auto;padding:24px;">
<h1 style="color:#112b50;">We couldn't process your file</h1>
<p>Hi {{client_name}},</p>
<p>We received your upload, but we weren't able to generate your insights. Here's what went wrong:</p>
<p style="background:#fdf2f2;border-left:4px solid #d64545;padding:12px 16px;color:#7a1f1f;">{{error_message}}</p>
<p>Your CSV should have exactly these columns: <code>{{required_columns}}</code></p>
<p>Please fix the file and upload it again. If you keep seeing this, just reply to this email and we'll help.</p>
<p><a href="{{cta_url}}">{{cta_label}}</a></p>
<p style="color:#888;font-size:12px;">Powered by Clearpath</p>
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
        "top2_name": report.get("top2_name", ""),
        "top2_units": str(report.get("top2_units", "")),
        "top2_revenue": report.get("top2_revenue", ""),
        "top3_name": report.get("top3_name", ""),
        "top3_units": str(report.get("top3_units", "")),
        "top3_revenue": report.get("top3_revenue", ""),
        "slow_product_name": report.get("slow_product_name", ""),
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
    values = {
        "client_name": client_name or "there",
        "error_message": error_message or "",
        "required_columns": ", ".join(["date", "product_name", "category", "size", "quantity", "price"]),
        "cta_url": "https://app.clearpathdata.org",
        "cta_label": "Open your portal",
    }
    try:
        html = ERROR_TEMPLATE_PATH.read_text(encoding="utf-8")
    except Exception:
        html = _ERROR_FALLBACK_HTML
    for key, value in values.items():
        html = html.replace("{{" + key + "}}", str(value))
    return html


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


ALERT_RECIPIENT_EMAIL = "contact@clearpathdata.org"


def send_pipeline_alert(error, client_name=None, s3_key=None):
    """
    Email the operator when the pipeline fails with an unexpected error.
    Includes client, file, error type/message, and full traceback so the
    failure can be diagnosed. Never raises: if the alert can't be sent, it
    just logs, so it never masks the original error.
    """
    import traceback

    try:
        if not RESEND_API_KEY or not FROM_EMAIL:
            print("Pipeline alert skipped: missing RESEND_API_KEY or FROM_EMAIL.")
            return

        who = client_name or "unknown client"
        where = s3_key or "unknown file"
        err_type = type(error).__name__
        err_msg = str(error)
        tb = "".join(traceback.format_exception(type(error), error, error.__traceback__))

        subject = f"[Clearpath ALERT] Pipeline failed for {who}"
        text_content = (
            "The Clearpath pipeline failed while processing an upload.\n\n"
            f"Client: {who}\n"
            f"File: {where}\n"
            f"Error type: {err_type}\n"
            f"Error message: {err_msg}\n\n"
            "Full traceback:\n"
            f"{tb}\n"
            "Check the CloudWatch logs for the function clearpath-pipeline for more detail."
        )

        resend.api_key = RESEND_API_KEY
        resend.Emails.send({
            "from": FROM_EMAIL,
            "to": ALERT_RECIPIENT_EMAIL,
            "subject": subject,
            "text": text_content,
            "reply_to": REPLY_TO_EMAIL,
        })
        print(f"Pipeline alert sent to {ALERT_RECIPIENT_EMAIL} for {who}.")
    except Exception as alert_err:
        # Never let the alert itself break anything.
        print(f"Failed to send pipeline alert: {alert_err}")