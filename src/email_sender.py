from markdown_it import MarkdownIt
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Content, MimeType

from src.config import FROM_EMAIL, SENDGRID_API_KEY


def _markdown_to_html(text: str) -> str:
    """Convert markdown insights text to HTML."""
    md = MarkdownIt()
    return md.render(text)


def _build_html_email(client_name: str, insights_text: str) -> str:
    body = _markdown_to_html(insights_text)
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
    h2 {{ color: #1a1a2e; border-bottom: 1px solid #e0e0e0; padding-bottom: 6px; }}
    h3 {{ color: #333; }}
    hr {{ border: none; border-top: 1px solid #e0e0e0; margin: 16px 0; }}
    .footer {{ background: #f5f5f5; color: #888; font-size: 12px; text-align: center;
               padding: 12px; border: 1px solid #e0e0e0; border-top: none;
               border-radius: 0 0 6px 6px; }}
  </style>
</head>
<body>
  <div class="header">
    <h1>Weekly Insights for {client_name}</h1>
    <p>Your Clearpath retail analytics report</p>
  </div>
  <div class="content">
    {body}
  </div>
  <div class="footer">Powered by Clearpath</div>
</body>
</html>"""


def send_weekly_insights(client_name: str, client_email: str, insights_text: str) -> bool:
    """
    Send the weekly insights email to a client via SendGrid.

    Returns True on success, False on failure.
    """
    missing = []
    if not SENDGRID_API_KEY:
        missing.append("SENDGRID_API_KEY")
    if not FROM_EMAIL:
        missing.append("FROM_EMAIL")
    if not client_email:
        missing.append("REPORT_RECIPIENT_EMAIL")
    if missing:
        print(f"Email step skipped: missing env var(s): {', '.join(missing)}.")
        return False

    subject = f"Weekly Retail Insights for {client_name}"
    html_content = _build_html_email(client_name, insights_text)

    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=client_email,
        subject=subject,
    )
    message.reply_to = "insights@clearpathdata.org"
    message.content = [
        Content(MimeType.text, insights_text),
        Content(MimeType.html, html_content),
    ]

    try:
	print(f"DEBUG SendGrid key ends with: {SENDGRID_API_KEY[-6:]}, length: {len(SENDGRID_API_KEY)}")
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print(f"Weekly insights email sent to {client_email} (status {response.status_code}).")
        return True
    except Exception as e:
        print(f"Failed to send insights email: {e}")
        return False
