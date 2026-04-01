import os
import re
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Content, MimeType

load_dotenv()


def _markdown_to_html(text: str) -> str:
    """Convert the markdown-style insights output to clean HTML."""
    lines = text.split("\n")
    html_lines = []

    for line in lines:
        # ## Heading 2
        if line.startswith("## "):
            html_lines.append(f"<h2>{line[3:].strip()}</h2>")
        # ### Heading 3
        elif line.startswith("### "):
            html_lines.append(f"<h3>{line[4:].strip()}</h3>")
        # Horizontal rule
        elif line.strip() == "---":
            html_lines.append("<hr>")
        # Empty line → paragraph break
        elif line.strip() == "":
            html_lines.append("<br>")
        else:
            # **bold** → <strong>bold</strong>
            formatted = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", line)
            html_lines.append(f"<p>{formatted}</p>")

    return "\n".join(html_lines)


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
    api_key = os.getenv("SENDGRID_API_KEY")
    from_email = os.getenv("FROM_EMAIL")

    if not api_key or not from_email:
        print("Email step skipped: SENDGRID_API_KEY and FROM_EMAIL must be set in your environment.")
        return False

    subject = f"Weekly Retail Insights for {client_name}"
    html_content = _build_html_email(client_name, insights_text)

    message = Mail(
        from_email=from_email,
        to_emails=client_email,
        subject=subject,
    )
    message.content = [
        Content(MimeType.text, insights_text),
        Content(MimeType.html, html_content),
    ]

    try:
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        print(f"Weekly insights email sent to {client_email} (status {response.status_code}).")
        return True
    except Exception as e:
        print(f"Failed to send insights email: {e}")
        return False
