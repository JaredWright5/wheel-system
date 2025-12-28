import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from loguru import logger

def send_email(subject: str, body_text: str) -> None:
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASS")
    to_addr = os.getenv("ALERT_EMAIL_TO")
    from_addr = os.getenv("ALERT_EMAIL_FROM") or user

    if not all([host, port, user, password, to_addr, from_addr]):
        missing = [k for k in ["SMTP_HOST","SMTP_PORT","SMTP_USER","SMTP_PASS","ALERT_EMAIL_TO","ALERT_EMAIL_FROM"]
                   if not os.getenv(k) and not (k=="ALERT_EMAIL_FROM" and os.getenv("SMTP_USER"))]
        raise RuntimeError(f"Missing SMTP env vars: {missing}")

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.attach(MIMEText(body_text, "plain"))

    logger.info(f"Sending email to {to_addr} via {host}:{port} ...")
    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(user, password)
        server.sendmail(from_addr, [to_addr], msg.as_string())
    logger.info("Email sent.")

