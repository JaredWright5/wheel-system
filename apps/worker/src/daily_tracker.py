from loguru import logger
from packages.core.src.alerts.emailer import send_email

def main():
    # TODO: replace with real MTM + premium-capture summary once tracker is implemented
    subject = "Wheel v1 — Daily Tracker (stub)"
    body = "\n".join([
        "Daily tracker ran successfully (stub).",
        "",
        "Next: pull Schwab executions + positions, store MTM, compute premium capture.",
    ])
    send_email(subject, body)
    logger.info("daily_tracker completed.")

if __name__ == "__main__":
    main()

