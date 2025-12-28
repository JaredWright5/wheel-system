from loguru import logger
from packages.core.src.alerts.emailer import send_email

def main():
    # TODO: replace with real weekly run output once screener is implemented
    subject = "Wheel v1 — Weekly Screener (stub)"
    body = "\n".join([
        "Weekly screener ran successfully (stub).",
        "",
        "Next: implement universe -> gates -> scoring -> trade_recos.",
    ])
    send_email(subject, body)
    logger.info("weekly_screener completed.")

if __name__ == "__main__":
    main()

