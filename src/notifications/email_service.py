import json
import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import List, Dict, Any

from jinja2 import Environment, FileSystemLoader, TemplateNotFound

from utils.logger import setup_logger

logger = setup_logger()


class EmailService:
    """Handles email notifications for FDA recalls."""

    def __init__(self, config_path: str = None):
        """
        Initialize EmailService.

        Args:
            config_path: Path to email_settings.json. Defaults to config/email_settings.json
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "email_settings.json"
        else:
            config_path = Path(config_path)

        self.config = self._load_config(config_path)
        self._setup_templates()

    def _load_config(self, config_path: Path) -> Dict[str, Any]:
        """Load email configuration from JSON file."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.debug("Email config loaded successfully")
            return config
        except Exception as e:
            logger.error(f"Failed to load email config: {e}")
            raise

    def _setup_templates(self):
        """Setup Jinja2 template environment."""
        template_dir = Path(__file__).parent / "templates"
        self.jinja_env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=True
        )

    def _get_smtp_credentials(self) -> tuple:
        """Get SMTP credentials from environment or config."""
        host = os.getenv('SMTP_HOST', self.config['smtp']['host'])
        port = int(os.getenv('SMTP_PORT', self.config['smtp']['port']))
        username = os.getenv('SMTP_USERNAME', self.config['sender']['email'])
        password = os.getenv('SMTP_PASSWORD', '')

        return host, port, username, password

    def _render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """Render a Jinja2 template with context."""
        try:
            template = self.jinja_env.get_template(template_name)
            return template.render(**context)
        except TemplateNotFound as e:
            logger.error(f"Template not found: {template_name}")
            raise

    def _send_email(self, subject: str, html_body: str, text_body: str,
                    recipients: List[str]) -> bool:
        """
        Send an email with retry logic.

        Args:
            subject: Email subject
            html_body: HTML content
            text_body: Plain text content (fallback)
            recipients: List of recipient email addresses

        Returns:
            True if email was sent successfully
        """
        if not self.config['notification_settings']['enabled']:
            logger.info("Notifications disabled in config")
            return True

        valid_recipients = [r for r in recipients if '@' in r]
        if not valid_recipients:
            logger.warning("No valid recipients found")
            return False

        host, port, username, password = self._get_smtp_credentials()
        max_retries = self.config['notification_settings']['max_retries']

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"{self.config['sender']['name']} <{self.config['sender']['email']}>"
        msg['To'] = ', '.join(valid_recipients)

        # Attach plain text first, then HTML (email clients prefer last)
        msg.attach(MIMEText(text_body, 'plain', 'utf-8'))
        msg.attach(MIMEText(html_body, 'html', 'utf-8'))

        for attempt in range(1, max_retries + 1):
            try:
                logger.debug(f"SMTP connection attempt {attempt}/{max_retries}")

                with smtplib.SMTP(host, port, timeout=30) as server:
                    if self.config['smtp']['use_tls']:
                        server.starttls()

                    if password:
                        server.login(username, password)

                    server.sendmail(
                        self.config['sender']['email'],
                        valid_recipients,
                        msg.as_string()
                    )

                logger.info(f"Email sent successfully to {len(valid_recipients)} recipients")
                return True

            except smtplib.SMTPAuthenticationError as e:
                logger.error(f"SMTP authentication failed: {e}")
                return False  # Don't retry auth errors

            except (smtplib.SMTPException, ConnectionError, TimeoutError) as e:
                logger.warning(f"SMTP attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All {max_retries} SMTP attempts failed")
                    return False

        return False

    def send_class1_alert(self, recalls: List[Dict[str, Any]]) -> bool:
        """
        Send Class I recall alert email.

        Args:
            recalls: List of recall dictionaries

        Returns:
            True if email was sent successfully
        """
        if not recalls:
            logger.debug("No recalls to notify")
            return True

        context = {
            'recalls': recalls,
            'count': len(recalls)
        }

        try:
            html_body = self._render_template('class1_alert.html', context)
            text_body = self._render_template('class1_alert.txt', context)
        except TemplateNotFound:
            logger.warning("Template not found, using plain text fallback")
            text_body = self._generate_fallback_text(recalls)
            html_body = f"<pre>{text_body}</pre>"

        subject = f"[URGENT] {len(recalls)} New FDA Class I Recall(s) Detected"
        recipients = self.config['recipients']['class1_alerts']

        return self._send_email(subject, html_body, text_body, recipients)

    def _generate_fallback_text(self, recalls: List[Dict[str, Any]]) -> str:
        """Generate fallback plain text when templates are unavailable."""
        lines = ["FDA CLASS I RECALL ALERT", "=" * 40, ""]

        for recall in recalls:
            lines.extend([
                f"Recall Number: {recall.get('recall_number', 'N/A')}",
                f"Product: {recall.get('product_description', 'N/A')}",
                f"Reason: {recall.get('reason_for_recall', 'N/A')}",
                f"Firm: {recall.get('recalling_firm', 'N/A')}",
                f"Distribution: {recall.get('distribution_pattern', 'N/A')}",
                "-" * 40, ""
            ])

        return "\n".join(lines)
