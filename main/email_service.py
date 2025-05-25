"""
Email service module for ETL pipeline notifications.

This module provides a robust email notification system that supports:
- Multiple email recipients
- HTML email templates
- Success/failure notifications
- Detailed pipeline execution reports
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging
from typing import Dict, List, Union, Any, Optional
from config.config import (
    SMTP_CREDENTIALS_VAR,
    ETL_EMAIL_RECIPIENT_VAR,
    XCOM_TRANSFORMED_ASSET_COUNT,
    XCOM_TRANSFORMED_USER_COUNT,
    XCOM_LOAD_SUMMARY_REPORT
)

try:
    from airflow.models import Variable
except ImportError:
    # For environments where Airflow is not installed
    Variable = None

logger = logging.getLogger(__name__)


class EmailServiceForAirflow:
    """
    Email service for sending ETL pipeline notifications.

    Supports sending emails to multiple recipients with HTML templates
    and comprehensive error handling.
    """

    def __init__(self, smtp_server: str, smtp_port: int, smtp_username: str, smtp_password: str):
        """
        Initialize email service with SMTP configuration.

        Args:
            smtp_server: SMTP server hostname
            smtp_port: SMTP server port
            smtp_username: SMTP authentication username
            smtp_password: SMTP authentication password
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_username = smtp_username
        self.smtp_password = smtp_password

    def _parse_recipients(self, recipient_input: Union[str, List[str]]) -> List[str]:
        """
        Parse recipient input into a list of valid email addresses.

        Args:
            recipient_input: Either a comma-separated string or list of email addresses

        Returns:
            List of valid email addresses
        """
        if isinstance(recipient_input, str):
            recipients = [email.strip()
                          for email in recipient_input.split(',') if email.strip()]
        elif isinstance(recipient_input, list):
            recipients = [email.strip() for email in recipient_input if isinstance(
                email, str) and email.strip()]
        else:
            logger.warning(
                f"Invalid recipient input type: {type(recipient_input)}")
            return []

        # Validate email format (basic validation)
        valid_recipients = []
        for email in recipients:
            if '@' in email and '.' in email:
                valid_recipients.append(email)
            else:
                logger.warning(f"Invalid email format: {email}")

        return valid_recipients

    def send_email(self, recipients: Union[str, List[str]], subject: str, body_html: str) -> bool:
        """
        Send email to multiple recipients efficiently.

        Args:
            recipients: Email recipients (string or list)
            subject: Email subject
            body_html: HTML email body

        Returns:
            True if email sent successfully, False otherwise
        """
        try:
            recipients_list = self._parse_recipients(recipients)

            if not recipients_list:
                logger.error("No valid recipients found")
                return False

            msg = MIMEMultipart()
            msg['From'] = self.smtp_username
            msg['To'] = ', '.join(recipients_list)
            msg['Subject'] = subject
            msg.attach(MIMEText(body_html, 'html'))

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_username, self.smtp_password)
                server.sendmail(self.smtp_username,
                                recipients_list, msg.as_string())

            logger.info(
                f"Email successfully sent to {len(recipients_list)} recipients: {', '.join(recipients_list)}")
            return True

        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP authentication failed: {e}")
            return False
        except smtplib.SMTPRecipientsRefused as e:
            logger.error(f"Recipients refused by SMTP server: {e}")
            return False
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error occurred: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error while sending email: {e}")
            return False

    def _generate_email_template(self, status_config: Dict, pipeline_name: str,
                                 run_id: str, status_upper: str, current_time: str, details_html: str) -> str:
        """
        Generate HTML email template for ETL notifications.

        Args:
            status_config: Status configuration with class, title, and color
            pipeline_name: Name of the ETL pipeline
            run_id: Pipeline run identifier
            status_upper: Uppercase status string
            current_time: Formatted current time
            details_html: Additional HTML details

        Returns:
            Complete HTML email template
        """
        return f"""  
        <!DOCTYPE html>
        <html lang="id">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Notifikasi Pipeline ETL</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    margin: 0;
                    padding: 0;
                    background-color: #f4f4f4;
                    color: #333333;
                    -webkit-font-smoothing: antialiased;
                    -moz-osx-font-smoothing: grayscale;
                }}
                .email-container {{
                    max-width: 680px;
                    margin: 20px auto;
                    background-color: #ffffff;
                    border: 1px solid #e0e0e0;
                    border-radius: 8px;
                    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
                    overflow: hidden;
                }}
                .header {{
                    padding: 25px 30px;
                    text-align: left;
                    color: #ffffff;
                    border-bottom: 4px solid;
                    background-color: {status_config['color']};
                }}
                .header h2 {{
                    margin: 0;
                    font-size: 22px;
                    font-weight: 600;
                }}
                .content {{
                    padding: 25px 30px;
                }}
                .info-table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-bottom: 25px;
                }}
                .info-table td {{
                    padding: 12px 0;
                    font-size: 15px;
                    border-bottom: 1px solid #f0f0f0;
                }}
                .info-table tr:last-child td {{
                    border-bottom: none;
                }}
                .info-table td:first-child {{
                    font-weight: 600;
                    color: #4A5568;
                    width: 140px;
                }}
                .status-badge {{
                    padding: 6px 12px;
                    border-radius: 16px;
                    font-size: 13px;
                    font-weight: 700;
                    color: #fff;
                    display: inline-block;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    background-color: {status_config['color']};
                }}
                .details-section {{
                    margin-top: 25px;
                    padding-top: 25px;
                    border-top: 1px solid #e0e0e0;
                }}
                .details-section h4 {{
                    font-size: 17px;
                    color: #2c3e50;
                    margin-top: 0;
                    margin-bottom: 15px;
                }}
                .details-section ul {{
                    list-style-type: none;
                    padding-left: 0;
                    margin: 0;
                }}
                .details-section ul li {{
                    background-color: #f8f9fa;
                    border: 1px solid #e9ecef;
                    padding: 12px 18px;
                    margin-bottom: 10px;
                    border-radius: 6px;
                    font-size: 14px;
                    line-height: 1.6;
                }}
                .details-section ul li strong {{
                    color: #0056b3;
                }}
                .details-section p {{
                    font-size: 14px;
                    line-height: 1.6;
                    margin-top: 15px;
                }}
                .footer {{
                    background-color: #f1f3f5;
                    padding: 20px 30px;
                    text-align: center;
                    font-size: 12px;
                    color: #6c757d;
                    border-top: 1px solid #e0e0e0;
                }}
                .footer p {{
                    margin: 5px 0;
                }}
                a {{
                    color: #007bff;
                    text-decoration: none;
                }}
                a:hover {{
                    text-decoration: underline;
                }}
            </style>
        </head>
        <body>
            <div class="email-container">
                <div class="header">
                    <h2>{status_config['title']}</h2>
                </div>
                <div class="content">
                    <table class="info-table">
                        <tr>
                            <td>Pipeline:</td>
                            <td><strong>{pipeline_name}</strong></td>
                        </tr>
                        <tr>
                            <td>Run ID:</td>
                            <td>{run_id}</td>
                        </tr>
                        <tr>
                            <td>Status:</td>
                            <td><span class="status-badge">{status_upper}</span></td>
                        </tr>
                        <tr>
                            <td>Waktu Eksekusi:</td>
                            <td>{current_time}</td>
                        </tr>
                    </table>

                    <div class="details-section">
                        {details_html if details_html else "<p>Tidak ada detail tambahan yang tersedia.</p>"}
                    </div>
                </div>
                <div class="footer">
                    <p>Ini adalah notifikasi otomatis dari sistem ETL Airflow Anda.</p>
                    <p>&copy; {datetime.now().year} Perusahaan Anda. Hak Cipta Dilindungi.</p>
                </div>
            </div>
        </body>
        </html>
        """

    def send_etl_notification(self, recipients: Union[str, List[str]], subject: str,
                              pipeline_name: str, status: str, run_id: str, details_html: str = "") -> bool:
        """
        Send ETL pipeline notification email with optimized multiple recipient support.

        Args:
            recipients: Email recipients (string or list)
            subject: Email subject
            pipeline_name: Name of the ETL pipeline
            status: Pipeline execution status (SUCCESS, FAILURE, etc.)
            run_id: Pipeline run identifier
            details_html: Additional HTML details for the email body

        Returns:
            True if email sent successfully, False otherwise
        """
        current_time = datetime.now().strftime("%H:%M:%S %d-%m-%Y")
        status_upper = status.upper()

        # Determine CSS class and header title based on status
        status_mapping = {
            "SUCCESS": {
                "class": "SUCCESS",
                "title": "Pipeline Berhasil Dijalankan",
                "color": "#28a745"
            },
            "FAILURE": {
                "class": "FAILURE",
                "title": "Pipeline Mengalami Kegagalan",
                "color": "#dc3545"
            }
        }

        status_config = status_mapping.get(status_upper, {
            "class": "DEFAULT",
            "title": f"Status Pipeline: {status_upper}",
            "color": "#007bff"
        })

        body_html_content = self._generate_email_template(
            status_config, pipeline_name, run_id, status_upper, current_time, details_html
        )

        return self.send_email(recipients, subject, body_html_content)

    def send_bulk_notifications(self, recipient_groups: Dict[str, List[str]],
                                subject: str, pipeline_name: str, status: str, run_id: str,
                                details_html: str = "") -> Dict[str, bool]:
        """
        Send notifications to multiple recipient groups efficiently.

        Args:
            recipient_groups: Dictionary mapping group names to recipient lists
            subject: Email subject
            pipeline_name: Name of the ETL pipeline  
            status: Pipeline execution status
            run_id: Pipeline run identifier
            details_html: Additional HTML details

        Returns:
            Dictionary mapping group names to success status
        """
        results = {}

        for group_name, recipients in recipient_groups.items():
            logger.info(
                f"Sending notification to group '{group_name}' with {len(recipients)} recipients")
            success = self.send_etl_notification(
                recipients, subject, pipeline_name, status, run_id, details_html)
            results[group_name] = success

        return results


def get_email_service_instance() -> Optional[EmailServiceForAirflow]:
    """
    Helper to create and configure EmailServiceForAirflow instance.

    Returns:
        EmailServiceForAirflow instance or None if configuration fails
    """
    email_logger = logging.getLogger("airflow.task.email_setup")
    try:
        if Variable is None:
            email_logger.error("Airflow Variable module not available")
            return None

        smtp_config = Variable.get(SMTP_CREDENTIALS_VAR, deserialize_json=True)
        return EmailServiceForAirflow(
            smtp_server=smtp_config["smtp_server"],
            smtp_port=int(smtp_config["smtp_port"]),
            smtp_username=smtp_config["smtp_username"],
            smtp_password=smtp_config["smtp_password"]
        )
    except Exception as e:
        email_logger.error(
            f"Failed to get SMTP credentials from Variable '{SMTP_CREDENTIALS_VAR}': {e}")
        return None


def get_recipients_list() -> List[str]:
    """
    Get and validate email recipients from Airflow Variables.

    Returns:
        List of validated email addresses
    """
    email_logger = logging.getLogger("airflow.task.recipients")

    try:
        recipients_str = Variable.get(ETL_EMAIL_RECIPIENT_VAR, default_var="")
        recipients_list = [email.strip()
                           for email in recipients_str.split(',') if email.strip()]

        if not recipients_list:
            email_logger.warning(
                f"No recipients configured in Variable '{ETL_EMAIL_RECIPIENT_VAR}'. "
                f"Using default fallback email."
            )
            recipients_list = ["rizkyyanuarkristianto@gmail.com"]
        else:
            # Log the number of recipients without exposing emails in logs
            email_logger.info(
                f"Found {len(recipients_list)} configured recipients")

    except Exception as e:
        email_logger.error(
            f"Failed to get recipients from Variable '{ETL_EMAIL_RECIPIENT_VAR}': {e}")
        recipients_list = ["rizkyyanuarkristianto@gmail.com"]

    return recipients_list


def send_etl_status_email(context: Dict[str, Any], status: str) -> bool:
    """
    Generic function to send ETL status email with optimized multiple recipient handling.

    Args:
        context: The Airflow task context
        status: The status of the ETL process ("SUCCESS" or "FAILURE")

    Returns:
        True if email sent successfully, False otherwise
    """
    ti = context['ti']
    dag_run = context.get('dag_run')
    run_id = dag_run.run_id if dag_run else ti.run_id
    dag_id = ti.dag_id
    task_instance = context.get('task_instance')

    email_logger = logging.getLogger("airflow.task.send_etl_email")

    # Initialize email service
    email_service_instance = get_email_service_instance()
    if not email_service_instance:
        email_logger.error(
            "Email service instance could not be created. Skipping email notification.")
        return False

    # Get recipients
    recipients_list = get_recipients_list()

    # Prepare email content
    subject_status = "BERHASIL" if status.upper() == "SUCCESS" else "GAGAL"
    subject = f'Airflow ETL: {dag_id} - {subject_status} - Run ID: {run_id}'

    details_html_content = _generate_details_html(
        ti, status, context, task_instance)

    # Send email to all recipients
    success = email_service_instance.send_etl_notification(
        recipients=recipients_list,
        subject=subject,
        pipeline_name=dag_id,
        status=status,
        run_id=run_id,
        details_html=details_html_content
    )

    if success:
        email_logger.info(
            f"ETL status email sent successfully to {len(recipients_list)} recipients")
    else:
        email_logger.error("Failed to send ETL status email")

    return success


def _generate_details_html(ti: Any, status: str, context: Dict[str, Any], task_instance: Any) -> str:
    """
    Generate HTML details content for email notifications.

    Args:
        ti: Task instance
        status: Pipeline status
        context: Airflow context
        task_instance: Task instance from context

    Returns:
        HTML content string
    """
    details_html_content = ""

    if status.upper() == "SUCCESS":
        new_asset_count_val = ti.xcom_pull(
            task_ids='transform_asset_data', key=XCOM_TRANSFORMED_ASSET_COUNT)
        new_user_count_val = ti.xcom_pull(
            task_ids='transform_user_data', key=XCOM_TRANSFORMED_USER_COUNT)
        load_summary_val = ti.xcom_pull(
            task_ids='load', key=XCOM_LOAD_SUMMARY_REPORT)

        details_html_content = "<h4>Ringkasan Data Baru yang Diproses dari Sumber:</h4><ul>"
        details_html_content += f"<li><strong>Data Aset Baru dari Sumber:</strong> {new_asset_count_val if new_asset_count_val is not None else 'Tidak ada'} baris.</li>"
        details_html_content += f"<li><strong>Data User Baru dari Sumber:</strong> {new_user_count_val if new_user_count_val is not None else 'Tidak ada'} baris.</li></ul>"

        details_html_content += "<h4>Ringkasan Data yang Dimuat:</h4><ul>"
        if load_summary_val and isinstance(load_summary_val, dict):
            if not load_summary_val:
                details_html_content += "<li>Tidak ada data yang dimuat pada run ini.</li>"
            else:
                for table_name, summary_details in load_summary_val.items():
                    if isinstance(summary_details, dict):
                        rows = summary_details.get("rows_processed", "N/A")
                        drive_status = summary_details.get(
                            "drive_upload_status", "N/A")
                        supabase_status = summary_details.get(
                            "supabase_load_status", "N/A")
                        details_html_content += (
                            f"<li><strong>Tabel {table_name}:</strong> "
                            f"Baris diproses: {rows}, "
                            f"Status Upload Drive: {drive_status}, "
                            f"Status Load Supabase: {supabase_status}</li>"
                        )
                    else:
                        details_html_content += f"<li><strong>Tabel {table_name}:</strong> {summary_details}</li>"
        else:
            details_html_content += "<li>Ringkasan load tidak tersedia atau format tidak sesuai.</li>"
        details_html_content += "</ul>"

    elif status.upper() == "FAILURE":
        exception_info = context.get('exception')
        failed_task_id = task_instance.task_id if task_instance else "N/A"
        log_url = task_instance.log_url if task_instance else "#"

        details_html_content = f"""
        <h4>Detail Kegagalan:</h4>
        <ul>
            <li><strong>Task Gagal:</strong> {failed_task_id}</li>
            <li><strong>Pesan Error:</strong> <pre>{exception_info}</pre></li>
        </ul>
        <p>Silakan periksa log untuk detail lebih lanjut: <a href="{log_url}" target="_blank" rel="noopener noreferrer">Lihat Log Task</a></p>
        """

    return details_html_content


def send_dag_success_notification_task(**kwargs) -> bool:
    """
    Callable for PythonOperator to send success email for the entire DAG.

    Args:
        **kwargs: Keyword arguments passed by Airflow (contains task context)

    Returns:
        True if email sent successfully, False otherwise
    """
    return send_etl_status_email(context=kwargs, status="SUCCESS")


def send_task_failure_notification_callback(context: Dict[str, Any]) -> bool:
    """
    Callable for on_failure_callback to send failure email when a task fails.

    Args:
        context: The Airflow task context

    Returns:
        True if email sent successfully, False otherwise
    """
    return send_etl_status_email(context=context, status="FAILURE")
