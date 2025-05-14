# File: c:\Users\rizky\OneDrive\Dokumen\GitHub\ETL\main\email_service.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging

# Menggunakan logger standar Python dengan nama modul
logger = logging.getLogger(__name__)


class EmailServiceForAirflow:
    def __init__(self, smtp_server, smtp_port, smtp_username, smtp_password):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_username = smtp_username
        self.smtp_password = smtp_password

    def send_email(self, recipient, subject, body_html):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.smtp_username
            msg['To'] = recipient
            msg['Subject'] = subject
            msg.attach(MIMEText(body_html, 'html'))

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_username, self.smtp_password)
                recipients_as_list = [
                    email.strip() for email in recipient.split(',') if email.strip()]
                server.sendmail(self.smtp_username,
                                recipients_as_list, msg.as_string())
            logger.info(
                f"Email berhasil dikirim ke {recipient} dengan subjek '{subject}'")
            return True
        except Exception as e:
            logger.error(f"Gagal mengirim email ke {recipient}: {e}")
            # Anda bisa memilih untuk raise exception di sini jika kegagalan kirim email kritis
            # raise
            return False

    def send_etl_notification(self, recipient: str, subject: str, pipeline_name: str, status: str, run_id: str, details_html: str = ""):
        """
        Mengirim email notifikasi ETL yang lebih generik.
        """
        current_time = datetime.now().strftime("%S:%M:%H %d-%m-%Y")
        status_upper = status.upper()

        # Tentukan kelas CSS dan judul header berdasarkan status
        if status_upper == "SUCCESS":
            status_class = "SUCCESS"
            header_title = "Pipeline Berhasil Dijalankan"
        elif status_upper == "FAILURE":
            status_class = "FAILURE"
            header_title = "Pipeline Mengalami Kegagalan"
        else:
            # Untuk status lain seperti 'RUNNING', 'SKIPPED', dll.
            status_class = "DEFAULT"
            header_title = f"Status Pipeline: {status_upper}"

        # Konten HTML email yang disempurnakan
        body_html_content = f"""  
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
                    max-width: 680px; /* Sedikit lebih lebar untuk konten */
                    margin: 20px auto;
                    background-color: #ffffff;
                    border: 1px solid #e0e0e0;
                    border-radius: 8px;
                    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
                    overflow: hidden; /* Memastikan border-radius diterapkan pada anak elemen */
                }}
                .header {{
                    padding: 25px 30px;
                    text-align: left; /* Judul rata kiri */
                    color: #ffffff;
                    border-bottom: 4px solid; /* Border lebih tebal untuk penekanan */
                }}
                .header.status-SUCCESS {{ background-color: #28a745; border-color: #1e7e34; }} /* Hijau */
                .header.status-FAILURE {{ background-color: #dc3545; border-color: #b02a37; }} /* Merah */
                .header.status-DEFAULT {{ background-color: #007bff; border-color: #0056b3; }} /* Biru untuk status lain */

                .header h2 {{
                    margin: 0;
                    font-size: 22px; /* Sedikit lebih kecil agar terlihat rapi */
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
                    padding: 12px 0; /* Padding lebih besar */
                    font-size: 15px;
                    border-bottom: 1px solid #f0f0f0; /* Pemisah baris yang halus */
                }}
                .info-table tr:last-child td {{
                    border-bottom: none; /* Tidak ada border untuk baris terakhir */
                }}
                .info-table td:first-child {{
                    font-weight: 600;
                    color: #4A5568; /* Abu-abu gelap untuk label */
                    width: 140px; /* Lebar tetap untuk label */
                }}
                .status-badge {{
                    padding: 6px 12px;
                    border-radius: 16px; /* Bentuk pil */
                    font-size: 13px;
                    font-weight: 700; /* Lebih tebal */
                    color: #fff;
                    display: inline-block;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                }}
                .status-badge.status-SUCCESS {{ background-color: #28a745; }}
                .status-badge.status-FAILURE {{ background-color: #dc3545; }}
                .status-badge.status-DEFAULT {{ background-color: #007bff; }}

                .details-section {{
                    margin-top: 25px;
                    padding-top: 25px;
                    border-top: 1px solid #e0e0e0;
                }}
                .details-section h4 {{ /* Styling untuk h4 dari details_html */
                    font-size: 17px;
                    color: #2c3e50; /* Biru-abu gelap */
                    margin-top: 0;
                    margin-bottom: 15px;
                }}
                .details-section ul {{ /* Styling untuk ul dari details_html */
                    list-style-type: none;
                    padding-left: 0;
                    margin: 0;
                }}
                .details-section ul li {{ /* Styling untuk li dari details_html */
                    background-color: #f8f9fa;
                    border: 1px solid #e9ecef;
                    padding: 12px 18px;
                    margin-bottom: 10px;
                    border-radius: 6px;
                    font-size: 14px;
                    line-height: 1.6;
                }}
                .details-section ul li strong {{
                    color: #0056b3; /* Konsisten dengan tema biru atau aksen berbeda */
                }}
                .details-section p {{ /* Styling untuk p dari details_html, misal "Silakan periksa..." */
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
                <div class="header status-{status_class}">
                    <h2>{header_title}</h2>
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
                            <td><span class="status-badge status-{status_class}">{status_upper}</span></td>
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
        return self.send_email(recipient, subject, body_html_content)
