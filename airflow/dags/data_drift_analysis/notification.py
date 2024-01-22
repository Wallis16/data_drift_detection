import smtplib, ssl
import os

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

load_dotenv()

sender_email = os.getenv('SENDER_EMAIL')
receiver_email = os.getenv('RECEIVER_EMAIL')
password = os.getenv('EMAIL_PASSWORD')

subject = 'An email with attachment from Python'
body = 'This is an email with attachment sent from Python'
mode = 'data_drift'

def notification(sender_email, receiver_email, password, subject, mode):

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject

    # Add body to email
    message.attach(MIMEText(body, 'plain'))

    reference_day, current_day = 1, 2
    analysis_path = f'airflow/dags/reports/nyc_taxi/results/2023_01_0{reference_day}__2023_01_0{current_day}/'+'data_drift.html'

    with open(analysis_path, 'rb') as attachment:  # r to open file in READ mode
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())

    encoders.encode_base64(part)

    part.add_header(
        'Content-Disposition',
        f'attachment; filename= {mode}',
    )

    message.attach(part)
    text = message.as_string()

    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)
