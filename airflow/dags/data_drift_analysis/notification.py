import smtplib, ssl

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def notification(sender_email, receiver_email, password, subject, body, mode, reference_day, current_day):

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject

    # Add body to email
    message.attach(MIMEText(body, 'plain'))

    analysis_path = f'/opt/airflow/dags/reports/nyc_taxi/results/2023_01_0{reference_day}__2023_01_0{current_day}/'+'data_drift.html'

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

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)
