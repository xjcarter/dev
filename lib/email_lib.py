import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Gmail SMTP server configuration
smtp_server = "smtp.gmail.com"
smtp_port = 587  # 587 is the TLS port

# Gmail account credentials
email_address = "xjcarter@gmail.com"
app_password = "tbej rnrv gwnl kalr"  # Generate this from your Google Account

def send_email(recipient_email, subject, message, attachment_path=None, attachment_title=None):

    # Create a connection to the SMTP server
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()

    # Log in to your Gmail account
    server.login(email_address, app_password)

    # Create the email content
    msg = MIMEMultipart()
    msg["From"] = email_address
    msg["To"] = recipient_email
    msg["Subject"] = subject 

    # Create an HTML part with 'Courier New' font
    message = message.replace('\n','<br>')
    html_body = f"<html><body style='font-family: Courier New; font-size: 10pt;'>{message}</body></html>"
    msg.attach(MIMEText(html_body, "html"))

    # Attach the text file if provided
    if attachment_path:
        attachment = MIMEBase("application", "octet-stream")
        with open(attachment_path, "rb") as file:
            attachment.set_payload(file.read())
        encoders.encode_base64(attachment)
        attachment.add_header("Content-Disposition", f"attachment; filename={attachment_title}")
        msg.attach(attachment)

    # Send the email
    server.sendmail(email_address, recipient_email, msg.as_string())

    # Quit the SMTP server
    server.quit()


def send_html(recipient_email, subject, html_body, attachment_path=None, attachment_title=None):

    # Create a connection to the SMTP server
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()

    # Log in to your Gmail account
    server.login(email_address, app_password)

    # Create the email content
    msg = MIMEMultipart()
    msg["From"] = email_address
    msg["To"] = recipient_email
    msg["Subject"] = subject

    msg.attach(MIMEText(html_body, "html"))

    # Attach the text file if provided
    if attachment_path:
        attachment = MIMEBase("application", "octet-stream")
        with open(attachment_path, "rb") as file:
            attachment.set_payload(file.read())
        encoders.encode_base64(attachment)
        attachment.add_header("Content-Disposition", f"attachment; filename={attachment_title}")
        msg.attach(attachment)

    # Send the email
    server.sendmail(email_address, recipient_email, msg.as_string())

    # Quit the SMTP server
    server.quit()

