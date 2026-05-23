import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# Email configuration
SMTP_SERVER = "smtp.gmail.com"  # Change based on your email provider
SMTP_PORT = 587
SENDER_EMAIL = "your_email@gmail.com"  # Replace with your email
SENDER_PASSWORD = "your_app_password"  # Replace with your app password
RECIPIENT_EMAIL = "recipient@example.com"  # Replace with recipient email

def send_reminder_email():
    """Send the contract rotation reminder email."""
    current_month = datetime.now().month
    
    # Determine the target month based on current month
    month_mapping = {
        3: "June",      # March -> June
        6: "September", # June -> September
        9: "December",  # September -> December
        12: "March"     # December -> March
    }
    
    target_month = month_mapping.get(current_month, "")
    message = f"Rotate Contracts to {target_month}"
    
    subject = message
    body = f"""
    Hello,
    
    {message}
    
    Please ensure all contract rotations are completed in a timely manner.
    
    Best regards,
    Automated Reminder System
    """
    
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = RECIPIENT_EMAIL
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        # Connect to SMTP server and send email
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        
        print(f"Email sent successfully at {datetime.now()}")
    
    except Exception as e:
        print(f"Error sending email: {e}")

def should_send_today():
    """Check if today is a reminder day."""
    today = datetime.now()
    month = today.month
    day = today.day
    
    # Check if current month is March (3), June (6), September (9), or December (12)
    reminder_months = [3, 6, 9, 12]
    # Check if current day is 10th or 20th
    reminder_days = [10, 20]
    
    return month in reminder_months and day in reminder_days

def main():
    """Main function to check and send email if appropriate."""
    if should_send_today():
        send_reminder_email()
    else:
        print(f"No reminder scheduled for today ({datetime.now().strftime('%Y-%m-%d')})")

if __name__ == "__main__":
    main()
