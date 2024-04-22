import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import os
import pathlib

load_dotenv()
EMAIL_HOST = os.getenv('EMAIL_HOST')
EMAIL_PORT = int(os.getenv('EMAIL_PORT'))
EMAIL_HOST_USER = os.getenv('EMAIL_HOST_USER')
EMAIL_HOST_PASSWORD = os.getenv('EMAIL_HOST_PASSWORD')

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()

def send_email_with_file(
        recipient_emails : list,
        subject : str,
        body : str,
        file_paths : list
    ) -> None:
    """
    Send an email with attachments.

    Args:
        recipient_emails (list): List of email addresses to send the email to.
        subject (str): Subject of the email.
        body (str): Body of the email.
        file_paths (list): List of file paths to attach to the email.
    """
    # Check arguments
    assert isinstance(recipient_emails, list), "recipient_emails must be a list."
    assert all(isinstance(email, str) for email in recipient_emails), "recipient_emails must be a list of strings."
    assert isinstance(subject, str), "subject must be a string."
    assert isinstance(body, str), "body must be a string."
    assert isinstance(file_paths, list), "file_paths must be a list."

    # Create the email message
    message = MIMEMultipart()
    message['Subject'] = subject
    message['From'] = EMAIL_HOST_USER
    recipient_emails.append(EMAIL_HOST_USER)
    message['To'] = ", ".join(recipient_emails)

    # Add disclaimer to the body
    body += "\n\nO conteúdo deste e-mail é confidencial e destinado exclusivamente ao destinatário especificado apenas na mensagem. É estritamente proibido compartilhar qualquer parte desta mensagem com terceiros, sem o consentimento por escrito do remetente. Se você recebeu esta mensagem por engano, responda a esta mensagem e siga com sua exclusão, para que possamos garantir que tal erro não ocorra no futuro."

    # Attach the body of the email
    message_body = MIMEText(body)
    message.attach(message_body)

    # Attach the files
    for file_path in file_paths:
        if not os.path.exists(file_path):
            file_path = ROOT_PATH / file_path
        assert os.path.exists(file_path), f"File does not exist: {file_path}"
        
        with open(file_path, 'rb') as file:
            attachment = MIMEApplication(file.read(), Name=os.path.basename(file_path))
            attachment['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
            message.attach(attachment)

    # Send the email
    try:
        with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT) as server:
            server.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
            server.sendmail(EMAIL_HOST_USER, recipient_emails, message.as_string())
    except Exception as e:
        raise Exception(f'Failed to send email "{subject}". Error: {e}')
    


