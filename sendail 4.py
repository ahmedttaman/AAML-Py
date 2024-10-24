# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 11:04:10 2024

@author: Ahmad
"""

import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email():
    smtp_server = 'smtp.office365.com'
    smtp_port = 587
    sender_email = "ahmedttaman"
    receiver_email = 'ahmad.tamman@aaml.com.sa'
    password = "hkdrblndhgfnbjdq"  # Use app password if 2FA is enabled

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = 'Test Email'
    body = 'This is a test email.'
    msg.attach(MIMEText(body, 'plain'))
    
    

        

server = smtplib.SMTP("smtp.office365.com",587)
       
context=ssl.create_default_context()
server.ehlo()
server.starttls(context=context)
server.login(sender_email, password)
server.connect()
         # Upgrade to a secure connection
server.login(sender_email, password)
server.sendmail(sender_email, receiver_email, "aaaa")
print("Email sent successfully!")
    
server.quit()

send_email()
    
    
    
    
    
    
    
    
    
    

    try:
        

        server = smtplib.SMTP("smtp.office365.com",25)
       
        context=ssl.create_default_context()
        server.ehlo()
        server.starttls(context=context)
        server.connect()
         # Upgrade to a secure connection
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, "aaaa")
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        server.quit()

send_email()