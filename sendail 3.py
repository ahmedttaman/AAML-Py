# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 15:51:58 2024

@author: Ahmad
"""

from email.message import EmailMessage
import smtplib

sender = "ahmedttaman@aaml.com.sa"
recipient = "admin@aaml.com.sa"
message = "Hello world!"

email = EmailMessage()
email["From"] = sender
email["To"] = recipient
email["Subject"] = "Test Email"
email.set_content(message)

smtp = smtplib.SMTP("smtp-mail.outlook.com", port=587)
smtp.starttls()
smtp.login(sender, "hkdrblndhgfnbjdq")
smtp.sendmail(sender, recipient, email.as_string())
smtp.quit()