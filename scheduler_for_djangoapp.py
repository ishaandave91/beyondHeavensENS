import time
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import smtplib
from email.message import EmailMessage


DATABASE = 'media'
CONTENT_TABLE = 'contentmodel'

# Create MySQL database connection
class EstablishConnection:
    def __init__(self, host="localhost", user="root", password="justdo!t",
                 database=DATABASE):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def database_connection(self):
        try:
            connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            if connection.is_connected():
                return connection
        except Error as e:
            print(f"Error: {e}")
        return None


est_connection = EstablishConnection()


def send_email(receiver, recording_name, sender='ishaanrdave1991@gmail.com', password='czbl zksg omef dgjy'):
    try:
        email_message = EmailMessage()
        email_message['Subject'] = "Beyond Heavens: Video recording!"
        body_content = f"""
            Hey, Someone just shared a video recording with you. You can watch it at below link.
    
            File: {recording_name}
            http://127.0.0.1:8000/viewuploaded/
            """
        email_message.set_content(body_content)
        # Create gmail server
        gmail = smtplib.SMTP('smtp.gmail.com', 587)  # Port for gmail = 587
        gmail.ehlo()
        gmail.starttls()
        gmail.login(sender, password)
        gmail.sendmail(sender, receiver, email_message.as_string())
        gmail.quit()
        return True
    except Error as e:
        print(f"Error sending email: {e}")
    return False


def fetch_unsend_records():
    try:
        cursor = new_connection.cursor()
        # Only extract files with status != sent
        records_query = f"""
            SELECT id, content,receiver_email, content_send_date  FROM {CONTENT_TABLE}
            WHERE DATE(content_send_date) <= CURRENT_DATE()
            AND LOWER(receiver_email_status) = 'verified'
            AND LOWER(content_status) <> 'sent';
        """
        cursor.execute(records_query)
        records = cursor.fetchall()
        cursor.close()
        return records
    except Error as e:
        print(f"Error: {e}")
    return []


def mark_as_sent(sent_file_id):
    try:
        cursor = new_connection.cursor()
        # Only extract files with status != sent
        update_record_query = f"""
                    UPDATE {CONTENT_TABLE} SET content_status = "sent"
                    WHERE id = {sent_file_id};
                """
        cursor.execute(update_record_query)
        new_connection.commit()
        cursor.close()
    except Error as e:
        print(f"Error: {e}")


def main(todays_records):
    print("Recordings to send: ", len(todays_records),
          "(", datetime.now(), ")")
    if len(todays_records) > 0:
        now = datetime.now().minute
        for index, file_row in enumerate(todays_records):
            video_send_time = datetime.strptime(str(file_row[3]), '%Y-%m-%d %H:%M:%S').minute
            if video_send_time <= now:
                recording_name = file_row[1]
                receiver_email = file_row[2]
                email_status = send_email(receiver_email, recording_name)
                if email_status:
                    file_id = file_row[0]
                    mark_as_sent(file_id)


while True:
    new_connection = est_connection.database_connection()
    existing_records = fetch_unsend_records()
    main(existing_records)
    existing_records.clear()
    new_connection.commit()
    new_connection.close()
    time.sleep(900)          # execution every 15 minutes
    # time.sleep(60)
