import time
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import smtplib
from email.message import EmailMessage


# Create MySQL database connection
class EstablishConnection:
    def __init__(self, host="localhost", user="root", password="justdo!t",
                 database="local_data"):
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
new_connection = est_connection.database_connection()


def send_email(receiver, recording_name, sender='ishaanrdave1991@gmail.com', password='czbl zksg omef dgjy'):
    try:
        email_message = EmailMessage()
        email_message['Subject'] = "Beyond Heavens: Video recording!"
        body_content = f"""
            Hey, Someone just shared a video recording with you. You can watch it at below link.
    
            File: {recording_name}
            http://192.168.0.101:8501/View_uploaded
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
        records_query = """
            SELECT id, filename,receiveremailid, senddate  FROM videos
            WHERE DATE(senddate) = CURRENT_DATE()
            AND (status IS NULL OR LOWER(status) <> 'sent');
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
                    UPDATE videos SET status = "sent"
                    WHERE id = {sent_file_id};
                """
        cursor.execute(update_record_query)
        new_connection.commit()
        cursor.close()
    except Error as e:
        print(f"Error: {e}")


def main(todays_records):
    if len(todays_records) > 0:
        for index, file_row in enumerate(todays_records):
            video_send_date = datetime.strptime(file_row[3], '%Y-%m-%d %H:%M:%S').hour
            recording_name = file_row[1]
            receiver_email = file_row[2]
            email_status = send_email(receiver_email, recording_name)
            if email_status:
                file_id = file_row[0]
                mark_as_sent(file_id)


while True:
    existing_records = fetch_unsend_records()
    main(existing_records)
    existing_records.clear()
    time.sleep(300)          # execution every 5 minutes
