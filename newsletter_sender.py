import base64
import re
import smtplib
import ssl
import tempfile
import time
import pymysql
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from sys import argv
from bs4 import BeautifulSoup

FILE_NAME, REGION, NEWSLETTER_ID = argv
a = REGION.lower()
REGION = a

if REGION == 'pl':
    REGION = 5
elif REGION == 'eu':
    REGION = 2
elif REGION == 'na':
    REGION = 3

print(f'START\nRegion: {REGION}\nNewsletter: {NEWSLETTER_ID}')

sender_email = 'test'
password = 'test'

with open('../config/db.json', 'r') as config:
    data = json.load(config)

con = pymysql.connect(
    user=data['user'],
    password=data['password'],
    host=data['host'],
    database=data['database'],
    port=data['port'],
    ssl={'ssl': {
        'ca': data['certificate']['ca'],
        'key': data['certificate']['cert'],
        'cert': data['certificate']['key']}}
)


def newsletter_sender():
    context = ssl.create_default_context()
    with smtplib.SMTP("dm smtp", 587) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(sender_email, password)
        with con.cursor() as cur:
            if REGION == 5 or REGION == 3 or REGION == 2:
                # cur.execute(test)
            else:
                cur.execute(
                    "SELECT id, email "
                    "FROM dreammachiql_1.orders "
                    "WHERE email = 'test'"
                    "GROUP BY email")

            bd_emails = cur.fetchall()

            cur.execute(f"SELECT html, title FROM newsletter_html WHERE id = '{NEWSLETTER_ID}'")
            html_from_db = cur.fetchone()
            html_from_db_str = f'{"".join(html_from_db[0])}'
            newsletter_title_str = f'{"".join(html_from_db[1])}'

            for id_email, client_email in bd_emails:
                id_email_str = f'{"".join(str(id_email))}'
                id_email_str_base64 = base64.b64encode(bytes(id_email_str, 'utf-8'))
                id_email_str_base64_decode = id_email_str_base64.decode("utf-8")

                message = MIMEMultipart("alternative")
                message["Subject"] = newsletter_title_str
                message["From"] = f'Dream Machines <{sender_email}>'
                message["To"] = client_email

                curent_id = str(id_email)
                newsletter_id = str(NEWSLETTER_ID)
                unsub_id = str(id_email_str_base64_decode)

                with tempfile.NamedTemporaryFile('w+') as file_temp:
                    file_temp.write(html_from_db_str)
                    file_temp.seek(0)
                    html_in_file_temp = file_temp.read()
                    soup = BeautifulSoup(html_in_file_temp, 'html.parser')
                    for i in soup.findAll('img'):
                        x = (i['src'] + '"')
                        y = (i['src'] + '?nid=' + ''.join(newsletter_id) + '&uid=' + ''.join(curent_id) + '&r=1' + '"')
                        html_in_file_temp = html_in_file_temp.replace(x, y)
                    for a in soup.findAll('a', href=True):
                        b = (a['href'] + '"')
                        if re.match(r'https://dreammachines', b):
                            c = (a['href'] + '?nid=' + ''.join(newsletter_id) + '&uid=' + ''.join(
                                curent_id) + '&r=2' + '"')
                            html_in_file_temp = html_in_file_temp.replace(b, c)
                    for a in soup.findAll('a', href=True):
                        b = (a['href'] + '"')
                        if re.match(r'https://www.dreammachines', b):
                            c = (a['href'] + ''.join(unsub_id) + '"')
                            html_in_file_temp = html_in_file_temp.replace(b, c)

                mail_html = MIMEText(html_in_file_temp, "html")
                message.attach(mail_html)
                server.set_debuglevel(0)

                try:
                    print(f'Send email to: {client_email}')
                    server.sendmail(sender_email, client_email, message.as_string())
                    with con.cursor() as cur:
                        cur.execute(f"INSERT INTO newsletter_emails_log (email, region_id, email_encode, newsletter_id, email_id) "
                                    f"VALUES ('{client_email}', '{REGION}', '{unsub_id}', '{newsletter_id}', '{curent_id}')")
                        con.commit()
                except Exception as err:
                    with con.cursor() as cur:
                        cur.execute(f"INSERT INTO newsletter_log (newsletter_id, region_id, err_data) "
                                    f"VALUES ('{NEWSLETTER_ID}', '{REGION}', '{err}')")
                        con.commit()
                    continue
                time.sleep(0.1)


if __name__ == "__main__":
    try:
        newsletter_sender()
    except Exception as err:
        print(err)
        with con.cursor() as cur:
            cur.execute(f"INSERT INTO newsletter_log (newsletter_id, region_id, err_data) "
                        f"VALUES ('{NEWSLETTER_ID}', '{REGION}', '{err}')")
            con.commit()
    finally:
        con.close()
        if con.open:
            print('DB: connection is open')
        else:
            print('DB: connection is closed')
        print('END')
