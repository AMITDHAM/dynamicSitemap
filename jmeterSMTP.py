import subprocess
import os
import time
import csv
from datetime import datetime
from smtplib import SMTP
from email.mime.text import MIMEText

# === CONFIGURATION ===
JMETER_PATH = "/home/ec2-user/jmeter/bin/jmeter"
JMX_FILE = "/home/ec2-user/jmeter-automation/QA2suit (7).jmx"
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
JTL_FILE = f"/home/ec2-user/jmeter-automation/results_{TIMESTAMP}.jtl"

# Email Configuration
SENDER_EMAIL = 'praveenkumarkesh@gmail.com'
SENDER_PASSWORD = 'tsuu pemx zhek vxas'
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
RECIPIENT_EMAILS = ['praveen@jobtrees.com']

# === LOGGING FUNCTION (console only) ===
def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {msg}")

# === RUN JMETER ===
def run_jmeter():
    log("🚀 Running JMeter test plan...")
    try:
        result = subprocess.run(
            [JMETER_PATH, "-n", "-t", JMX_FILE, "-l", JTL_FILE, "-Jjmeter.save.saveservice.autoflush=true"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            log(f"❌ JMeter exited with error code {result.returncode}")
            log(result.stderr.strip())
            return False
        else:
            log("✅ JMeter execution completed.")
            return True
    except Exception as e:
        log(f"❗ Error running JMeter: {e}")
        return False

# === WAIT FOR .JTL FILE ===
def wait_for_jtl(timeout=30):
    log("⏳ Waiting for results.jtl to be created...")
    for _ in range(timeout):
        if os.path.exists(JTL_FILE) and os.path.getsize(JTL_FILE) > 100:
            log("✅ results.jtl generated and non-empty.")
            return True
        time.sleep(1)
    log("❌ results.jtl was NOT found or is empty.")
    return False

# === PARSE .JTL FOR FAILED TESTS ===
def extract_failed_apis(jtl_path):
    failed = []
    seen = set()
    try:
        with open(jtl_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row.get('success', '').lower() == 'false':
                    key = (
                        row.get('label', ''),
                        row.get('responseCode', ''),
                        row.get('URL', ''),
                        row.get('responseMessage', '') or row.get('failureMessage', '')
                    )
                    if key not in seen:
                        seen.add(key)
                        failed.append({
                            'label': row.get('label', 'N/A'),
                            'url': row.get('URL', 'N/A'),
                            'responseCode': row.get('responseCode', 'N/A'),
                            'message': row.get('responseMessage') or row.get('failureMessage') or 'No error message'
                        })
    except Exception as e:
        log(f"❗ Error reading JTL file: {e}")
    return failed

# === EMAIL REPORT ===
def send_email(failed_tests):
    if not failed_tests:
        log("✅ All APIs passed. No email needed.")
        return

    body_lines = ["❌ JMeter Test Failures Report"]
    body_lines.append(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    body_lines.append(f"🔢 Total Unique Failed APIs: {len(failed_tests)}")
    body_lines.append("\n📄 Details:")

    for i, test in enumerate(failed_tests, 1):
        body_lines.append(f"\n{i}. API Label: {test['label']}")
        body_lines.append(f"   URL        : {test['url']}")
        body_lines.append(f"   Status Code: {test['responseCode']}")
        body_lines.append(f"   Message    : {test['message']}")

    body = "\n".join(body_lines)

    msg = MIMEText(body)
    msg['Subject'] = f"JMeter Failed API Report - {datetime.now().strftime('%Y-%m-%d')}"
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECIPIENT_EMAILS)

    try:
        with SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
            smtp.starttls()
            smtp.login(SENDER_EMAIL, SENDER_PASSWORD)
            smtp.send_message(msg)
            log("📧 Email report sent successfully.")
    except Exception as e:
        log(f"❗ Email sending failed: {str(e)}")

# === MAIN ===
if __name__ == "__main__":
    log("====================================")
    log("✅ Automation started")

    if run_jmeter() and wait_for_jtl():
        failed = extract_failed_apis(JTL_FILE)
        log(f"🔍 Found {len(failed)} failed API(s).")
        send_email(failed)
    else:
        log("⚠️ Skipping email - JMeter failed or .jtl not found.")

    log(f"📁 JTL path used: {JTL_FILE}")
    log("✅ Automation completed")
    log("====================================")
