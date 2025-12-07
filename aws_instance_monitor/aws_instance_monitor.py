import boto3
from tabulate import tabulate
from datetime import datetime, timezone
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from collections import defaultdict

def main():
    # Get report type from environment variable
    report_type = os.getenv('REPORT_TYPE', 'daily')  # 'exceeding' or 'daily'
    
    # Create EC2 client
    ec2 = boto3.client('ec2', region_name='us-east-1')
    
    # Get all regions
    regions = ec2.describe_regions()['Regions']
    
    instances = []
    exceeding_instances = []  # For exceeding report
    
    for region in regions:
        region_name = region['RegionName']
        ec2_reg = boto3.client('ec2', region_name=region_name)
        
        # Get all running instances in this region
        response = ec2_reg.describe_instances(
            Filters=[
                {
                    'Name': 'instance-state-name',
                    'Values': ['running']
                }
            ]
        )
        
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                tags = {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
                instance_name = tags.get('Name', 'N/A')
                
                # Ignore specific instances that should always be running
                if instance_name in ['build-status-monitor', 'prometheus-grafana-server']:
                    continue
                
                # Calculate uptime
                launch_time = instance['LaunchTime']
                uptime_delta = datetime.now(timezone.utc) - launch_time
                days = uptime_delta.days
                hours, remainder = divmod(uptime_delta.seconds, 3600)
                minutes, _ = divmod(remainder, 60)
                uptime_str = f"{days}d {hours}h {minutes}m"
                uptime_hours = uptime_delta.total_seconds() / 3600
                
                # Check keep status
                keep_value = tags.get('keep', 'N/A')
                exceeding_keep = False
                if keep_value != 'N/A':
                    try:
                        keep_hours = float(keep_value)
                        if uptime_hours > keep_hours:
                            keep_status = 'Exceeding'
                            exceeding_keep = True
                        else:
                            keep_status = 'Within'
                    except ValueError:
                        keep_status = 'Invalid Keep'
                else:
                    if uptime_hours > 8:
                        keep_status = 'Exceeding (default 8h)'
                        exceeding_keep = True
                    else:
                        keep_status = 'Within (default 8h)'
                
                row = {
                    'Region': region_name,
                    'Instance ID': instance['InstanceId'],
                    'Instance type': instance['InstanceType'],
                    'Public IP': instance.get('PublicIpAddress', 'N/A'),
                    'JenkinsJobTag': tags.get('JenkinsJobTag', 'N/A'),
                    'RunByUser': tags.get('RunByUser', 'N/A'),
                    'keep': keep_value,
                    'Name': instance_name,
                    'Uptime': uptime_str,
                    'Keep Status': keep_status
                }
                
                # Only add instances with "Within" status to the full list for daily reports
                # Exceeding instances are tracked separately
                if keep_status != 'Within' and keep_status != 'Within (default 8h)':
                    instances.append(row)
                
                # Track exceeding instances separately
                if exceeding_keep:
                    exceeding_instances.append(row)
    
    # Print the table based on report type
    if report_type == 'daily':
        print(f"\n=== DAILY REPORT - All Running Instances ===")
        if instances:
            table_console = tabulate(instances, headers='keys', tablefmt='simple', maxcolwidths=25)
            print(table_console)
        else:
            print("No running instances found.")
    else:
        print(f"\n=== EXCEEDING INSTANCES REPORT ===")
        if exceeding_instances:
            table_console = tabulate(exceeding_instances, headers='keys', tablefmt='simple', maxcolwidths=25)
            print(table_console)
        else:
            print("No instances exceeding keep time found.")
    
    # Report instances exceeding keep time (monitoring only - no termination)
    if exceeding_instances:
        print(f"\n⚠️  Found {len(exceeding_instances)} instance(s) exceeding keep time:")
        for instance in exceeding_instances:
            print(f"     - {instance['Instance ID']} in {instance['Region']} (Uptime: {instance['Uptime']})")
    else:
        print("\n✅ No instances found exceeding keep time.")
    
    # Send email notification
    smtp_server = os.getenv('SMTP_SERVER')
    smtp_port = os.getenv('SMTP_PORT', '587')
    smtp_user = os.getenv('SMTP_USER')
    smtp_pass = os.getenv('SMTP_PASS')
    email_from = os.getenv('EMAIL_FROM')
    email_to_str = os.getenv('EMAIL_TO')
    if not email_to_str:
        print("EMAIL_TO not set")
        email_to = []
    else:
        email_to = [addr.strip() for addr in email_to_str.split(',')]
    
    if not smtp_server:
        print("SMTP_SERVER not set")
    if not smtp_user:
        print("SMTP_USER not set")
    if not smtp_pass:
        print("SMTP_PASS not set")
    if not email_from:
        print("EMAIL_FROM not set")
    
    # Send email based on report type
    should_send_email = False
    if report_type == 'daily':
        # Only send daily report if there are exceeding instances
        should_send_email = bool(exceeding_instances)
        email_subject = 'Daily AWS Instance Report - Instances Exceeding Keep Time'
    else:
        # Only send email if there are exceeding instances
        should_send_email = bool(exceeding_instances)
        email_subject = 'AWS Instance Alert - Instances Exceeding Keep Time'
    
    if smtp_server and smtp_user and smtp_pass and email_from and email_to and should_send_email:
        table_html = tabulate(exceeding_instances, headers='keys', tablefmt='html')
        
        summary = f"<p><strong>⚠️ Instances Exceeding Keep Time:</strong> {len(exceeding_instances)}</p>"
        
        html_body = f"""
        <html>
        <head>
        <style>
        body {{ font-family: Arial, sans-serif; }}
        h2 {{ color: #333; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        tr:hover {{ background-color: #e9e9e9; }}
        .summary {{ background-color: #f0f8ff; padding: 15px; border-radius: 5px; margin: 10px 0; }}
        </style>
        </head>
        <body>
        <h2>{email_subject}</h2>
        <div class="summary">
        {summary}
        </div>
        {table_html}
        </body></html>"""
        
        msg = MIMEMultipart()
        msg['From'] = email_from
        msg['To'] = ', '.join(email_to)
        msg['Subject'] = email_subject
        msg.attach(MIMEText(html_body, 'html'))
        
        try:
            print(f"Connecting to {smtp_server}:{smtp_port} with user {smtp_user}")
            server = smtplib.SMTP(smtp_server, int(smtp_port))
            server.starttls()
            server.login(smtp_user, smtp_pass)
            text = msg.as_string()
            server.sendmail(email_from, email_to, text)
            server.quit()
            print("📧 Email notification sent.")
        except Exception as e:
            print(f"❌ Error sending email: {e}")
    elif smtp_server and not exceeding_instances:
        print("No exceeding instances to report - email not sent.")

if __name__ == "__main__":
    main()
