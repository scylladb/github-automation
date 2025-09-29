import boto3
from tabulate import tabulate
from datetime import datetime, timezone
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from collections import defaultdict

def main():
    # Create EC2 client
    ec2 = boto3.client('ec2')
    
    # Get all regions
    regions = ec2.describe_regions()['Regions']
    
    instances = []
    instances_to_terminate = []
    
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
                should_terminate = False
                if keep_value != 'N/A':
                    try:
                        keep_hours = float(keep_value)
                        if uptime_hours > keep_hours:
                            keep_status = 'Exceeding'
                            should_terminate = True
                        else:
                            keep_status = 'Within'
                    except ValueError:
                        keep_status = 'Invalid Keep'
                else:
                    if uptime_hours > 8:
                        keep_status = 'Exceeding (default 8h)'
                        should_terminate = True
                    else:
                        keep_status = 'Within (default 8h)'
                
                if should_terminate:
                    instances_to_terminate.append({'id': instance['InstanceId'], 'region': region_name})
                
                row = {
                    'Region': region_name,
                    'Instance ID': instance['InstanceId'],
                    'Instance type': instance['InstanceType'],
                    'Public IP': instance.get('PublicIpAddress', 'N/A'),
                    'JenkinsJobTag': tags.get('JenkinsJobTag', 'N/A'),
                    'RunByUser': tags.get('RunByUser', 'N/A'),
                    'keep': keep_value,
                    'Name': tags.get('Name', 'N/A'),
                    'Uptime': uptime_str,
                    'Keep Status': keep_status
                }
                instances.append(row)
    
    # Print the table
    if instances:
        table_console = tabulate(instances, headers='keys', tablefmt='simple', maxcolwidths=25)
        print(table_console)
    else:
        print("No running instances found.")
    
    # Terminate instances if any
    if instances_to_terminate:
        terminate_by_region = defaultdict(list)
        for item in instances_to_terminate:
            terminate_by_region[item['region']].append(item['id'])
        for reg, ids in terminate_by_region.items():
            ec2_term = boto3.client('ec2', region_name=reg)
            print(f"\nTerminating {len(ids)} instance(s) in {reg}: {', '.join(ids)}")
            ec2_term.terminate_instances(InstanceIds=ids)
            print("Termination initiated.")
    else:
        print("\nNo instances to terminate.")
    
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
    
    if smtp_server and smtp_user and smtp_pass and email_from and email_to and instances:
        table_html = tabulate(instances, headers='keys', tablefmt='html')
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
        </style>
        </head>
        <body>
        <h2>AWS Instance Report</h2>
        {table_html}
        """
        if instances_to_terminate:
            html_body += f"<p>Terminated {len(instances_to_terminate)} instance(s).</p>"
        html_body += "</body></html>"
        
        msg = MIMEMultipart()
        msg['From'] = email_from
        msg['To'] = ', '.join(email_to)
        msg['Subject'] = 'AWS Instance Report'
        msg.attach(MIMEText(html_body, 'html'))
        
        try:
            print(f"Connecting to {smtp_server}:{smtp_port} with user {smtp_user}")
            server = smtplib.SMTP(smtp_server, int(smtp_port))
            server.starttls()
            server.login(smtp_user, smtp_pass)
            text = msg.as_string()
            server.sendmail(email_from, email_to, text)
            server.quit()
            print("Email notification sent.")
        except Exception as e:
            print(f"Error sending email: {e}")
    elif smtp_server and not instances:
        print("No instances to report.")

if __name__ == "__main__":
    main()
