import boto3
from tabulate import tabulate
from datetime import datetime, timezone
import os
import sys
import json
import urllib.request
from collections import defaultdict

def post_to_slack(webhook_url, payload):
    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp.read()
    except Exception as e:
        sys.exit(f"❌ Error sending Slack message: {e}")
    print("📨 Slack notification sent.")

def main():
    webhook_url = os.getenv('SLACK_WEBHOOK_URL', '').strip().strip('"\'')
    if webhook_url and not webhook_url.startswith('https://hooks.slack.com/'):
        sys.exit("❌ SLACK_WEBHOOK_URL is not a Slack incoming webhook URL "
                 "(expected https://hooks.slack.com/services/...)")
    if os.getenv('SLACK_TEST_MESSAGE', '').lower() == 'true':
        if not webhook_url:
            sys.exit("❌ SLACK_WEBHOOK_URL not set")
        post_to_slack(webhook_url, {'text': 'AWS Instance Monitor: test message - Slack notifications are working.'})
        return

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
        try:
            response = ec2_reg.describe_instances(
                Filters=[
                    {
                        'Name': 'instance-state-name',
                        'Values': ['running']
                    }
                ]
            )
        except Exception as e:
            print(f"Warning: Skipping region {region_name} - {e}")
            continue
        
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
    
    # Send Slack notification
    if not exceeding_instances:
        print("No exceeding instances to report - Slack message not sent.")
        return

    if not webhook_url:
        sys.exit("❌ SLACK_WEBHOOK_URL not set - cannot report exceeding instances")

    if report_type == 'daily':
        title = 'Daily AWS Instance Report - Instances Exceeding Keep Time'
    else:
        title = 'AWS Instance Alert - Instances Exceeding Keep Time'

    columns = ['Region', 'Instance ID', 'Name', 'RunByUser', 'Uptime', 'Keep Status']
    rows = [[inst.get(c, 'N/A') for c in columns] for inst in exceeding_instances]
    table = tabulate(rows, headers=columns, tablefmt='simple', maxcolwidths=25)
    # Slack section text is limited to 3000 characters
    if len(table) > 2900:
        table = table[:2900] + '\n... (truncated, see workflow log)'

    run_url = None
    if os.getenv('GITHUB_RUN_ID'):
        run_url = (f"{os.getenv('GITHUB_SERVER_URL', 'https://github.com')}/"
                   f"{os.getenv('GITHUB_REPOSITORY')}/actions/runs/{os.getenv('GITHUB_RUN_ID')}")

    summary = f"⚠️ *{len(exceeding_instances)}* instance(s) exceeding keep time"
    if run_url:
        summary += f" (<{run_url}|workflow run>)"

    payload = {
        'text': f"{title}: {len(exceeding_instances)} instance(s)",
        'blocks': [
            {'type': 'header', 'text': {'type': 'plain_text', 'text': title}},
            {'type': 'section', 'text': {'type': 'mrkdwn', 'text': summary}},
            {'type': 'section', 'text': {'type': 'mrkdwn', 'text': f"```{table}```"}},
        ],
    }

    post_to_slack(webhook_url, payload)

if __name__ == "__main__":
    main()
