# AWS Instance Monitor

This Python script monitors all running EC2 instances across all AWS regions in your account and provides daily reports.

## Features

- **Daily Monitoring**: Runs once per day at 8 AM UTC
- **Monitoring Only**: Reports on instances but does not terminate them automatically
- **Multi-region Support**: Monitors all AWS regions automatically
- **Instance Status Tracking**: Shows which instances are within or exceeding their keep time
- **Smart Email Notifications**: Sends comprehensive daily reports
- **Flexible Keep Time**: Supports custom keep times via instance tags or defaults to 8 hours

## Prerequisites

- Python 3.x installed
- AWS credentials configured (e.g., via AWS CLI, environment variables, or IAM roles)
- Required Python packages: `boto3`, `tabulate`
- (Optional) SMTP settings for email notifications

## Installation

1. Clone or download this repository.
2. Install dependencies:
   ```
   pip install boto3 tabulate
   ```

## Usage

### Local Execution

Run the script using Python:

```bash
# Daily report (default)
python aws_instance_monitor.py

# Exceeding instances report only
REPORT_TYPE=exceeding python aws_instance_monitor.py
```

### Environment Variables

Set the following environment variables for email notifications:
- `REPORT_TYPE` - Set to `daily` for full report (default) or `exceeding` for exceeding-only report
- `SMTP_SERVER` (e.g., smtp.gmail.com)
- `SMTP_PORT` (e.g., 587)
- `SMTP_USER`
- `SMTP_PASS`
- `EMAIL_FROM`
- `EMAIL_TO` (comma-separated list of email addresses for multiple recipients)

### GitHub Actions

The script runs automatically via GitHub Actions:
- **Daily at 8 AM UTC**: Sends comprehensive report of all instances

You can also trigger it manually:
1. Go to Actions tab in GitHub
2. Select "Daily AWS Instance Monitor" workflow
3. Click "Run workflow"

## Report Details

## Instance Keep Time

The script uses the `keep` tag on EC2 instances to determine how long they should run:
- If `keep` tag exists: Uses the value (in hours)
- If no `keep` tag: Defaults to 8 hours

**Note:** This script is for monitoring only and does not terminate instances automatically. It will report on instances that exceed their keep time for manual review.

The script will output a table with the following columns:
- Instance ID
- Instance type
- Public IP
- JenkinsJobTag
- RunByUser
- keep
- Name
- Uptime (calculated as days, hours, and minutes since launch)
- Keep Status (whether the instance is within or exceeding its 'keep' time in hours)

Each row represents an instance, with tag values filled in or 'N/A' if the tag is missing.

## Output

The script will output a table with the following columns:
- **Region**: AWS region where the instance is running
- **Instance ID**: EC2 instance identifier
- **Instance type**: EC2 instance type (e.g., t3.micro, m5.large)
- **Public IP**: Public IP address (if assigned)
- **JenkinsJobTag**: Custom tag for Jenkins job identification
- **RunByUser**: User who launched the instance
- **keep**: Keep time in hours from the instance tag
- **Name**: Instance name from the Name tag
- **Uptime**: Time since instance launch (days, hours, minutes)
- **Keep Status**: Whether the instance is within or exceeding its keep time

Each row represents an instance, with tag values filled in or 'N/A' if the tag is missing.

## GitHub Actions Setup

The workflow is configured to run once a day at 8 AM UTC.

### Setting up Secrets

In your GitHub repository, go to Settings > Secrets and variables > Actions and add the following secrets:

- `AWS_ACCESS_KEY_ID`: Your AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key
- `SMTP_USER`: SMTP username for email notifications
- `SMTP_PASS`: SMTP password for email notifications

### Workflow Details

The workflow file is located at `.github/workflows/daily-aws-instance-monitor.yml`. It will:

1. Checkout the repository
2. Set up Python 3.x
3. Install required dependencies (`boto3`, `tabulate`)
4. Run the script with the environment variables set from secrets
5. Send email notifications with the daily report

You can also manually trigger the workflow from the Actions tab in GitHub.

## Notes

- Ensure your AWS credentials have the necessary permissions to describe EC2 instances across all regions
- The script monitors running instances only
- No instances are terminated automatically - this is a monitoring-only tool
- Email notifications are sent daily with comprehensive instance reports
