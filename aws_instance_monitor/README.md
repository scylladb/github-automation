# AWS Instance Monitor

This Python script monitors all running EC2 instances across all AWS regions in your account. It provides two types of reports:

1. **Exceeding Instances Report** (runs every 4 hours): Only reports instances that have exceeded their keep time
2. **Daily Full Report** (runs once per day at 8 AM UTC): Shows all running instances from the last 24 hours

## Features

- Monitors all AWS regions automatically
- Tracks instance uptime and compares against 'keep' tag values
- Automatically terminates instances exceeding their keep time
- Sends email notifications based on report type
- Provides detailed HTML email reports

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
# For exceeding instances report (default)
python aws_instance_monitor.py

# For daily full report
REPORT_TYPE=daily python aws_instance_monitor.py
```

### Environment Variables

Set the following environment variables for email notifications:
- `REPORT_TYPE` - Set to `daily` for full report or `exceeding` for exceeding-only report (default: `exceeding`)
- `SMTP_SERVER` (e.g., smtp.gmail.com)
- `SMTP_PORT` (e.g., 587)
- `SMTP_USER`
- `SMTP_PASS`
- `EMAIL_FROM`
- `EMAIL_TO` (comma-separated list of email addresses for multiple recipients)

### GitHub Actions

The script runs automatically via GitHub Actions:
- **Every 4 hours**: Checks for instances exceeding keep time and sends alerts
- **Daily at 8 AM UTC**: Sends comprehensive report of all instances

You can also trigger it manually:
1. Go to Actions tab in GitHub
2. Select "AWS Instance Monitor" workflow
3. Click "Run workflow"
4. Choose report type (exceeding or daily)

## Report Types

### Exceeding Instances Report
- Runs every 4 hours
- Only includes instances that have exceeded their keep time
- Email sent only if there are exceeding instances
- Terminates instances that exceed their keep time

### Daily Full Report
- Runs once per day at 8 AM UTC
- Includes all running instances
- Shows comprehensive statistics:
  - Total running instances
  - Number of instances exceeding keep time
  - Number of terminated instances
- Email always sent if there are any instances

## Instance Keep Time

The script uses the `keep` tag on EC2 instances to determine how long they should run:
- If `keep` tag exists: Uses the value (in hours)
- If no `keep` tag: Defaults to 8 hours

**Warning:** This script will automatically terminate EC2 instances that exceed their keep time. Ensure you have the necessary permissions and that termination is intended.

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

## GitHub Actions Setup

To automate the script to run daily, you can use GitHub Actions. The workflow is configured to run once a day at midnight UTC.

### Setting up Secrets

In your GitHub repository, go to Settings > Secrets and variables > Actions and add the following secrets:

- `AWS_ACCESS_KEY_ID`: Your AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key
- `SMTP_SERVER`: SMTP server (e.g., smtp.gmail.com)
- `SMTP_PORT`: SMTP port (e.g., 587)
- `SMTP_USER`: SMTP username
- `SMTP_PASS`: SMTP password
- `EMAIL_FROM`: Sender email address
- `EMAIL_TO`: Recipient email addresses (comma-separated for multiple)

### Workflow Details

The workflow file is located at `.github/workflows/daily-run.yml`. It will:

1. Checkout the repository
2. Set up Python 3.x
3. Install required dependencies (`boto3`, `tabulate`)
4. Run the script with the environment variables set from secrets

You can also manually trigger the workflow from the Actions tab in GitHub.

## Notes

- Ensure your AWS credentials have the necessary permissions to describe EC2 instances.
- The script only shows running instances.
