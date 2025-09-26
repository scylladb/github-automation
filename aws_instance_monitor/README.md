# AWS Instance Monitor

This Python script retrieves all running EC2 instances in your AWS account and displays them in a table format, including all associated tags.

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

Run the script using Python:

```
python aws_instance_monitor.py
```

Set the following environment variables for email notifications:
- `SMTP_SERVER` (e.g., smtp.gmail.com)
- `SMTP_PORT` (e.g., 587)
- `SMTP_USER`
- `SMTP_PASS`
- `EMAIL_FROM`
- `EMAIL_TO` (comma-separated list of email addresses for multiple recipients)

The script will display the table of running instances and automatically terminate any instances that have exceeded their 'keep' time (in hours) or 8 hours if no 'keep' tag is set.

**Warning:** This script will terminate EC2 instances. Ensure you have the necessary permissions and that termination is intended.

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
