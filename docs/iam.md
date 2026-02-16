# IAM for AWS Glue ETL Pipeline

This pipeline uses two S3 buckets:

- Raw data bucket: `industry-layoffs-data-raw-cleaned-v1`
- Curated insights bucket: `industry-layoffs-insights-buckets-v1`

The AWS Glue execution role is scoped to:
- Read-only access on the raw bucket
- Write-only access on the curated bucket
- CloudWatch logging

This enforces logical separation between ingestion and analytics layers.

## Trust Relationship

Glue must be allowed to assume this role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

## S3 Permissions

# Read Raw Data (source bucket)
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::industry-layoffs-data-raw-cleaned-v1",
    "arn:aws:s3:::industry-layoffs-data-raw-cleaned-v1/*"
  ]
}

# Write Outputs (target bucket)
{
  "Effect": "Allow",
  "Action": [
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::industry-layoffs-insights-buckets-v1",
    "arn:aws:s3:::industry-layoffs-insights-buckets-v1/*"
  ]
}