# Rearc Data Quest Submission

This repository contains my solution for the Rearc Data Quest. It uses AWS CDK (Infrastructure as Code) to deploy a serverless data pipeline.

## Architecture
- **Part 1 & 2:** A Python Lambda (`SyncLambda`) scheduled to run daily via EventBridge. It syncs BLS Time Series data and US Population data into S3.
- **Part 3:** An S3 Bucket Notification triggers an SQS Queue when data is updated.
- **Part 4:** An Analytics Lambda consumes messages from SQS and performs data analysis using Pandas.

## Key Links
- **S3 Data Bucket:** [public S3 link](https://foruhar-rearc-quest.s3.amazonaws.com/)

## How to Deploy
1. Install dependencies: `pip install aws-cdk-lib constructs`
2. Bootstrap your account: `cdk bootstrap`
3. Deploy: `cdk deploy`

## AI Usage Disclosure
I used **Gemini (Google AI)** to assist with:
- Troubleshooting `cdk bootstrap` errors and IAM permissions.
- Architecting the event-driven trigger (S3 -> SQS -> Lambda).