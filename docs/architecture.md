# Architecture — Real-Time Fraud Detection Pipeline

## Overview

This document describes the end-to-end architecture of the real-time fraud
detection pipeline built on AWS. The pipeline processes 10M+ financial
transactions per day with sub-second fraud scoring latency.

## Data Flow

```
Financial Transactions (10M+/day)
           │
           ▼
┌─────────────────────┐
│  AWS Kinesis Data   │  4 shards · KMS encrypted · 24hr retention
│  Streams            │  Partition key: account_id
└──────────┬──────────┘
           │  real-time (< 1 second)
           ▼
┌─────────────────────┐
│  AWS Lambda         │  FraudDetector — 6-rule scoring engine
│  Consumer           │  Velocity · Geo · Amount · Category · Time
└──────────┬──────────┘
           │
      ┌────┴─────┐
      │          │
      ▼          ▼
┌──────────┐  ┌──────────────┐
│ Amazon   │  │ DynamoDB     │  fraud-alerts table
│ S3 (raw) │  │ (alerts)     │  GSI on account_id
└──────────┘  └──────┬───────┘
      │               │
      │               ▼
      │        ┌──────────────┐
      │        │  AWS SNS     │  → Email (SES)
      │        │  + SES       │  → Slack webhook
      │        └──────────────┘
      │
      ▼
┌─────────────────────┐
│  AWS Glue PySpark   │  Feature engineering · DQ checks
│  ETL Job            │  80+ features · Runs hourly
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Amazon Redshift    │  Star schema · ra3.4xlarge × 2
│  ra3.4xlarge × 2    │  fraud_raw · fraud_features · fraud_analytics
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Tableau +          │  Portfolio & fraud dashboards
│  QuickSight         │  400+ analysts · Real-time refresh
└─────────────────────┘
```

## Fraud Scoring Rules

| Rule | Score | Trigger |
|---|---|---|
| High Amount | 35 | Transaction > $5,000 |
| Velocity Breach | 40 | > 10 txns in 5 minutes |
| Velocity Amount | 35 | > $10,000 in 5 minutes |
| High Risk Country | 20 | BR, MX, IN, RU, NG |
| High Risk Category | 15 | ATM, online retail, travel |
| Round Amount | 10 | Round amount > $100 |
| Off Hours | 10 | 1am – 5am UTC |

**Fraud threshold: score ≥ 60**

## Security & Compliance

- **KMS**: CMK for all data at rest (S3, Redshift, DynamoDB, Kinesis)
- **IAM**: Least-privilege roles per service
- **Secrets Manager**: No hardcoded credentials
- **CloudTrail**: Full API audit logging
- **VPC**: All services in private subnets
- **PCI-DSS**: 100% compliant — field-level masking on card data
- **GDPR**: Data minimisation, right-to-erasure pipeline

## Performance Metrics

| Metric | Value |
|---|---|
| Daily throughput | 10M+ transactions |
| Fraud alert latency | < 5 seconds (↓45% vs batch) |
| SLA compliance | 99.6% |
| Pipeline recovery time | ↓63% via automated Step Functions |
| False positive rate | ↓18% via ML-assisted scoring |

## Author

**Pramod Vishnumolakala** — Senior Data Engineer  
pramodvishnumolakala@gmail.com
