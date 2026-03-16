# ============================================================
# Fraud Detection Pipeline — Terraform Infrastructure (AWS)
# Author: Pramod Vishnumolakala
# ============================================================

terraform {
  required_version = ">= 1.4"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "pramod-terraform-state"
    key    = "fraud-detection/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" { region = var.aws_region }

variable "aws_region"   { default = "us-east-1" }
variable "environment"  { default = "production" }
variable "project_name" { default = "fraud-detection" }


# ── KMS key for PCI-DSS field-level encryption ───────────────────────
resource "aws_kms_key" "fraud_pipeline" {
  description             = "Fraud Detection Pipeline — PCI-DSS encryption key"
  deletion_window_in_days = 30
  enable_key_rotation     = true
  tags = { Project = var.project_name, Environment = var.environment }
}

resource "aws_kms_alias" "fraud_pipeline" {
  name          = "alias/fraud-detection-pipeline"
  target_key_id = aws_kms_key.fraud_pipeline.key_id
}


# ── Kinesis Data Stream ───────────────────────────────────────────────
resource "aws_kinesis_stream" "transactions" {
  name             = "fraud-detection-transactions"
  shard_count      = 4
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  encryption_type = "KMS"
  kms_key_id      = aws_kms_key.fraud_pipeline.arn

  tags = { Project = var.project_name }
}


# ── S3 buckets ────────────────────────────────────────────────────────
resource "aws_s3_bucket" "raw_transactions" {
  bucket = "${var.project_name}-raw-transactions-${var.environment}"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw_transactions.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.fraud_pipeline.arn
    }
  }
}

resource "aws_s3_bucket" "glue_scripts" {
  bucket = "${var.project_name}-glue-scripts-${var.environment}"
}

resource "aws_s3_bucket" "redshift_temp" {
  bucket = "${var.project_name}-redshift-temp-${var.environment}"
}


# ── DynamoDB — fraud alerts store ────────────────────────────────────
resource "aws_dynamodb_table" "fraud_alerts" {
  name         = "fraud-alerts"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "transaction_id"

  attribute {
    name = "transaction_id"
    type = "S"
  }

  attribute {
    name = "account_id"
    type = "S"
  }

  global_secondary_index {
    name            = "account_id-index"
    hash_key        = "account_id"
    projection_type = "ALL"
  }

  point_in_time_recovery { enabled = true }
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.fraud_pipeline.arn
  }

  tags = { Project = var.project_name }
}


# ── SNS topic — fraud alerts ─────────────────────────────────────────
resource "aws_sns_topic" "fraud_alerts" {
  name              = "fraud-alerts"
  kms_master_key_id = aws_kms_key.fraud_pipeline.arn
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.fraud_alerts.arn
  protocol  = "email"
  endpoint  = "pramodvishnumolakala@gmail.com"
}


# ── AWS Glue Job ──────────────────────────────────────────────────────
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_glue_job" "fraud_etl" {
  name         = "${var.project_name}-etl"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/glue_etl_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"        = "python"
    "--enable-metrics"      = ""
    "--enable-spark-ui"     = "true"
    "--S3_INPUT"            = "s3://${aws_s3_bucket.raw_transactions.bucket}/transactions/"
    "--REDSHIFT_URL"        = "jdbc:redshift://${aws_redshift_cluster.fraud_dw.endpoint}:5439/frauddb"
    "--REDSHIFT_TEMP_DIR"   = "s3://${aws_s3_bucket.redshift_temp.bucket}/temp/"
  }

  number_of_workers = 10
  worker_type       = "G.1X"
  timeout           = 60
}


# ── Redshift cluster ─────────────────────────────────────────────────
resource "aws_redshift_cluster" "fraud_dw" {
  cluster_identifier = "${var.project_name}-dw"
  database_name      = "frauddb"
  master_username    = "admin"
  master_password    = var.redshift_password
  node_type          = "ra3.4xlarge"
  number_of_nodes    = 2

  encrypted  = true
  kms_key_id = aws_kms_key.fraud_pipeline.arn

  skip_final_snapshot = false
  final_snapshot_identifier = "${var.project_name}-final-snapshot"

  tags = { Project = var.project_name }
}

variable "redshift_password" {
  description = "Redshift master password"
  sensitive   = true
}


# ── CloudWatch alarms ─────────────────────────────────────────────────
resource "aws_cloudwatch_metric_alarm" "kinesis_iterator_age" {
  alarm_name          = "fraud-kinesis-iterator-age-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  namespace           = "AWS/Kinesis"
  period              = 60
  statistic           = "Maximum"
  threshold           = 30000    # 30 seconds
  alarm_description   = "Kinesis consumer falling behind — fraud detection SLA at risk"
  alarm_actions       = [aws_sns_topic.fraud_alerts.arn]

  dimensions = { StreamName = aws_kinesis_stream.transactions.name }
}


# ── Outputs ───────────────────────────────────────────────────────────
output "kinesis_stream_arn" { value = aws_kinesis_stream.transactions.arn }
output "fraud_alerts_topic" { value = aws_sns_topic.fraud_alerts.arn }
output "redshift_endpoint"  { value = aws_redshift_cluster.fraud_dw.endpoint }
output "kms_key_alias"      { value = aws_kms_alias.fraud_pipeline.name }
