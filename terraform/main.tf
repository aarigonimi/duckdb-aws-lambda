terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  description = "AWS region for the deployment."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used in resource naming."
  type        = string
  default     = "duckdb-aws-lambda"
}

variable "sales_bucket" {
  description = "The bucket that stores the raw Parquet input files and the generated result objects."
  type        = string
}

variable "sales_prefix" {
  description = "Input prefix for the raw sales dataset."
  type        = string
  default     = "raw/sales"
}

variable "raw_prefix" {
  description = "Validated prefix for input Parquet files in the Lambda function."
  type        = string
  default     = "raw/"
}

variable "results_prefix" {
  description = "Prefix used for deterministic output objects."
  type        = string
  default     = "results/"
}

variable "lambda_memory_mb" {
  description = "Lambda memory configuration, in MB."
  type        = number
  default     = 1024
}

variable "lambda_timeout_seconds" {
  description = "Lambda timeout in seconds."
  type        = number
  default     = 30
}

variable "allow_public_demo" {
  description = "Allow an unauthenticated demo route. Production should keep this false and default to AWS IAM authorization."
  type        = bool
  default     = false
}

locals {
  api_auth_mode = var.allow_public_demo ? "NONE" : "AWS_IAM"
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.project_name}"
  retention_in_days = 14
}

resource "aws_iam_role" "lambda_exec" {
  name = "${var.project_name}-lambda-exec"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda_execution" {
  name = "${var.project_name}-lambda-execution"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WriteLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = [
          aws_cloudwatch_log_group.lambda.arn,
          "${aws_cloudwatch_log_group.lambda.arn}:*"
        ]
      },
      {
        Sid    = "ReadWriteSalesObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:GetBucketLocation",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.sales_bucket}",
          "arn:aws:s3:::${var.sales_bucket}/${var.sales_prefix}/*",
          "arn:aws:s3:::${var.sales_bucket}/${var.results_prefix}/*"
        ]
      }
    ]
  })
}

resource "aws_ecr_repository" "lambda" {
  name = "${var.project_name}-lambda"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_lambda_function" "sales_analytics" {
  function_name = var.project_name
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda.repository_url}:latest"
  memory_size   = var.lambda_memory_mb
  timeout       = var.lambda_timeout_seconds

  environment {
    variables = {
      APP_AWS_REGION = var.aws_region
      SALES_BUCKET   = var.sales_bucket
      SALES_PREFIX   = var.sales_prefix
      RAW_PREFIX     = var.raw_prefix
      RESULTS_PREFIX = var.results_prefix
      API_AUTH_MODE  = local.api_auth_mode
    }
  }
}

resource "aws_apigatewayv2_api" "http" {
  name          = "${var.project_name}-http"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "lambda" {
  api_id                 = aws_apigatewayv2_api.http.id
  integration_type      = "AWS_PROXY"
  integration_uri       = aws_lambda_function.sales_analytics.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "sales_summary" {
  api_id             = aws_apigatewayv2_api.http.id
  route_key          = "GET /sales/summary"
  target             = "integrations/${aws_apigatewayv2_integration.lambda.id}"
  authorization_type = local.api_auth_mode
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.http.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "http_api" {
  statement_id  = "AllowHttpApiInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sales_analytics.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http.execution_arn}/*/*"
}

output "api_gateway_url" {
  value = aws_apigatewayv2_api.http.api_endpoint
}

output "api_auth_mode" {
  value = local.api_auth_mode
}
