variable "timezone" {
  description = "tz database timezone name (e.g. Asia/Tokyo)"
  default     = "UTC"
}

variable "memory" {
  description = "Lambda Function memory in megabytes"
  default     = 256
}

variable "timeout" {
  description = "Lambda Function timeout in seconds"
  default     = 60
}

variable "max_batch_size" {
  description = "Maximum number of records passed for a single Lambda invocation"
}

variable "kinesis_stream_arn" {
  description = "Source Kinesis Data Streams stream name"
}

variable "elasticsearch_host" {
  description = "Elasticsearch Service endpoint (without https://)"
}

variable "elasticsearch_arn" {
  description = "Elasticsearch Service ARN"
}

variable "index_name_prefix" {
  description = "Prefix for Elasticsearch indices names"
}

variable "lambda_package_url" {
  description = "Lambda package URL (see Usage in README)"
}

variable "handler" {
  description = "Lambda Function handler (entrypoint)"
  default     = "main.handler"
}

variable "runtime" {
  description = "Lambda Function runtime"
  default     = "python3.7"
}

variable "name" {
  description = "Resource name"
}

variable "starting_position" {
  description = "Kinesis ShardIterator type (see: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html )"
  default     = "TRIM_HORIZON"
}

variable "failed_log_s3_bucket" {
  description = "S3 bucket name for saving failed logs (ES API errors etc.)"
}

variable "failed_log_s3_prefix" {
  description = "Path prefix for failed logs"
}

variable "log_id_field" {
  description = "Key name for unique log ID"
  default     = "log_id"
}

variable "log_type_field" {
  description = "Key name for log type"
  default     = "log_type"
}

variable "log_type_unknown_prefix" {
  description = "Log type prefix for logs without log type field"
  default     = "unknown"
}

variable "log_timestamp_field" {
  description = "Key name for log timestamp"
  default     = "time"
}

variable "log_type_field_whitelist" {
  description = "Log type whitelist (if empty, all types will be processed)"
  default     = []
  type        = "list"
}

variable "tracing_mode" {
  description = "X-Ray tracing mode (see: https://docs.aws.amazon.com/lambda/latest/dg/API_TracingConfig.html )"
  default     = "PassThrough"
}

variable "tags" {
  description = "Tags for Lambda Function"
  type        = "map"
  default     = {}
}

variable "log_retention_in_days" {
  description = "Lambda Function log retention in days"
  default     = 30
}
