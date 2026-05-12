
resource "aws_s3_bucket" "bronze_bucket" {

  bucket = "retail-lakehouse-bronze-dev"
  tags = {
    Environment = "dev"
    Layer       = "bronze"
  }
}

resource "aws_s3_bucket" "silver_bucket" {

  bucket = "retail-lakehouse-silver-dev"
  tags = {
    Environment = "dev"
    Layer       = "silver"
  }
}

resource "aws_s3_bucket" "gold_bucket" {
  bucket = "retail-lakehouse-gold-dev"

  tags = {
    Environment = "dev"
    Layer       = "gold"
  }
}

resource "aws_s3_bucket" "glue_temp_bucket" {

  bucket = "retail-glue-temp-dev"
  tags = {
    Environment = "dev"
  }
}

# Versioning

resource "aws_s3_bucket_versioning" "bronze_versioning" {

  bucket = aws_s3_bucket.bronze_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "silver_versioning" {

  bucket = aws_s3_bucket.silver_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "gold_versioning" {

  bucket = aws_s3_bucket.gold_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Server Side Encryption

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze_encryption" {

  bucket = aws_s3_bucket.bronze_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }

  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver_encryption" {

  bucket = aws_s3_bucket.silver_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }

  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold_encryption" {

  bucket = aws_s3_bucket.gold_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }

  }
}