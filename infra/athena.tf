resource "aws_athena_workgroup" "pipeline" {
  name = local.workgroup_name

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.id}/athena/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}