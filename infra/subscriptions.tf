resource "aws_sns_topic_subscription" "lambda_subscription" {
  topic_arn = module.sns.sns_topic_arn
  protocol  = "lambda"
  endpoint  = module.lambda.lambda_function_arn

  depends_on = [
    module.sns,
    module.lambda
  ]
}

resource "aws_lambda_permission" "allow_sns" {
  statement_id  = "AllowExecutionFromSNS_amazon_review"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda.lambda_function_name
  principal     = "sns.amazonaws.com"
  source_arn    = module.sns.sns_topic_arn

  depends_on = [
    aws_sns_topic_subscription.lambda_subscription
  ]
}