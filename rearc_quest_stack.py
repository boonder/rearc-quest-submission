from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
    aws_s3_notifications as s3n,
    aws_lambda_event_sources as eventsources
)
from constructs import Construct

class RearcQuestStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. Create the S3 Bucket (CDK will auto-generate a unique name)
        bucket = s3.Bucket(self, "RearcDataBucket")

        # 2. Lambda 1: Sync BLS and API Data
        sync_lambda = _lambda.Function(
            self, "SyncLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda_1_sync"),
            timeout=Duration.minutes(3),
            environment={
                "BUCKET_NAME": bucket.bucket_name
            }
        )
        
        # Give Lambda 1 permission to read/write to the bucket
        bucket.grant_read_write(sync_lambda)

        # 3. Trigger Lambda 1 Daily via EventBridge
        rule = events.Rule(
            self, "DailySyncRule",
            schedule=events.Schedule.rate(Duration.days(1))
        )
        rule.add_target(targets.LambdaFunction(sync_lambda))

        # 4. SQS Queue for Event Driven Architecture
        queue = sqs.Queue(
            self, "PopulationDataQueue",
            visibility_timeout=Duration.minutes(3)
        )

        # 5. S3 Notification: When population JSON is updated, send message to SQS
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue),
            s3.NotificationKeyFilter(prefix="population_data/", suffix=".json")
        )

        # 6. Lambda 2: Analytics
        # We use the official AWS Managed Pandas layer ARN (for us-east-1)
        pandas_layer_arn = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:14"
        pandas_layer = _lambda.LayerVersion.from_layer_version_arn(self, "PandasLayer", pandas_layer_arn)

        analytics_lambda = _lambda.Function(
            self, "AnalyticsLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda_2_analytics"),
            timeout=Duration.minutes(2),
            layers=[pandas_layer],
            environment={
                "BUCKET_NAME": bucket.bucket_name
            }
        )
        
        # Give Lambda 2 permission to read from the bucket
        bucket.grant_read(analytics_lambda)

        # 7. Trigger Analytics Lambda whenever a message hits the SQS Queue
        analytics_lambda.add_event_source(eventsources.SqsEventSource(queue))