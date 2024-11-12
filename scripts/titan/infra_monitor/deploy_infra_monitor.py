import typing
import pathlib
import logging
import argparse
import zipfile
import time
import tempfile
import json
import os
import shutil
import botocore
import boto3
from importlib import resources

from titan.infra_monitor import (
    LambdaPackage,
    LambdaDependenciesPackage,
    InfraHelperException,
    InfraHelper
)


logger = logging.getLogger(__name__)




class Deployer:

    def __init__(self, region: str, account_id: str, force_overwrite: bool):
        self._region = region
        self._account_id = account_id
        self._force_overwrite = force_overwrite
        self._infra_helper = InfraHelper(   region=self.region(),
                                            account_id=self.account_id()  )
    def region(self):
        return self._region

    def account_id(self):
        return self._account_id

    def force_overwrite(self):
        return self._force_overwrite

    def infra_helper(self):
        return self._infra_helper

    @classmethod
    def create_lambda_policy_doc(cls, region, account_id, lambda_name, secret_arn_prefix):
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "logs:CreateLogGroup",
                    "Resource": f"arn:aws:logs:{region}:{account_id}:*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource": [
                        f"arn:aws:logs:{region}:{account_id}:log-group:/aws/lambda/{lambda_name}:*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "cloudwatch:PutMetricData",
                        "cloudwatch:GetMetricWidgetImage"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "ec2:DescribeInstances"
                    ],
                    "Resource": [
                        "*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:GetObjectVersion"
                    ],
                    "Resource": [
                        "arn:aws:s3:::aws-billing-reports-097039683978/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "secretsmanager:GetSecretValue"
                    ],
                    "Resource": [
                        f"{secret_arn_prefix}-??????"
                    ]
                }
            ]
        }

    @classmethod
    def create_sns_topic_policy_doc(cls, account_id, topic_arn, bucket_arn):
        return {
            "Version": "2008-10-17",
            "Id": "__default_policy_ID",
            "Statement": [
                {
                    "Sid": "__default_statement_ID",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "*"
                    },
                    "Action": [
                        "SNS:GetTopicAttributes",
                        "SNS:SetTopicAttributes",
                        "SNS:AddPermission",
                        "SNS:RemovePermission",
                        "SNS:DeleteTopic",
                        "SNS:Subscribe",
                        "SNS:ListSubscriptionsByTopic",
                        "SNS:Publish"
                    ],
                    "Resource": topic_arn,
                    "Condition": {
                        "StringEquals": {
                            "AWS:SourceOwner": account_id
                        }
                    }
                },
                {
                    "Sid": "s3-can-publish",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "s3.amazonaws.com"
                    },
                    "Action": "SNS:Publish",
                    "Resource": topic_arn,
                    "Condition": {
                        "StringEquals": {
                            "aws:SourceAccount": account_id
                        },
                        "ArnLike": {
                            "aws:SourceArn": bucket_arn
                        }
                    }
                },
                {
                    "Sid": "budget-can-publish",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "budgets.amazonaws.com"
                    },
                    "Action": "SNS:Publish",
                    "Resource": topic_arn
                }
            ]
        }



    def remove_lambda_function(self, lambda_name: str):
        logger.info(f"Attempting to remove any previous resource associated with infra-monitor ...")
        try:
            self.infra_helper().delete_lambda_function(lambda_name)
        except InfraHelperException as e:
            logger.info(f"Failed to delete lambda function `{lambda_name}`: {e}")
        try:
            self.infra_helper().detach_policy_from_lambda_role(lambda_name)
        except InfraHelperException as e:
            logger.info(f"Failed to detach IAM policy from role for lambda `{lambda_name}`: {e}")
        try:
            self.infra_helper().delete_lambda_policy(lambda_name)
        except InfraHelperException as e:
            logger.info(f"Failed to delete IAM policy for lambda `{lambda_name}`: {e}")
        try:
            self.infra_helper().delete_lambda_role(lambda_name)
        except InfraHelperException as e:
            logger.info(f"Failed to delete IAM role for lambda `{lambda_name}`: {e}")

    def unschedule_lambda_function(self, lambda_name: str):
        try:
            self.infra_helper().unschedule_lambda_function(lambda_name)
        except InfraHelperException as e:
            logger.info(f"Failed to unschedule lambda `{lambda_name}`: {e}")

    def schedule_lambda_function(self, lambda_name: str, interval_mins: int):
        self.infra_helper().schedule_lambda_function(   lambda_name=lambda_name,
                                                        interval_mins=interval_mins )

    def deploy_lambda_function(self, lambda_name: str, package_bytes: bytes):
        self.infra_helper().create_lambda_policy(   lambda_name=lambda_name,
                                                    policy_doc=self.create_lambda_policy_doc(   region=self.region(),
                                                                                                account_id=self.account_id(),
                                                                                                lambda_name=lambda_name,
                                                                                                secret_arn_prefix=self.infra_helper().create_secret_arn_prefix(lambda_name)  ))
        self.infra_helper().wait_for_lambda_policy(lambda_name)
        self.infra_helper().create_lambda_role(lambda_name)
        self.infra_helper().wait_for_lambda_role(lambda_name)
        self.infra_helper().attach_policy_to_lambda_role(lambda_name)
        self.infra_helper().wait_for_aws(15)
        self.infra_helper().create_lambda_function(lambda_name=lambda_name,
                                            package_bytes=package_bytes)
        self.infra_helper().wait_for_lambda_function(lambda_name)



    def deploy_lambda_package(self, lambda_name: str):
        with resources.path(f"scripts.titan.infra_monitor.assets.{lambda_name}", "__init__.py") as p:
            package_path = pathlib.Path(p).parent
            self.deploy_lambda_function(lambda_name=lambda_name,
                                        package_bytes=LambdaPackage.create_package_bytes(package_path))

    def remove_cloudwatch_topic(self, topic_name):
        try:
            self.infra_helper().delete_sns_topic(topic_name)
        except InfraHelperException as e:
            logger.info(f"Failed to delete sns topic: {e}")


    def remove_cloudwatch_alarms(self, alarm_names):
        try:
            self.infra_helper().delete_cloudwatch_alarms(['SuddenIncreaseInInstancesAlarm', 'SuddenDecreaseInInstancesAlarm'])
        except InfraHelperException as e:
            logger.info(f"Failed to delete CloudWatch alarms: {e}")



    def create_instance_count_growth_alarm_fields(self, topic_arn):
        return {
            "AlarmDescription": 'Sudden increase in number of EC2 running instances',
            "ActionsEnabled": True,
            "OKActions": [],
            "AlarmActions": [
                topic_arn
            ],
            "InsufficientDataActions": [],
            "EvaluationPeriods": 1,
            "DatapointsToAlarm": 1,
            "Threshold": 1.2,
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
            "TreatMissingData": "missing",
            "Metrics": [
                {
                    "Id": "e1",
                    "Label": "Proportional change in running instances",
                    "ReturnData": True,
                    "Expression": "IF(DIFF(m1) == 0, 1.0, IF((m1-DIFF(m1))==0, 99.9, m1/(m1-DIFF(m1))))"# "IF(DIFF(m1)==0, 0, 100*(DIFF(m1) / (m1 - DIFF(m1))))"# "100*(DIFF(m1) / (m1 - DIFF(m1)))"
                },
                {
                    "Id": "m1",
                    "ReturnData": False,
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "InfraMonitor",
                            "MetricName": "InstanceCountPerState",
                            "Dimensions": [
                                {
                                    "Name": "InstanceState",
                                    "Value": "running"
                                }
                            ]
                        },
                        "Period": 60,
                        "Stat": "Average"
                    }
                }
            ]
        }

    def create_instance_count_decline_alarm_fields(self, topic_arn):
        return {
            "AlarmDescription": 'Sudden decrease in number of EC2 running instances',
            "ActionsEnabled": True,
            "OKActions": [],
            "AlarmActions": [
                topic_arn
            ],
            "InsufficientDataActions": [],
            "EvaluationPeriods": 1,
            "DatapointsToAlarm": 1,
            "Threshold": 0.8,
            "ComparisonOperator": "LessThanOrEqualToThreshold",
            "TreatMissingData": "missing",
            "Metrics": [
                {
                    "Id": "e1",
                    "Label": "Proportional change in running instances",
                    "ReturnData": True,
                    "Expression": "IF(DIFF(m1) == 0, 1.0, IF((m1-DIFF(m1))==0, 99.9, m1/(m1-DIFF(m1))))"#"IF(DIFF(m1)==0, 0, 100*(DIFF(m1) / (m1 - DIFF(m1))))"#"100*(DIFF(m1) / (m1 - DIFF(m1)))"
                },
                {
                    "Id": "m1",
                    "ReturnData": False,
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "InfraMonitor",
                            "MetricName": "InstanceCountPerState",
                            "Dimensions": [
                                {
                                    "Name": "InstanceState",
                                    "Value": "running"
                                }
                            ]
                        },
                        "Period": 60,
                        "Stat": "Average"
                    }
                }
            ]
        }


    # def delete_secret(self, secret_name):
    #     try:
    #         self.infra_helper().delete_secret(secret_name=secret_name)
    #     except InfraHelperException as e:
    #         logger.info(f"Failed to delete secret `{secret_name}`: {e}")

    def create_or_update_secret(self, secret_name, secret_value):
        try:
            self.infra_helper().create_secret(  secret_name=secret_name,
                                                secret_value=secret_value  )
        except InfraHelperException as e:
            self.infra_helper().update_secret(  secret_name=secret_name,
                                                secret_value=secret_value  )

    def undeploy(self):
        # self.delete_secret('ec2_usage_report_bot_secret')
        self.remove_lambda_function('ec2_usage_report_bot')
        self.unschedule_lambda_function('ec2_usage_metrics')
        self.remove_lambda_function('ec2_usage_metrics')
        self.remove_cloudwatch_topic('infra-monitor')
        self.remove_cloudwatch_alarms(['SuddenIncreaseInInstancesAlarm', 'SuddenDecreaseInInstancesAlarm'])

    def deploy(self, slack_token, slack_channel):
        if self.force_overwrite():
            self.undeploy()
        self.create_or_update_secret(   secret_name=self.infra_helper().create_secret_name('ec2_usage_report_bot'),
                                        secret_value=json.dumps({'slack-token': slack_token, 'slack-channel': slack_channel})  )
        self.deploy_lambda_package('ec2_usage_report_bot')
        self.deploy_lambda_package('ec2_usage_metrics')
        self.schedule_lambda_function(  lambda_name='ec2_usage_metrics',
                                        interval_mins=1 )

        topic_name = 'infra-monitor'
        topic_arn = self.infra_helper().create_sns_topic_arn(topic_name)
        self.infra_helper().create_sns_topic(   topic_name='infra-monitor',
                                                policy_doc=self.create_sns_topic_policy_doc(account_id=self.account_id(),
                                                                                            topic_arn=topic_arn,
                                                                                            bucket_arn='arn:aws:s3:::aws-billing-reports-097039683978')  )
        self.infra_helper().create_cloudwatch_alarm(alarm_name='SuddenIncreaseInInstancesAlarm',
                                                    alarm_fields=self.create_instance_count_growth_alarm_fields(topic_arn=topic_arn))
        self.infra_helper().create_cloudwatch_alarm(alarm_name='SuddenDecreaseInInstancesAlarm',
                                                    alarm_fields=self.create_instance_count_decline_alarm_fields(topic_arn=topic_arn))
        self.infra_helper().wait_for_cloudwatch_alarms(['SuddenIncreaseInInstancesAlarm', 'SuddenDecreaseInInstancesAlarm'])
        self.infra_helper().create_sns_lambda_subscription(topic_name, 'ec2_usage_report_bot')



class ArgValidator:
    
    @classmethod
    def ensure_valid_region(cls, region: str):
        assert region in {  'us-east-2',
                            'us-east-1',
                            'us-west-1',
                            'us-west-2',
                            'af-south-1',
                            'ap-east-1',
                            'ap-southeast-3',
                            'ap-south-1',
                            'ap-northeast-3',
                            'ap-northeast-2',
                            'ap-southeast-1',
                            'ap-southeast-2',
                            'ap-northeast-1',
                            'ca-central-1',
                            'eu-central-1',
                            'eu-west-1',
                            'eu-west-2',
                            'eu-south-1',
                            'eu-west-3',
                            'eu-north-1',
                            'me-south-1',
                            'sa-east-1' }, f"Invalid region `{self.region()}`"


    @classmethod
    def ensure_valid_env(cls):
        try:
            assert os.environ['SLACK_TOKEN'], f"SLACK_TOKEN needs to be in the shell environment"
            assert os.environ['SLACK_CHANNEL'], f"SLACK_CHANNEL needs to be in the shell environment"
        except KeyError as e:
            raise ValueError(f"Shell environment needs to contain a value for `{e}`")


def main():
    parser = argparse.ArgumentParser(description="Deploy the Infra Monitor to a region")
    parser.add_argument("-r", "--region", type=str, required=True, help="Region to deploy to")
    parser.add_argument("-a", "--account-id", type=str, required=True, help="AWS account id")
    parser.add_argument("-f", "--force-overwrite", action='store_true', default=False, required=False, help="Force overwrite any existing version of Infra Monitor")
    parser.add_argument("-u", "--undeploy", action='store_true', default=False, required=False, help="Remove any existing version of Infra Monitor")
    args = parser.parse_args()

    # configure the logger
    logging.basicConfig(level=logging.INFO)


    try:
        ArgValidator.ensure_valid_region(args.region)
        ArgValidator.ensure_valid_env()
        if args.undeploy:
            Deployer(   region=args.region,
                        account_id=args.account_id,
                        force_overwrite=args.force_overwrite   ).undeploy()
        else:
            Deployer(   region=args.region,
                        account_id=args.account_id,
                        force_overwrite=args.force_overwrite   ).deploy(slack_token=os.environ['SLACK_TOKEN'],
                                                                        slack_channel=os.environ['SLACK_CHANNEL'])
    except Exception as e:
        print(f"Failed due to exception: {e}")
        raise


if __name__ == "__main__"   :
    main()


