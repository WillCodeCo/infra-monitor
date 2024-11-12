import logging
import time
import json
import botocore
import boto3
from titan.infra_monitor.lambda_package import (
    LambdaPackage,
    LambdaDependenciesPackage
)


logger = logging.getLogger(__name__)


class InfraHelperException(Exception):
    pass

class InfraHelper:

    def __init__(self, region: str, account_id: str):
        self._region = region
        self._account_id = account_id
        self._clients = {}        

    def region(self):
        return self._region

    def account_id(self):
        return self._account_id

    def boto_config(self):
        return botocore.config.Config(region_name=self.region())

    def create_client(self, client_name):
        try:
            return self._clients[client_name]
        except KeyError:
            self._clients[client_name] = boto3.client(client_name, config=self.boto_config())
            return self._clients[client_name]

    def wait_for_aws(self, wait_time):
        logger.info(f"Sleeping a bit to wait for AWS")
        time.sleep(wait_time)

    def create_lambda_arn(self, lambda_name):
        return f"arn:aws:lambda:{self.region()}:{self.account_id()}:function:{lambda_name}"

    def create_lambda_role_name(self, lambda_name):
        return f"{lambda_name}-{self.region()}-role"

    def create_lambda_role_arn(self, lambda_name):
        return f'arn:aws:iam::{self.account_id()}:role/{self.create_lambda_role_name(lambda_name)}'

    def create_lambda_policy_name(self, lambda_name):
        return f"{lambda_name}-{self.region()}-policy"

    def create_lambda_policy_arn(self, lambda_name):
        return f'arn:aws:iam::{self.account_id()}:policy/{self.create_lambda_policy_name(lambda_name)}'

    def create_events_rule_name(self, lambda_name):
        return f"{lambda_name}-{self.region()}-rule"

    def create_events_rule_arn(self, lambda_name):
        return f"arn:aws:events:{self.region()}:{self.account_id()}:rule/{self.create_events_rule_name(lambda_name)}"

    def create_lambda_policy(self, lambda_name, policy_doc):
        try:
            iam = self.create_client('iam')
            response = iam.create_policy(
                PolicyName=self.create_lambda_policy_name(lambda_name),
                PolicyDocument=json.dumps(policy_doc)
            )
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def create_lambda_role(self, lambda_name):
        try:
            iam = self.create_client('iam')
            role_policy = {
              "Version": "2012-10-17",
              "Statement": {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
              }
            }
            response = iam.create_role(
                RoleName=self.create_lambda_role_name(lambda_name),
                AssumeRolePolicyDocument=json.dumps(role_policy),
            )
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def wait_for_lambda_role(self, lambda_name):
        try:
            logger.info(f"Waiting for lambda role `{self.create_lambda_role_name(lambda_name)}` ...")
            iam = self.create_client('iam')
            waiter = iam.get_waiter('role_exists')
            waiter.wait(
                RoleName=self.create_lambda_role_name(lambda_name)
            )
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def wait_for_lambda_policy(self, lambda_name):
        try:
            logger.info(f"Waiting for lambda policy {self.create_lambda_policy_arn(lambda_name)} ...")
            iam = self.create_client('iam')
            waiter = iam.get_waiter('policy_exists')
            waiter.wait(
                PolicyArn=self.create_lambda_policy_arn(lambda_name)
            )
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def wait_for_lambda_function(self, lambda_name):
        try:
            logger.info(f"Waiting for lambda function `{lambda_name}` ...")
            lambda_client = self.create_client('lambda')
            waiter = lambda_client.get_waiter('function_active')
            waiter.wait(FunctionName=lambda_name)
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))

    def create_lambda_function(self, lambda_name, package_bytes):
        try:
            lambda_client = self.create_client('lambda')
            response = lambda_client.create_function(
                FunctionName=lambda_name,
                Runtime='python3.9',
                Role=self.create_lambda_role_arn(lambda_name),
                Handler='lambda_function.lambda_handler',
                Code={
                    'ZipFile': package_bytes
                },
                Timeout=300, # Maximum allowable timeout
                MemorySize=512
            )
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def attach_policy_to_lambda_role(self, lambda_name):
        try:
            iam = self.create_client('iam')
            response = iam.attach_role_policy(
                RoleName=self.create_lambda_role_name(lambda_name),
                PolicyArn=self.create_lambda_policy_arn(lambda_name)
            )
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def detach_policy_from_lambda_role(self, lambda_name):
        try:
            iam = self.create_client('iam')
            response = iam.detach_role_policy(
                RoleName=self.create_lambda_role_name(lambda_name),
                PolicyArn=self.create_lambda_policy_arn(lambda_name)
            )
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def delete_lambda_policy(self, lambda_name):
        try:
            iam = self.create_client('iam')
            response = iam.delete_policy(
                PolicyArn=self.create_lambda_policy_arn(lambda_name)
            )
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))

    def delete_lambda_role(self, lambda_name):
        try:
            iam_resource = boto3.resource('iam')
            role = iam_resource.Role(self.create_lambda_role_name(lambda_name))
            role.delete()
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def delete_lambda_function(self, lambda_name):
        try:
            iam = self.create_client('iam')
            lambda_client = self.create_client('lambda')
            response = lambda_client.delete_function(
                FunctionName=lambda_name
            )
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def invoke_lambda_function(self, lambda_name, event_dict: dict):
        try:
            iam = self.create_client('iam')
            lambda_client = self.create_client('lambda')
            response = lambda_client.invoke(
                FunctionName=lambda_name,
                Payload=json.dumps(event_dict),
            )
            if 'Payload' in response:
                response['Payload'] = response['Payload'].read().decode("utf-8")
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))

    def unschedule_lambda_function(self, lambda_name):
        try:
            eventbridge_client = self.create_client('events')
            eventbridge_client.remove_targets(Rule=self.create_events_rule_name(lambda_name), Ids=[lambda_name])
            eventbridge_client.delete_rule(Name=self.create_events_rule_name(lambda_name))
            logger.info(f"Removed rule `{self.create_events_rule_name(lambda_name)}`")
        except botocore.exceptions.ClientError as e:
            logger.error(f"Failed to remove rule `{self.create_events_rule_name(lambda_name)}`: {e}")
            raise InfraHelperException(str(e))


    def schedule_lambda_function(self, lambda_name, interval_mins: int):
        try:
            eventbridge_client = self.create_client('events')
            unit = 'minute' if interval_mins == 1 else 'minutes'
            response = eventbridge_client.put_rule( Name=self.create_events_rule_name(lambda_name),
                                                    ScheduleExpression=f"rate({interval_mins} {unit})")
            logger.info(f"Put rule {self.create_events_rule_name(lambda_name)} with ARN {response['RuleArn']}")
            self.wait_for_aws(2)
            lambda_client = self.create_client('lambda')
            lambda_client.add_permission(   FunctionName=lambda_name,
                                            StatementId=f'{lambda_name}-invoke',
                                            Action='lambda:InvokeFunction',
                                            Principal='events.amazonaws.com',
                                            SourceArn=self.create_events_rule_arn(lambda_name)  )
            logger.info(f"Granted permission to let Amazon EventBridge call function `{lambda_name}")
            self.wait_for_aws(2)
            response = eventbridge_client.put_targets(  Rule=self.create_events_rule_name(lambda_name),
                                                        Targets=[{'Id': lambda_name, 'Arn': self.create_lambda_arn(lambda_name)}]  )
            if response['FailedEntryCount'] > 0:
                raise InfraHelperException(f"Couldn't set `{lambda_name}` as the target for rule `{self.create_events_rule_name(lambda_name)}`")
            else:
                logger.info(f"Set `{lambda_name}` as the target for rule `{self.create_events_rule_name(lambda_name)}`")
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def create_sns_topic_arn(self, topic_name):
        return f'arn:aws:sns:{self.region()}:{self.account_id()}:{topic_name}'

    def create_sns_topic(self, topic_name, policy_doc):
        try:
            sns_client = self.create_client('sns')
            response = sns_client.create_topic(
                Name=topic_name,
                Attributes={
                    'FifoTopic': "False",
                    'Policy': json.dumps(policy_doc)
                }
            )
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def create_sns_lambda_subscription(self, topic_name, lambda_name):
        try:
            lambda_client = self.create_client('lambda')
            response = lambda_client.add_permission(
                         StatementId='sns-invoke-lambda',
                         FunctionName=lambda_name,
                         Action='lambda:InvokeFunction',
                         Principal='sns.amazonaws.com',
                         SourceArn=self.create_sns_topic_arn(topic_name),
            )
            sns_client = self.create_client('sns')
            response = sns_client.subscribe(
                TopicArn=self.create_sns_topic_arn(topic_name),
                Protocol='lambda',
                Endpoint=self.create_lambda_arn(lambda_name)
            )
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def delete_sns_topic(self, topic_name):
        try:
            sns_client = self.create_client('sns')
            # remove subscriptions
            for subscription in sns_client.list_subscriptions_by_topic(TopicArn=self.create_sns_topic_arn(topic_name)).get('Subscriptions'):
                sns_client.unsubscribe(SubscriptionArn=subscription.get('SubscriptionArn'))
            response = sns_client.delete_topic(
                TopicArn=self.create_sns_topic_arn(topic_name)
            )
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))

    def create_cloudwatch_alarm_arn(self, alarm_name):
        return f'arn:aws:cloudwatch:{self.region()}:{self.account_id()}:alarm:{alarm_name}'


    def create_cloudwatch_alarm(self, alarm_name, alarm_fields: dict):
        try:
            cloudwatch_client = self.create_client('cloudwatch')
            alarm_fields = {**alarm_fields, **{'AlarmName': alarm_name}}
            response = cloudwatch_client.put_metric_alarm(**alarm_fields)
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))

    def wait_for_cloudwatch_alarms(self, alarm_names):
        try:
            logger.info(f"Waiting for cloudwatch alarms `{alarm_names}` ...")
            cloudwatch_client = self.create_client('cloudwatch')
            waiter = cloudwatch_client.get_waiter('alarm_exists')
            waiter.wait(AlarmNames=alarm_names)
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def delete_cloudwatch_alarms(self, alarm_names):
        try:
            cloudwatch_client = self.create_client('cloudwatch')
            response = cloudwatch_client.delete_alarms(AlarmNames=alarm_names)
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))


    def get_metric_widget_image(self, widget_dict):
        try:
            cloudwatch_client = self.create_client('cloudwatch')
            response = cloudwatch_client.get_metric_widget_image(
                MetricWidget=json.dumps(widget_dict)
            )
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))

    def create_secret_name(self, lambda_name):
        return f'{lambda_name}_secret'

    def create_secret_arn_prefix(self, lambda_name):
        return f'arn:aws:secretsmanager:{self.region()}:{self.account_id()}:secret:{self.create_secret_name(lambda_name)}'

    def create_secret(self, secret_name, secret_value):
        try:
            secret_client = self.create_client('secretsmanager')
            response = secret_client.create_secret(
                Name=secret_name,
                SecretString=secret_value,
                ForceOverwriteReplicaSecret=True,
            )
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))

    def update_secret(self, secret_name, secret_value):
        try:
            secret_client = self.create_client('secretsmanager')
            response = secret_client.update_secret(
                SecretId=secret_name,
                SecretString=secret_value,
            )
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))

    def delete_secret(self, secret_name):
        try:
            secret_client = self.create_client('secretsmanager')
            response = secret_client.delete_secret(
                SecretId=secret_name,
                ForceDeleteWithoutRecovery=True
            )
            return response
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))
