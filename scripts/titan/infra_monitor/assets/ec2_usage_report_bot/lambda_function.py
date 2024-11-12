#!/usr/bin/env python3

import json
import typing
import os
import csv
import zipfile
import logging
import botocore
import tempfile
import pathlib
import boto3
from datetime import datetime
import enum
import slack_sdk

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class InfraHelperException(Exception):
    pass

class InfraHelper:

    REGIONS = { 'us-east-2',
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
                'sa-east-1' }


    def __init__(self, region):
        self._region = region
        self._clients = {}        
        self._boto_session = None

    def region(self):
        return self._region

    def boto_session(self):
        if not self._boto_session:
            self._boto_session = boto3.session.Session(region_name=self.region())
        return self._boto_session

    def boto_config(self):
        return botocore.config.Config(region_name=self.region())

    def create_resource(self, resource_name):
        return self.boto_session().resource(resource_name)

    def create_client(self, client_name):
        try:
            return self._clients[client_name]
        except KeyError:
            self._clients[client_name] = self.boto_session().client(client_name)
            return self._clients[client_name]

    def get_metric_widget_image(self, widget_dict):
        try:
            cloudwatch_client = self.create_client('cloudwatch')
            response = cloudwatch_client.get_metric_widget_image(
                MetricWidget=json.dumps(widget_dict)
            )
            return response['MetricWidgetImage']
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))
        except KeyError:
            raise InfraHelperException('Invalid response for get_metric_widget_image request')

    def get_secret_value(self, secret_name):
        try:
            secret_client = self.create_client('secretsmanager')
            response = secret_client.get_secret_value(
                SecretId=secret_name
            )
            return response['SecretString']
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))

    def get_file_from_s3(self, bucket, object_key, output_path):
        try:
            s3_client = self.create_client('s3')
            s3_client.download_file(bucket, object_key, output_path)
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))

    def gen_instance_dicts(self):
        try:
            ec2 = self.create_resource('ec2')
            for instance in ec2.instances.all():
                yield { 'id': instance.id,
                        'type': instance.instance_type,
                        'state': instance.state['Name'] }
        except botocore.exceptions.ClientError as e:
            raise InfraHelperException(str(e))

    def gen_instance_counts(self):
        per_state_and_type_count = {}
        for instance_dict in self.gen_instance_dicts():
            try:
                per_state_and_type_count[(instance_dict['type'], instance_dict['state'])] += 1
            except KeyError:
                per_state_and_type_count[(instance_dict['type'], instance_dict['state'])] = 1
        for i_type, i_state in per_state_and_type_count.keys():
            yield { 'type': i_type,
                    'state': i_state,
                    'count': per_state_and_type_count[(i_type, i_state)] }



class ReportPeriod(enum.Enum):
    LAST_HOUR = "LAST_HOUR"
    LAST_8_HOURS = "LAST_8_HOURS"
    LAST_24_HOURS = "LAST_24_HOURS"
    LAST_WEEK = "LAST_WEEK"
    LAST_MONTH = "LAST_MONTH"
    LAST_3_MONTHS = "LAST_3_MONTHS"

class ReportType(enum.Enum):
    REALTIME_EC2_USAGE_REPORT = 'REALTIME_EC2_USAGE_REPORT'
    EC2_USAGE_REPORT = 'EC2_USAGE_REPORT'
    BILLING_REPORT = 'BILLING_REPORT'
    AWS_BUDGET_NOTIFICATION = 'AWS_BUDGET_NOTIFICATION'



class Attachment:

    def __init__(self, attachment_name: str, attachment_bytes: bytes):
        self._attachment_name = attachment_name
        self._attachment_bytes = attachment_bytes

    def attachment_name(self):
        return self._attachment_name

    def attachment_bytes(self):
        return self._attachment_bytes


class Report:

    def __init__(self, title, body, attachments: typing.Tuple[Attachment, ...], fields={}):
        self._title = title
        self._body = body
        self._attachments = attachments
        self._fields = fields

    def title(self):
        return self._title

    def body(self):
        return self._body

    def attachments(self):
        return self._attachments

    def fields(self):
        return self._fields

    def save_to_filesystem(self, output_path):
        output_path = pathlib.Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        with open(output_path / 'report-fields.json', 'w') as f:
            f.write(json.dumps({'title': self.title(), 'body': self.body()}, indent=4))
        for attachment in self.attachments():
            with open(output_path / attachment.attachment_name(), 'wb') as f:
                f.write(attachment.attachment_bytes())



class Ec2RealtimeUsageReport(Report):

    @classmethod
    def create(cls, infra_helper):
        report_text = 'TYPE'.ljust(20) + 'STATE'.ljust(12) + 'COUNT'.ljust(5) + '\n' + ('-'*37) + '\n'
        for instance_dict in infra_helper.gen_instance_counts():
            report_text += f"{instance_dict['type'].ljust(20)}{instance_dict['state'].ljust(12)}{str(instance_dict['count']).ljust(5)}\n"

        body = (f"```" +
                report_text +
                f"```")

        return Report(  title=f'Listing of instances in {infra_helper.region()}',
                        body=body,
                        attachments=[] )

class Ec2UsageReport(Report):

    PERIOD_LOOKUP = {
        ReportPeriod.LAST_HOUR: "-PT1H",
        ReportPeriod.LAST_8_HOURS: "-PT8H",
        ReportPeriod.LAST_24_HOURS: "-PT24H",
        ReportPeriod.LAST_WEEK: "-PT168H",
        ReportPeriod.LAST_MONTH: "-PT720H",
        ReportPeriod.LAST_3_MONTHS: "-PT2160H"
    }

    INSTANCE_TYPES = [  'c5a.16xlarge',
                        'c5a.xlarge'  ]

    @classmethod
    def human_period_string(cls, report_period: ReportPeriod):
        if report_period == ReportPeriod.LAST_HOUR:
            return "past hour"
        elif report_period == ReportPeriod.LAST_8_HOURS:
            return "past 8 hours"
        elif report_period == ReportPeriod.LAST_24_HOURS:
            return "past 24 hours"
        elif report_period == ReportPeriod.LAST_WEEK:
            return "past week"
        elif report_period == ReportPeriod.LAST_MONTH:
            return "past month"
        elif report_period == ReportPeriod.LAST_3_MONTHS:
            return "past 3 months"

    @classmethod
    def create_cloud_metric_widget_dict(cls, region: str, report_period: ReportPeriod):
        metrics = [["InfraMonitor", "InstanceCountPerStateAndType", "InstanceState", "running", "InstanceType", i_type] for i_type in cls.INSTANCE_TYPES]
        return {
            "metrics": metrics,
            "view": "timeSeries",
            "stacked": True,
            "region": region,
            "title": f"Running Instances in {region}",
            "period": 60,
            "stat": "Average",
            "yAxis": {
                "left": {
                    "showUnits": False,
                    "label": "Number of instances"
                }
            },
            "start": cls.PERIOD_LOOKUP[report_period],
            "width": 1280,
            "height": 380,
        }


    @classmethod
    def create(cls, infra_helper: InfraHelper, report_period: ReportPeriod):
        metric_widget_dict = cls.create_cloud_metric_widget_dict(   region=infra_helper.region(),
                                                                    report_period=report_period  )
        report_name = f"{infra_helper.region()}-usage-{report_period.value.lower()}"
        return Report(  title=f'EC2 Usage in {infra_helper.region()} for the {cls.human_period_string(report_period)}',
                        body='',
                        attachments=[Attachment(attachment_name=f"{report_name}.png",
                                                attachment_bytes=infra_helper.get_metric_widget_image(metric_widget_dict))] )

class BillingReport(Report):

    ACCOUNT_ID = '097039683978'
    S3_BUCKET = 'aws-billing-reports-097039683978'

    MONTHS = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    @classmethod
    def current_year_month(cls):
        return (datetime.now().year, datetime.now().month)

    @classmethod
    def next_year_month(cls):
        if datetime.now().month == 12:
            return (datetime.now().year + 1, 1)
        else:
            return (datetime.now().year, datetime.now().month + 1)

    @classmethod
    def current_billing_period(cls):
        cur_year, cur_month = cls.current_year_month()
        next_year, next_month = cls.next_year_month()
        return f"{cur_year}{cur_month:>02}01-{next_year}{next_month:>02}01"

    @classmethod
    def billing_period_human_string(cls):
        cur_year, cur_month = cls.current_year_month()
        next_year, next_month = cls.next_year_month()
        return f"01-{cls.MONTHS[cur_month]}-{cur_year} to 01-{cls.MONTHS[next_month]}-{next_year}"

    @classmethod
    def current_billing_report_key(cls):
        return f"reports/AwsCostOverview/{cls.current_billing_period()}/AwsCostOverview-00001.csv.zip"

    @classmethod
    def current_billing_report_file_name(cls):
        return f"aws-cost-usage-{cls.current_billing_period()}.csv.zip"

    @classmethod
    def fetch_billing_report_bytes(cls, infra_helper):
        with tempfile.TemporaryDirectory() as working_dir:
            working_dir = pathlib.Path(working_dir)
            infra_helper.get_file_from_s3(  bucket=cls.S3_BUCKET,
                                            object_key=cls.current_billing_report_key(),
                                            output_path=str(working_dir / cls.current_billing_report_file_name())  )
            with open(working_dir / cls.current_billing_report_file_name(), 'rb') as f:
                return f.read()


    @classmethod
    def parse_per_period_spend(cls, billing_report_bytes):
        cur_year, cur_month = cls.current_year_month()
        next_year, next_month = cls.next_year_month()
        periods = [
            {
                'name': 'this-month',
                'start': datetime(cur_year, cur_month, 1),
                'end': datetime(next_year, next_month, 1)
            }
        ]
        if datetime.now().day > 3:
            periods.append({
                'name': f"day-{datetime.now().day - 3:>02}",
                'start': datetime(datetime.now().year, datetime.now().month, datetime.now().day - 3),
                'end': datetime(datetime.now().year, datetime.now().month, datetime.now().day - 2)
            })
        if datetime.now().day > 2:
            periods.append({
                'name': f"day-{datetime.now().day - 2:>02}",
                'start': datetime(datetime.now().year, datetime.now().month, datetime.now().day - 2),
                'end': datetime(datetime.now().year, datetime.now().month, datetime.now().day - 1)
            })
        if datetime.now().day > 1:
            periods.append({
                'name': f"day-{datetime.now().day - 1:>02}",
                'start': datetime(datetime.now().year, datetime.now().month, datetime.now().day - 1),
                'end': datetime(datetime.now().year, datetime.now().month, datetime.now().day)
            })
        try:
            per_period_spend = {}

            with tempfile.TemporaryDirectory() as working_dir:
                working_dir = pathlib.Path(working_dir)

                path_to_zip_file = working_dir / 'AwsCostOverview-00001.csv.zip'
                with open(path_to_zip_file, 'wb') as f:
                    f.write(billing_report_bytes)
                with zipfile.ZipFile(path_to_zip_file, 'r') as zipf:
                    zipf.extractall(working_dir)
                with open(working_dir / 'AwsCostOverview-00001.csv', 'r') as f:
                    csv_reader = csv.DictReader(f, delimiter=',', quotechar='"')
                    for item in csv_reader:
                        
                        if item['lineItem/UsageAccountId'] != cls.ACCOUNT_ID:
                            continue
                        start_date = datetime.strptime(item['lineItem/UsageStartDate'], '%Y-%m-%dT%H:%M:%SZ')
                        end_date = datetime.strptime(item['lineItem/UsageEndDate'], '%Y-%m-%dT%H:%M:%SZ')
                        for period in periods:
                            if start_date >= period['start'] and end_date <= period['end']:
                                try:
                                    per_period_spend[period['name']] += float(item['lineItem/UnblendedCost'])
                                except KeyError:
                                    per_period_spend[period['name']] = float(item['lineItem/UnblendedCost'])

            return {period['name']: per_period_spend[period['name']] for period in periods if period['name'] in per_period_spend}
        except TypeError:
            raise ValueError(f"failed to parse billing report csv !")


    @classmethod
    def create(cls, infra_helper: InfraHelper):
        logger.info(f"Fetching the cost & usage csv file from S3 ...")
        report_bytes = cls.fetch_billing_report_bytes(infra_helper)
        logger.info(f"Extracting and parsing the cost & usage csv file ...")
        per_period_spend = cls.parse_per_period_spend(report_bytes)
        logger.info(f"Creating the billing report ...")
        report_text = 'PERIOD'.ljust(20) + 'SPEND'.ljust(15) + '\n' + ('-'*35) + '\n'
        for period_name, spend in per_period_spend.items():
            report_text += f"{period_name.ljust(20)}${spend:<15.2f}\n"

        body = (f"```" +
                report_text +
                f"```")



        return Report(  title=f'AWS Cost & Usage Report for {cls.billing_period_human_string()} has been updated.',
                        body=body,
                        attachments=[Attachment(attachment_name=cls.current_billing_report_file_name(),
                                                attachment_bytes=cls.fetch_billing_report_bytes(infra_helper))] )



class BudgetNotificationReport(Report):

    @classmethod
    def create(cls, subject, message):
        fields = {}
        for k, v in [tuple(l.split(':')) for l in message.splitlines() if ':' in l]:
            if all(c.isalpha() or c.isspace() for c in k):
                fields[k] = v.strip()
        return Report(  title=':exclamation: AWS Budget Alert !',
                        body='',
                        attachments=[],
                        fields=fields )



class ReportFactory:

    @classmethod
    def gen_reports(cls, report_spec: dict):
        report_type = ReportType(report_spec['report_type'].upper())
        if report_type == ReportType.EC2_USAGE_REPORT:
            report_period = ReportPeriod(report_spec['report_period'].upper())
            for report_region in report_spec['report_regions']:
                infra_helper = InfraHelper(region=report_region)
                yield Ec2UsageReport.create(infra_helper=InfraHelper(region=report_region),
                                            report_period=report_period)
        elif report_type == ReportType.REALTIME_EC2_USAGE_REPORT:
            for report_region in report_spec['report_regions']:
                infra_helper = InfraHelper(region=report_region)
                yield Ec2RealtimeUsageReport.create(infra_helper)
        elif report_type == ReportType.BILLING_REPORT:
            yield BillingReport.create(infra_helper=InfraHelper(region=os.environ['AWS_REGION']))
        elif report_type == ReportType.AWS_BUDGET_NOTIFICATION:
            yield BudgetNotificationReport.create(  subject=report_spec['subject'],
                                                    message=report_spec['message'] )


class ReportPublisherException(Exception):
    pass

class ReportPublisher:
    pass


class ReportPublisherToSlack(ReportPublisher):
    
    def  __init__(self, slack_token, slack_channel):
        self._slack_token = slack_token
        self._slack_channel = slack_channel
        self._slack_client = None

    def slack_token(self):
        return self._slack_token

    def slack_channel(self):
        return self._slack_channel

    def slack_client(self):
        if not self._slack_client:
            self._slack_client = slack_sdk.WebClient(token=self.slack_token())
        return self._slack_client
        

    @classmethod
    def gen_field_value_blocks(cls, fields: dict):
        field_values = list(fields.items())
        if (len(field_values) % 2):
            field_values.append((' ', ' '))
        while field_values:
            k1, v1 = field_values.pop(0)
            k2, v2 = field_values.pop(0)
            yield {
                "type": "section",
                "text": {
                    "text": " ",
                    "type": "plain_text"
                },
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*{k1}*"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*{k2}*"
                    },
                    {
                        "type": "plain_text",
                        "text": v1
                    },
                    {
                        "type": "plain_text",
                        "text": v2
                    }
                ]
            }


    @classmethod
    def create_slack_message_dict(cls, slack_channel, report: Report):
        result = {
            "channel": slack_channel,
            "text": report.title(),
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": report.title()
                    }
                }
            ]
        }
        if report.body():
            result['blocks'].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": report.body()
                }
            })
        if report.fields():
            for field_block in cls.gen_field_value_blocks(report.fields()):
                result['blocks'].append(field_block)
        return result



    def publish(self, report: Report):
        try:
            message_dict = self.create_slack_message_dict(  slack_channel=self.slack_channel(),
                                                            report=report )
            self.slack_client().chat_postMessage(**message_dict)

            for attachment in report.attachments():
                file_type = pathlib.Path(attachment.attachment_name()).suffix
                response = self.slack_client().files_upload(
                    channels=self.slack_channel(),
                    file=attachment.attachment_bytes(),
                    filename=attachment.attachment_name(),
                    title=attachment.attachment_name(),
                    filetype=file_type
                )
        except slack_sdk.errors.SlackApiError as e:
            raise ReportPublisherException(f"Failed to publish report to slack channel: {e}")




class ReportSpecFactory:

    @classmethod
    def create_from_event(cls, event, context):        
        try:
            event_source = event['Records'][0]['EventSource']
            if event_source == 'aws:sns':
                sns_event = event['Records'][0]['Sns']
                if sns_event['Subject'].startswith('AWS Budgets'):
                    return {
                        "report_type": "AWS_BUDGET_NOTIFICATION",
                        "subject": sns_event['Subject'],
                        "message": sns_event['Message']
                    }
                elif sns_event['Subject'].startswith('ALARM'):
                    message_dict = json.loads(sns_event['Message'])
                    alarm_arn = message_dict['AlarmArn']
                    try:
                        region = next(region for region in InfraHelper.REGIONS if region in alarm_arn)
                    except StopIteration:
                        raise ValueError(f"Failed to parse region from alarm_arn")
                    return {
                        "report_type": "EC2_USAGE_REPORT",
                        "report_regions": [region],
                        "report_period": "LAST_8_HOURS"
                    }
                elif sns_event['Subject'].startswith('Amazon S3'):
                    message_dict = json.loads(sns_event['Message'])
                    s3_event = message_dict['Records'][0]['s3']
                    bucket_name = s3_event['bucket']['name']
                    object_key = s3_event['object']['key']
                    if (bucket_name == BillingReport.S3_BUCKET) and (object_key == BillingReport.current_billing_report_key()):
                        return {
                            "report_type": "BILLING_REPORT"
                        }


            raise ValueError(f"Could not determine a report_spec for event: {json.dumps(event, indent=4)}")
        except KeyError as e:
            raise ValueError(f"Could not interpret event into a report_spec. Missing field `{e}`")



class ArgValidator:
    
    @classmethod
    def ensure_valid_report_region(cls, report_region: str):
        assert report_region in InfraHelper.REGIONS, f"Invalid region `{report_region}`"

    @classmethod
    def ensure_valid_report_period(cls, report_spec: dict):
        assert 'report_period' in report_spec, f"`report_period` not present in report_spec"
        assert type(report_spec['report_period']) == str, f"invalid type of `report_period` in report_spec"  
        try:
            ReportPeriod(report_spec['report_period'].upper())
        except ValueError:
            raise ValueError(f"Invalid report_period `{report_spec['report_period']}`")

    @classmethod
    def ensure_valid_report_type(cls, report_spec: dict):
        assert 'report_type' in report_spec, f"`report_type` not present in report_spec"
        assert type(report_spec['report_type']) == str, f"invalid type of `report_type` in report_spec"  
        try:
            ReportType(report_spec['report_type'].upper())
        except ValueError:
            raise ValueError(f"Invalid report_type `{report_spec['report_type']}`")

    @classmethod
    def ensure_valid_report_regions(cls, report_spec: dict):
        assert 'report_regions' in report_spec, f"`report_regions` not present in report_spec"
        assert type(report_spec['report_regions']) == list, f"`report_regions` must be a list of strings"
        for report_region in report_spec['report_regions']:
            cls.ensure_valid_report_region(report_region)

    @classmethod
    def ensure_valid_report_spec(cls, report_spec: dict):
        cls.ensure_valid_report_type(report_spec)
        if ReportType(report_spec['report_type'].upper()) == ReportType.EC2_USAGE_REPORT:
            cls.ensure_valid_report_period(report_spec)
            cls.ensure_valid_report_regions(report_spec)
        elif ReportType(report_spec['report_type'].upper()) == ReportType.REALTIME_EC2_USAGE_REPORT:
            assert 'report_period' not in report_spec, f"`report_period` not expected for this report_type"
            cls.ensure_valid_report_regions(report_spec)
        else:
            assert 'report_period' not in report_spec, f"`report_period` not expected for this report_type"




def lambda_handler(event, context):
    logger.info(f"Lambda invoked with event: {event}")

    if 'report_type' in event:
        report_spec = event
    else:
        report_spec = ReportSpecFactory.create_from_event(event, context)

    ArgValidator.ensure_valid_report_spec(report_spec)

    infra_helper = InfraHelper(region=os.environ['AWS_REGION'])
    slack_config = json.loads(infra_helper.get_secret_value('ec2_usage_report_bot_secret'))
    report_publisher = ReportPublisherToSlack(  slack_token=slack_config['slack-token'],
                                                slack_channel=slack_config['slack-channel']  )
    i = None
    for i, report in enumerate(ReportFactory.gen_reports(report_spec)):
        report_publisher.publish(report)

    return {
        'success': (i is not None),
    }