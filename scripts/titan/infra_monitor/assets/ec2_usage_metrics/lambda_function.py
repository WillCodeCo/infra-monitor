#!/usr/bin/env python3

import json
import os
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class LambdaHelper:

    def get_current_region():
        return os.environ['AWS_REGION']

class CustomCloudWatchMetrics:

    @classmethod
    def publish(cls, metric_dicts):
        num_metrics_published = 0
        MAX_METRICS_PER_CALL = 20
        cloudwatch = boto3.client('cloudwatch')
        for metric_chunk in [metric_dicts[i:i+MAX_METRICS_PER_CALL] for i in range(0, len(metric_dicts), MAX_METRICS_PER_CALL)]:
            response = cloudwatch.put_metric_data(  MetricData=metric_chunk,
                                                    Namespace='InfraMonitor' )

            try:
                assert response['ResponseMetadata']['HTTPStatusCode'] == 200
                num_metrics_published += len(metric_chunk)
            except (KeyError, AssertionError) as e:
                logger.error(f"Failed to publish metrics: {response}")
                break
        return num_metrics_published




class Ec2ResourceQuery:

    @classmethod
    def gen_instance_dicts(cls):
        ec2 = boto3.resource('ec2')
        for instance in ec2.instances.all():
            yield { 'id': instance.id,
                    'type': instance.instance_type,
                    'state': instance.state['Name'] }

    @classmethod
    def gen_instance_count_metrics(cls):
        per_state_count = {
            'pending': 0,
            'running': 0,
            'stopping': 0,
            'stopped': 0,
            'shutting-down': 0,
            'terminated': 0,
        }
        per_state_and_type_count = {}
        for instance_dict in cls.gen_instance_dicts():
            per_state_count[instance_dict['state']] += 1
            try:
                per_state_and_type_count[(instance_dict['type'], instance_dict['state'])] += 1
            except KeyError:
                per_state_and_type_count[(instance_dict['type'], instance_dict['state'])] = 1
        for i_state, count in per_state_count.items():
            yield {
                'MetricName': 'InstanceCountPerState',
                'Dimensions': [
                    {
                        'Name': 'InstanceState',
                        'Value': i_state
                    }
                ],
                'Unit': 'None',
                'Value': count
            }
        for i_type, i_state in per_state_and_type_count.keys():
            yield {
                'MetricName': 'InstanceCountPerStateAndType',
                'Dimensions': [
                    {
                        'Name': 'InstanceType',
                        'Value': i_type
                    },
                    {
                        'Name': 'InstanceState',
                        'Value': i_state
                    }
                ],
                'Unit': 'None',
                'Value': per_state_and_type_count[(i_type, i_state)]
            }


def lambda_handler(event, context):
    instance_count_metrics = list(Ec2ResourceQuery.gen_instance_count_metrics())
    num_metrics_published = CustomCloudWatchMetrics.publish(instance_count_metrics)
    return {
        'success': (num_metrics_published == len(instance_count_metrics)),            
        'received-metrics': len(instance_count_metrics),
        'published-metrics': num_metrics_published
    }