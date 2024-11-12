# infra-monitor

## Install the `infra-monitor` package

```
$ git clone git+ssh://git@github.com/WillCodeCo/infra-monitor.git
$ cd infra-monitor
$ virtualenv .env
$ source .env/bin/activate
$ .env) pip install -e .
```

## Customize the source code if needed

- In order to change which instance types are used in the graph plots, you need to adjust the value `INSTANCE_TYPES` of class `Ec2UsageReport`
  - in `scripts/titan/infra_monitor/assets/ec2_usage_report_bot/lambda_function.py`


## Setup Slack

- We need a slack channel identifier where the lambda will deliver reports to
- We need a token with which our lambda can authenticate with Slack

Create a new app https://api.slack.com/apps (From Scratch)

- Give it the following permissions: `chat:write, files:write, im:write`
- install to your workspace
- remember the `Bot User OAuth Token` as `SLACK_TOKEN`

Create a slack channel for the reports to be delivered to
  - Add the app to it
  - remember the `CHANNEL_ID`



## Automated deployment to a region

```
usage: deploy_infra_monitor [-h] -r REGION -a ACCOUNT_ID [-f] [-u]
```

- `-u` Uninstall infra-monitor AWS resources
- `-f` Perform an uninstall first before deploying


For example:

```
EXPORT SLACK_TOKEN=XXXXXXXXXX
EXPORT SLACK_CHANNEL=XXXXXXXXXX
$ .env) deploy_infra_monitor -r REGION -a ACCOUNT_ID -f
```


## Manual deployment steps



### S3 Bucket & Event notifications

- Create a bucket `aws-billing-reports-{ACCOUNT-ID}`
- Create an event notification `BillingReportUpdatedEvent`
  - suffix filter: `.csv.zip`
  - event type: `All object create events`
  - destination: `arn:aws:sns:{BUCKET-REGION}:{ACCOUNT-ID}:infra-monitor`


### Daily cost & usage report to upload to bucket

This will deliver a zipped .csv file to S3 at least every day

- https://console.aws.amazon.com/billing/home?region=us-east-1#/reports
- deliver to bucket: `aws-billing-reports-{ACCOUNT-ID}`

### Budget reports & alerts

Create a budget and alarms based on thresholds.

- destination SNS: `arn:aws:sns:{REGION}:{ACCOUNT-ID}:infra-monitor`


### (Optional) Create AWS Chatbot

This allows the AWS slack bot to invoke lambdas and create reports, so in Slack a user can send the following message:

```
@aws lambda invoke --function-name ec2_usage_report_bot --region us-west-1 --payload {"report_type": "EC2_USAGE_REPORT", "report_period": "LAST_8_HOURS", "report_regions": ["us-west-2", "ap-east-1"]}
```

The lambda will be invoked and a usage report graph will be delivered to the previously configured slack channel.

#### Create chat bot policy: `InfraReportChatBotPolicy`

- as follows:
  ```
  {
      "Version": "2012-10-17",
      "Statement": [
          {
              "Action": "lambda:InvokeFunction",
              "Effect": "Allow",
              "Resource": "arn:aws:lambda:us-west-1:{{ACCOUNT-ID}}:function:ec2_usage_report_bot"
          }
      ]
  }
  ```

#### Create chat bot role: ``InfraReportChatBotRole``

- With custom trust policy:

  ```
  {
      "Version": "2012-10-17",
      "Statement": [
          {
              "Effect": "Allow",
              "Principal": {
                  "Service": "chatbot.amazonaws.com"
              },
              "Action": "sts:AssumeRole"
          }
      ]
  }
  ```

- Add permissions (select previous policy)
- Call it `InfraReportChatBotRole`


#### Configure new client

- Configure new client at https://us-east-2.console.aws.amazon.com/chatbot/home?region=us-east-2#/chat-clients
- Go through slack authorization wizard

#### Configure channel

- This channel can be different to the one where the reports are delivered as it keeps things tidier

- Configure new channel at https://us-east-2.console.aws.amazon.com/chatbot/home?region=us-east-2#/chat-clients/slack/workspaces/T02RXJFKNMB/configurations
  - get the channel ID from slack
  - use `InfraReportChatBotRole` for the role
  - use `InfraReportChatBotPolicy` for guard rail

