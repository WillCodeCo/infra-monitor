import typing
import pathlib
import logging
import argparse
import json
from titan.infra_monitor import (
    InfraHelper,
    LambdaPackage
)
from importlib import resources


logger = logging.getLogger(__name__)



class LambdaExecutor:

    def __init__(self, region: str, account_id: str):
        self._region = region
        self._account_id = account_id

    def region(self):
        return self._region

    def account_id(self):
        return self._account_id

    def invoke(self, lambda_name: str, event_dict: dict):
        infra_helper = InfraHelper( region=self.region(),
                                    account_id=self.account_id() )
        response = infra_helper.invoke_lambda_function( lambda_name=lambda_name,
                                                        event_dict=event_dict )
        print(json.dumps(response, indent=4))



class LocalLambdaExecutor(LambdaExecutor):
    
    def invoke(self, lambda_name: str, event_dict: dict):
        with resources.path(f"scripts.titan.infra_monitor.assets.{lambda_name}", "__init__.py") as p:
            package_path = pathlib.Path(p).parent
            package_bytes = LambdaPackage.create_package_bytes(package_path)
            LambdaPackage.execute_package(  package_bytes=package_bytes,
                                            event_dict=event_dict  )




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
    def ensure_valid_account_id(cls, account_id: str):
        try:
            assert str(int(account_id)) == account_id
        except Exception as e:
            raise ValueError(f"Invalid account_id")

    @classmethod
    def ensure_valid_json_file_path(cls, json_file_path: str):
        try:
            with open(json_file_path, 'r') as f:
                json.loads(f.read())
        except:
            raise ValueError(f"Invalid JSON file path `{json_file_path}` !")

def main():
    parser = argparse.ArgumentParser(description="Run a lambda function")
    parser.add_argument("-n", "--lambda-name", type=str, required=True, help="Lambda Function Name")
    parser.add_argument("-r", "--region", type=str, required=True, help="Region to deploy to")
    parser.add_argument("-a", "--account-id", type=str, required=True, help="AWS account id")
    parser.add_argument("-e", "--event-file", type=str, required=False, help="Path to json file containing event")
    parser.add_argument("-l", "--local", action='store_true', default=False, required=False, help="Execute lambda locally")
    args = parser.parse_args()

    # configure the logger
    logging.basicConfig(level=logging.INFO)


    try:
        ArgValidator.ensure_valid_region(args.region)
        ArgValidator.ensure_valid_account_id(args.account_id)
        if args.event_file:
            ArgValidator.ensure_valid_json_file_path(args.event_file)

        Executor = LocalLambdaExecutor if args.local else LambdaExecutor
        if args.event_file:
            with open(args.event_file, 'r') as f:
                Executor( region=args.region,
                                account_id=args.account_id ).invoke(lambda_name=args.lambda_name,
                                                                    event_dict=json.loads(f.read()))
        else:
            Executor(   region=args.region,
                        account_id=args.account_id ).invoke(lambda_name=args.lambda_name,
                                                            event_dict={})
    except Exception as e:
        print(f"Failed due to exception: {e}")
        raise


if __name__ == "__main__"   :
    main()

