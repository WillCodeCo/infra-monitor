import pathlib
import logging
import argparse
import json
from lambda_function import lambda_handler

logger = logging.getLogger(__name__)



class ArgValidator:
    
    @classmethod
    def ensure_valid_json_file_path(cls, json_file_path: str):
        try:
            with open(json_file_path, 'r') as f:
                json.loads(f.read())
        except:
            raise ValueError(f"Invalid JSON file path `{json_file_path}` !")


def main():
    parser = argparse.ArgumentParser(description="Local Runner For Lambda Function")
    parser.add_argument("-e", "--event-file", type=str, required=False, help="Path to json file containing event")
    args = parser.parse_args()

    # configure the logger
    logging.basicConfig(level=logging.INFO)


    try:
        ArgValidator.ensure_valid_json_file_path(args.event_file)

        with open(args.event_file, 'r') as f:
            response = lambda_handler(  event=json.loads(f.read()),
                                        context={} )
            print(json.dumps(response, indent=4))

    except Exception as e:
        print(f"Failed due to exception: {e}")
        raise


if __name__ == "__main__"   :
    main()
