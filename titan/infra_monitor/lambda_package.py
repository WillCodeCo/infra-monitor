import pathlib
import logging
import zipfile
import tempfile
import os
import json


logger = logging.getLogger(__name__)


class LambdaDependenciesPackage:

    @classmethod
    def create(cls, requirements: str, output_path: str):
        with tempfile.TemporaryDirectory() as working_dir:
            working_dir = pathlib.Path(working_dir)
            with open(working_dir / 'requirements.txt', 'w') as f:
                f.write(requirements)
            os.system(f"pip install -r {working_dir / 'requirements.txt'} --target {working_dir / 'deps'}")
            os.system(f"cd {working_dir / 'deps'} && zip -r {output_path} .")

class LambdaPackage:

    @classmethod
    def add_file_to_zip(cls, zip_file_path, some_file_path):
        zip = zipfile.ZipFile(zip_file_path, 'a')
        zip.write(some_file_path, os.path.basename(some_file_path))
        zip.close()

    @classmethod
    def create_package_bytes(cls, package_path):
        requirements = open(package_path / 'requirements.txt', 'r').read()
        # create and merge with dependencies
        with tempfile.TemporaryDirectory() as working_dir:
            working_path = pathlib.Path(working_dir)
            result_zip_path = working_path / 'package.zip'
            if requirements:
                LambdaDependenciesPackage.create(requirements, result_zip_path)
            cls.add_file_to_zip(result_zip_path, package_path / 'lambda_function.py')
            cls.add_file_to_zip(result_zip_path, package_path / '__main__.py')
            return open(result_zip_path, 'rb').read()


    @classmethod
    def execute_package(cls, package_bytes: bytes, event_dict: json):
        with tempfile.TemporaryDirectory() as working_dir:
            working_path = pathlib.Path(working_dir)
            with open(working_path / 'lambda_package.zip', 'wb') as f:
                f.write(package_bytes)
            with open(working_path / 'event.json', 'w') as f:
                f.write(json.dumps(event_dict))
            os.system(f"cd {working_path} && python {working_path / 'lambda_package.zip'} -e {working_path / 'event.json'}")