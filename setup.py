from setuptools import setup, find_namespace_packages

setup(
    name="infra-monitor",
    install_requires=[
        'boto3'
    ],
    description="Infrastructure Monitoring Tool",
    packages=find_namespace_packages(include=['titan.*', 'scripts.*']),
    entry_points={
        'console_scripts': [
            'deploy_infra_monitor=scripts.titan.infra_monitor.deploy_infra_monitor:main',
            'invoke_lambda=scripts.titan.infra_monitor.invoke_lambda:main',
        ]
    }
)
