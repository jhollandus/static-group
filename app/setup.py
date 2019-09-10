# -*- coding: utf-8 -*-
from setuptools import find_packages
from setuptools import setup

requirements = ['confluent-kafka>=1.0.1', 'kazoo>=2.6.1']

with open('./test-requirements.txt') as test_reqs:
    test_requirements = [line for line in test_reqs]

setup(
    name='static_assignment',
    version='0.0.1',
    description='Use static assignments with your kafka consumer group',
    packages=find_packages(exclude=('test')),
    include_package_data=True,
    install_requires=requirements,
    tests_require=test_requirements,
    entry_points={'console_scripts': ['static-consumer = static_assignment.consumer:main']},
)
