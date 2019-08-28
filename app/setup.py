# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name='consumer-app',
    version='0.0.1',
    description='Surrogate consumer using leader-app semantics',
    packages=find_packages(exclude=('tests', 'docs'))
)
