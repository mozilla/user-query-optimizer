import codecs
import os
import re

from setuptools import setup, find_packages

setup(
    name='user-query-optimizer',
    version='0.1.1',
    author='Swathi Iyer',
    author_email='swathii@stanford.edu',
    packages=['user_query_optimizer'],
    url='https://github.com/mozilla/user-query-optimizer',
    license='LICENSE.txt',
    description='SQL query optimization hints',
    long_description=open('README.md').read(),
)
