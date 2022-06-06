from setuptools import setup, find_packages

setup(
    name='cbs_pandas',
    version='0.1.0',
    packages=find_packages(include=['cbs_pandas', 'cbs_pandas.*'])
)
