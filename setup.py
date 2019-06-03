from setuptools import setup, find_packages

setup(
    name             = 'policy_engine',
    version          = '0.0.0.1',
    packages         = find_packages(),
    license          = 'MIT',
    url              = 'https://github.com/cici-conclave/policy_engine',
    description      = 'Policy engine for verifying workflows',
    long_description = open('README.md').read(),
)
