from setuptools import setup, find_packages

setup(
    name             = 'dogma',
    version          = '0.0.0.1',
    packages         = find_packages(),
    license          = 'MIT',
    url              = 'https://github.com/cici-conclave/dogma',
    description      = 'Policy engine for verifying workflows',
    long_description = open('README.md').read(),
)
