from setuptools import setup

setup(
    name='DashGourd',
    version='0.1',
    url='https://github.com/richard-to/dashgourd',
    author='Richard To',
    packages=['dashgourd'],
    namespace_packages=['dashgourd'],
    install_requires=[
        'pymongo',
        'MySQL-python'
    ]	
)
