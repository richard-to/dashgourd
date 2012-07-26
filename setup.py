from setuptools import setup

setup(
    name='DashGourd',
    version='0.1.1',
    url='https://github.com/richard-to/dashgourd',
    author='Richard To',
    platforms='any',
    packages=[
        'dashgourd', 
        'dashgourd.api', 
        'dashgourd.importer', 
        'dashgourd.plugins'
    ],
    include_package_data=True,
    install_requires=[
        'pymongo',
        'MySQL-python'
    ]	
)
