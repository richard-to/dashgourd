from setuptools import setup

setup(
    name='Dashgourd',
    version='0.3.9',
    url='https://github.com/richard-to/dashgourd',
    author='Richard To',
    description='Generate stats and charts from mongodb',
    platforms='any',
    packages=[
        'dashgourd', 
        'dashgourd.api',
        'dashgourd.charts',
        'dashgourd.charts.formatters'
    ],
    namespace_packages=['dashgourd'],    
    include_package_data=True,
    install_requires=[
        'pymongo'
    ]	
)
