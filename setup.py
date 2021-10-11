import re
from setuptools import setup, find_packages


# Read property from project's package init file
def get_property(prop, project):
    result = re.search(r'{}\s*=\s*[\'"]([^\'"]*)[\'"]'.format(prop),
                       open(project + '/__init__.py').read())
    return result.group(1)


setup(
    name='raylink',
    version=get_property('__version__', 'raylink'),
    description='RayLink framework.',
    packages=find_packages(),
    python_requires='>=3.8',
    install_requires=[
        'thrift',
        'ray',
        'numpy',
        'gym',
        'portpicker',
        'easydict',
        'netifaces',
        'tabulate',
        'psutil',
        'schedule',
    ]
)
