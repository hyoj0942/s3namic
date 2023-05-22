from setuptools import setup, find_packages
from _version import get_version
from api import s3namic

v = get_version()
__version__ = v.get("closest-tag", v["version"])

del get_version, v

with open("README.md", encoding="utf-8") as fh:
    long_description = fh.read()
    
_hard_dependencies=[
    'pandas', 
    'boto3', 
    'botocore',
    'pyarrow',
]

setup(
    name='s3namic',
    version=__version__,
    description='A Python package for managing AWS S3 bucket',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='MIT',
    packages=['s3namic'],
    author='Joey Kim',
    author_email='hyoj0942@gmail.com',
    url='https://github.com/hyoj0942/s3namic',
    install_requires=_hard_dependencies,
    keywords=['aws', 's3', 'bucket', 'management'],
)

del _hard_dependencies