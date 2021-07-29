"""Package definition."""

from setuptools import find_packages, setup

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='pydbrepo',
    version='0.1.17',
    packages=find_packages(),
    description='Simple implementation of repository pattern for database connections.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/danteay/pydbrepo.git',
    author='Eduardo Aguilar',
    author_email='dante.aguilar41@gmail.com',
    keywords=['database', 'repository', 'sql', 'nosql', 'package'],
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7'
)