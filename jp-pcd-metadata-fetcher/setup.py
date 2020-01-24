# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from os import path

from io import open

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    install_requires = f.read().split('\n')

setup(
    name='pcd-metadata-fetcher',
    version='0.0.1',
    description='Japan Point Cloud Database Metadata Fetcher',
    url='https://github.com/colspan/pcd-open-datasets',
    author='Kunihiko Miyoshi',
    author_email='contact@colspan.net',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
    ],
    entry_points={
        'console_scripts': [
            'jp-pcd-metadata-fetcher=jppcdmetadatafetcher:command',
        ],
    },
    packages=find_packages(where='jppcdmetadatafetcher'),
    python_requires='>2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, <4',
    install_requires=install_requires
)
