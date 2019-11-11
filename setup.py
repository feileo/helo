#!/usr/bin/env python3

from setuptools import find_packages, setup

import trod

with open('README.rst', 'r', encoding='utf-8') as readme_file:
    TROD_README = readme_file.read().strip()

setup(
    name=trod.__name__,
    version=trod.__version__,
    license='MIT license',
    author='at7h',
    author_email='g@at7h.com',
    url='https://github.com/at7h/trod',
    description='Trod is a simple asynchronous(asyncio) Python ORM.',
    long_description=TROD_README,
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    python_requires='>=3.7',
    keywords='orm asyncio aiomysql python3 mysql',
    zip_safe=False,
    install_requires=[
        'aiomysql>=0.0.19',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        "License :: OSI Approved :: MIT License",
        'Intended Audience :: Developers',
    ]
)
