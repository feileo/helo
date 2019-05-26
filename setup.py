#!/usr/bin/env python3

from setuptools import find_packages, setup

with open('README.rst', 'r', encoding='utf-8') as readme_file:
    trod_readme = readme_file.read().strip()

setup(
    name='trod',
    version='0.0.14',
    license='BSD license',
    author='acthse',
    author_email='acthse@outlook.com',
    url='https://github.com/acthse/trod',
    description='Trod is a very simple asynchronous Python ORM based on asyncio',
    long_description=trod_readme,
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    python_requires='>=3.6',
    keywords='orm asyncio aiomysql python3 mysql',
    zip_safe=False,
    install_requires=[
        'aiomysql>=0.0.19',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        "License :: OSI Approved :: BSD License",
        'Intended Audience :: Developers',
    ]
)
