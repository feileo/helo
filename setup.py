#!/usr/bin/env python3

import sys

from setuptools import find_packages, setup


if sys.version_info.major >= 3 and sys.version_info.minor >= 7:
    import trod
else:
    raise RuntimeError(
        "Unsupported Python version, please upgrade to 3.7 and above")


with open('README.rst', 'r', encoding='utf-8') as readme_file:
    TROD_README = readme_file.read().strip()

setup(
    name=trod.__name__,
    version=trod.__version__,
    license='MIT',
    author='at7h',
    author_email='g@at7h.com',
    url='https://github.com/at7h/trod',
    description=(
        'Trod is a low-level simple asynchronous ORM using Python asyncio'
    ),
    long_description=TROD_README,
    packages=find_packages(),
    py_modules=['trod'],
    include_package_data=True,
    python_requires='>=3.7',
    keywords='orm asyncio mysql aiomysql pymysql python python3 async/await',
    zip_safe=False,
    platforms='any',
    install_requires=[
        'aiomysql>=0.0.19',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Intended Audience :: Developers',
    ],
)
