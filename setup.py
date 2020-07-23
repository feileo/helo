#!/usr/bin/env python3

import sys

from setuptools import find_packages, setup


if sys.version_info.major >= 3 and sys.version_info.minor >= 7:
    import helo
else:
    raise RuntimeError(
        "Unsupported Python version, please upgrade to 3.7 and above")


with open('README.rst', 'r', encoding='utf-8') as readme_file:
    HELO_README = readme_file.read().strip()


setup(
    name=helo.__name__,
    version=helo.__version__,
    license=helo.__license__,
    author='at7h',
    author_email='g@at7h.com',
    url='https://github.com/at7h/helo',
    description=(
        'Helo is a simple and small low-level asynchronous ORM using Python asyncio'
    ),
    long_description=HELO_README,
    packages=find_packages(),
    py_modules=['helo'],
    include_package_data=True,
    python_requires='>=3.7',
    keywords='orm asyncio mysql aiomysql pymysql python python3 async/await web',
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
