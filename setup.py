# -*- coding=utf8 -*-

from setuptools import find_packages, setup

setup(
    name='trod', version='0.0.1',
    license='PRIVATE',
    author='',
    author_email='',
    url='',
    description='async orm',
    packages=find_packages(exclude=['tests']),
    zip_safe=False,
    install_requires=[
        'aiomysql',
        'asyncinit',
        'qiniu',
    ],
    entry_points={
        # 'console_scripts': [
        #     'run = run:run',
        # ],
    }
)
