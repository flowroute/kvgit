#!/usr/bin/env python
from setuptools import setup


setup(
    name='kvgit',
    version='0.1.0',
    description='Git backed KV store',
    # long_description=open('README.md').read(),
    author='Matthew Williams',
    author_email='matthew@flowroute.com',
    url='http://github.com/mgwilliams/kvgit',
    packages=['kvgit'],
    license='MIT',
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7'
    ),
    install_requires=['pygit2>=0.20.3']
)
