#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-----------------------------------------------------------------------------
# Copyright (c) 2015, IBM Corp.
# All rights reserved.
#
# Distributed under the terms of the BSD Simplified License.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

"""
setup.py
"""

# -*- coding: utf-8 -*-
# Note: Always prefer setuptools over distutils
from setuptools import setup, find_packages
from codecs import open


# Get the long description from the relevant file


with open('README.md', 'r', encoding='utf-8') as f:
    longdesc = f.read()


classifiers = [
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'Topic :: Software Development :: Build Tools',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: BSD License',

        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',

        'Natural Language :: English',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: Implementation :: CPython',

        'Topic :: Database',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development'
      ]

setup(name='nzpyida',
      version='0.3.6',
      install_requires=['pandas','numpy','future','six','pypyodbc','pyodbc', 'lazy', 'nzpy'],

      extras_require={
        'jdbc':['JayDeBeApi==1.*', 'Jpype1==0.6.3'],
        'test':['pytest', 'flaky==3.4.0'],
        'doc':['sphinx', 'ipython', 'numpydoc', 'sphinx_rtd_theme']
      },
      description='Supports Custom ML/Analytics Execution Inside Netezza',
      long_description=longdesc,
      long_description_content_type='text/markdown',
      author='IBM Corp.',
      author_email='mlabenski@ibm.com,pawel.mroz1@ibm.com',
      license='BSD',
      classifiers=classifiers,
      keywords='data analytics database development ibm netezza pandas scikitlearn scalability machine-learning knowledge discovery',
      packages=find_packages(exclude=['docs', 'tests*']),
      package_data={
        'nzpyida.sampledata': ['*.txt']},
      url="https://github.com/ibm/nzpyida",
      project_urls={
        "Documentation": "https://nzpyida.readthedocs.io/en/latest/",
        "Source": "https://github.com/IBM/nzpyida",
        "Tracker": "https://github.com/IBM/nzpyida/issues"},
     )
