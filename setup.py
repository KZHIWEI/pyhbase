#!/usr/bin/env python
import setuptools
import lib
setuptools.setup(name='pyhbase',
                 version=lib.__version__,
                 description='PyHbase',
                 author='DP Technology',
                 packages=setuptools.find_packages(),
                 author_email='zhangzw@dp.tech',
                 python_requires='>=3.8',
                 install_requires=['thrift'],
                 )
