#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/21 0021 13:22
# @Author  : Hadrianl 
# @File    : setup

from setuptools import setup, find_packages

version = '1.1'
requires = ['pymongo']
setup(name='KRData',
      version=version,
      description='凯瑞投资数据获取接口',
      packages=find_packages(exclude=[]),
      author='Hadrianl',
      author_email='137150224@qq.com',
      install_requires=requires,
      zip_safe=False,
      entry_points={
          "console_scripts": [
              "KRData = KRData.__init__:entry_point"
          ]
      },
      classifiers=[
          'Programming Language :: Python',
          'Operating System :: Microsoft :: Windows',
          'Operating System :: Unix',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
      ],
      )