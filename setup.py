# -*- coding: utf-8 -*-
#!/usr/bin/env python

from setuptools import setup, find_packages


PACKAGE = "tornado_rest"
NAME = "tornado_rest"
VERSION = __import__(PACKAGE).__version__

setup(
    name=NAME,
    version=VERSION,
    #packages=find_packages(exclude=["tests.*", "tests"]),
    packages=find_packages(),
    install_requires=['motor==0.1.2', 'tornado==3.1.1', 'requests==2.4.3',
                      'schematics', 'six==1.8.0'],
    dependency_links=[
        "git+https://github.com/schematics/schematics.git#egg=schematics"
    ],

    author="indieman",
    author_email="ivan.kobzev@gmail.com",
    description="Tornado REST with mongo support."
)
