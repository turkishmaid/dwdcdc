#!/usr/bin/env python
# coding: utf-8

"""
Primer:
      pip install -e .  # from package root, install via symlinks (one-time)
      # change version in this file
      python setup.py sdist  # create versioned dist/*.tar.gz
      python3 -m twine upload dist/*  # choose the newest
      # commit and push latest
      # create version in GitHub

Created: 23.09.2020
"""

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='dwdcdc',
    version = '0.2.0',
    description = 'Fool around with data from DWD Climate Data Center',
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = 'https://github.com/turkishmaid/dwdcdc',
    author = 'Sara Ziner',
    # author_email = 'flyingcircus@example.com',
    license = 'MIT',
    packages = setuptools.find_packages(),
    include_package_data = True,
    zip_safe = False,
    scripts = [ "bin/dwd-at2h" ],
    install_requires = [ 'docopt', ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Atmospheric Science",
    ],
    python_requires='>=3.7',
)