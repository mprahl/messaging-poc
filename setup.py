#!/usr/bin/env python

import os
import re

from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, "messaging_poc", "__init__.py")) as fd:
    match = re.search(r'__version__ = "([^"]+)"$', fd.read(), re.MULTILINE)
    VERSION = match.group(1)


def get_requirements(requirements_file="requirements.txt"):
    """
    Get the contents of a file listing the requirements.

    :param str requirements_file: The path to the requirements file, relative to this file
    :return: The list of requirements from ``requirements_file``
    :rtype: list
    """
    with open(os.path.join(here, requirements_file), "r") as f:
        return f.readlines()


setup(
    name="messaging_poc",
    version=VERSION,
    description="A set of tools for using Fedora's messaging infrastructure",
    # Possible options are at https://pypi.python.org/pypi?%3Aaction=list_classifiers
    # TODO: Fix Development Status before deploying this
    classifiers=[
        "Development Status :: 2 - Production/Stable",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    license="GPLv3+",
    platforms=["Fedora", "GNU/Linux"],
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=get_requirements(),
    tests_require=get_requirements("dev-requirements.txt"),
    test_suite="messaging_poc.tests",
    # TODO: Implement this
    # entry_points={
    #     "console_scripts": ["fedora-messaging=fedora_messaging.cli:cli"],
    #     "fedora.messages": ["base.message=fedora_messaging.message:Message"],
    # },
)
