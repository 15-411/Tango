#!/usr/bin/env python
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import platform
import sys
import warnings

try:
    # Use setuptools if available, for install_requires (among other things).
    import setuptools
    from setuptools import setup
except ImportError:
    setuptools = None
    from distutils.core import setup

from distutils.core import Extension

# The following code is copied from
# https://github.com/mongodb/mongo-python-driver/blob/master/setup.py
# to support installing without the extension on platforms where
# no compiler is available.
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError
from distutils.errors import DistutilsPlatformError, DistutilsExecError
if sys.platform == 'win32' and sys.version_info > (2, 6):
    # 2.6's distutils.msvc9compiler can raise an IOError when failing to
    # find the compiler
    build_errors = (CCompilerError, DistutilsExecError,
                    DistutilsPlatformError, IOError)
else:
    build_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)

class custom_build_ext(build_ext):
    """Allow C extension building to fail.

    The C extension speeds up websocket masking, but is not essential.
    """

    warning_message = """
********************************************************************
WARNING: %s could not
be compiled. No C extensions are essential for Tornado to run,
although they do result in significant speed improvements for
websockets.
%s

Here are some hints for popular operating systems:

If you are seeing this message on Linux you probably need to
install GCC and/or the Python development package for your
version of Python.

Debian and Ubuntu users should issue the following command:

    $ sudo apt-get install build-essential python-dev

RedHat, CentOS, and Fedora users should issue the following command:

    $ sudo yum install gcc python-devel

If you are seeing this message on OSX please read the documentation
here:

http://api.mongodb.org/python/current/installation.html#osx
********************************************************************
"""

    def run(self):
        try:
            build_ext.run(self)
        except DistutilsPlatformError:
            e = sys.exc_info()[1]
            sys.stdout.write('%s\n' % str(e))
            warnings.warn(self.warning_message % ("Extension modules",
                                                  "There was an issue with "
                                                  "your platform configuration"
                                                  " - see above."))

    def build_extension(self, ext):
        name = ext.name
        if sys.version_info[:3] >= (2, 4, 0):
            try:
                build_ext.build_extension(self, ext)
            except build_errors:
                e = sys.exc_info()[1]
                sys.stdout.write('%s\n' % str(e))
                warnings.warn(self.warning_message % ("The %s extension "
                                                      "module" % (name,),
                                                      "The output above "
                                                      "this warning shows how "
                                                      "the compilation "
                                                      "failed."))
        else:
            warnings.warn(self.warning_message % ("The %s extension "
                                                  "module" % (name,),
                                                  "Please use Python >= 2.4 "
                                                  "to take advantage of the "
                                                  "extension."))


kwargs = {}

version = "3.2"

with open('README.rst') as f:
    kwargs['long_description'] = f.read()

if platform.python_implementation() == 'CPython':
    # This extension builds and works on pypy as well, although pypy's jit
    # produces equivalent performance.
    kwargs['ext_modules'] = [
        Extension('tornado.speedups',
                  sources=['tornado/speedups.c']),
    ]

if setuptools is not None:
    # If setuptools is not available, you're on your own for dependencies.
    if sys.version_info < (3, 2):
        kwargs['install_requires'] = ['backports.ssl_match_hostname']

setup(
    name="tornado",
    version=version,
    packages = ["tornado", "tornado.test", "tornado.platform"],
    package_data = {
        "tornado": ["ca-certificates.crt"],
        # data files need to be listed both here (which determines what gets
        # installed) and in MANIFEST.in (which determines what gets included
        # in the sdist tarball)
        "tornado.test": [
            "README",
            "csv_translations/fr_FR.csv",
            "gettext_translations/fr_FR/LC_MESSAGES/tornado_test.mo",
            "gettext_translations/fr_FR/LC_MESSAGES/tornado_test.po",
            "options_test.cfg",
            "static/robots.txt",
            "static/dir/index.html",
            "templates/utf8.html",
            "test.crt",
            "test.key",
            ],
        },
    author="Facebook",
    author_email="python-tornado@googlegroups.com",
    url="http://www.tornadoweb.org/",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    description="Tornado is a Python web framework and asynchronous networking library, originally developed at FriendFeed.",
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        ],
    cmdclass={"build_ext": custom_build_ext},
    **kwargs
)
