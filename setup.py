# https://setuptools.pypa.io/en/latest/userguide/quickstart.html

from setuptools import setup
from setuptools import find_packages

setup(
    name='cas_torrent',
    version='0.0.1',
    packages=find_packages(
        where='src',
        #include=['mypackage*'],  # ['*'] by default
        #exclude=['mypackage.tests'],  # empty by default
    ),
    install_requires=[
        'libtorrent',
        'watchdog',
    ],
)
