import sys
from dsnetclient import __version__

from setuptools import setup, find_packages

py_version = sys.version_info[:2]
if py_version < (3, 6):
    raise Exception("datashare-network requires Python >= 3.6.")

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='datashare-network-client',
      version=__version__,
      packages=find_packages(),
      description="Client Datashare Network Library",
      use_pipfile=True,
      long_description=long_description,
      long_description_content_type="text/markdown",
      url="https://github.com/icij/datashare-network-lib",
      test_suite='nose.collector',
      tests_require=['nose', 'responses'],
      setup_requires=['setuptools-pipfile'],
      keywords=['datashare', 'api', 'network', 'cryptography'],
      classifiers=[
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "Programming Language :: Python :: 3.8",
          "Intended Audience :: Developers",
          "License :: OSI Approved :: GNU Affero General Public License v3",
          "Operating System :: OS Independent",
          "Topic :: Security :: Cryptography"
      ],
      entry_points='''
        [console_scripts]
        dsnetclient=dsnetclient.main:cli
        ''',
      python_requires='>=3.6',
      )
