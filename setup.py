import os
import re

from setuptools import setup, find_packages
from setuptools.command.install import install

try:
    from pip._internal import main
except ImportError:
    import pip.main as main

try:  # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:  # for pip <= 9.0.3
    from pip.req import parse_requirements

with open(os.path.join(os.path.dirname(__file__), 'door2door', 'version.py')) as v_file:
    VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v_file.read()).group(1)


class OverrideInstall(install):
    """
    Emulate sequential install of pip install -r requirements.txt
    """

    def run(self):
        # install requirements first
        install_reqs = parse_requirements('./requirements/common.txt', session=False)
        # Generator must be converted to list, or we will only have one chance to read each element,
        # meaning that the first requirement will be skipped.
        requirements = list(install_reqs)
        try:
            reqs = [str(ir.req) for ir in requirements]
        except:
            reqs = [str(ir.requirement) for ir in requirements]
        for req in reqs:
            main(["install", req])

        install.run(self)


config = {
    'name': 'decc',
    'description': 'door2door repository',
    'author': 'G. Donetti',
    'url': '',
    'download_url': '',
    'author_email': '',
    'version': VERSION,
    'packages': find_packages(),
    'scripts': [],
    'cmdclass': {'install': OverrideInstall},
    'test_suite': 'nose.collector',
    'tests_require': ['nose'],
}

setup(**config)
