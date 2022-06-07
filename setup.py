#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('docs/history.rst') as history_file:
    history = history_file.read()

with open('requirements.txt') as req:
    requirements = req.read().split('\n')

with open('requirements_dev.txt') as req:
    # Ignore the -r on the two lines
    setup_requirements = req.read().split('\n')[2:]

setup_requirements += requirements
test_requirements = ['pytest>=3'] + requirements

setup(
    author="Micah Johnson",
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',

    ],
    description="Software for building and managing a SnowEx PostGIS database",
    entry_points={
        'console_scripts': [
            'clear_dataset=snowex_db.cli:clear_dataset',
        ],
    },
    install_requires=requirements,
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='snowex_db',
    name='snowex_db',
    packages=find_packages(include=['snowex_db', 'snowex_db.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/SnowEx/snowex_db',
    version='0.1.0',
    zip_safe=False,
)
