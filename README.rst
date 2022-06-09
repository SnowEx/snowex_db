==============================
Welcome to the SnowEx Database
==============================

.. image:: https://readthedocs.org/projects/snowex_db/badge/?version=latest
    :target: https://snowex_db.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://github.com/SnowEx/snowex_db/actions/workflows/main.yml/badge.svg
    :target: https://github.com/SnowEx/snowex_db/actions/workflows/main.yml
    :alt: Testing Status

.. image:: https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/micahjohnson150/d404e9cded83f72fcca65081358cc11b/raw/snowex_db_heads_master.json
    :alt: code coverage


Database creation and management software for SnowEx data. The goal is to
create a single source (citeable) dataset that is cross queriable for snow
researchers.

WARNING - This is under active development in preparation for SnowEx Hackweek.
Use at your own risk.  Data will change as it is QA/QC'd and the end goal is
for all data in this database to be pulled from NSIDC.  The goal is for this
to become a community database open to all.

Features
--------
* Auto parse header information and include it in upload
* Interpret and upload csv, rasters, uavsar files,
* Full provenance for download and upload
* Manage Site, Point, Profile and Raster Data
* PostgreSQL Database end point for researchers


Installing
----------
If you are are database maintainers or installing it for the first time
follow the instructions below completely.

Mac OS
~~~~~~

First ensure you have following prerequisites:

* Python3.7 +
* HomeBrew

Then to install the postgres database with postgis functionality run:

.. code-block:: bash

  cd scripts/install && sh install_mac.sh


Ubuntu
~~~~~~

First ensure you have following prerequisites:

* Python3.7 +
* wget

Then to install the postgres database with postgis functionality run:


.. code-block:: bash

  cd scripts/install && sh install_ubuntu.sh

Docker
~~~~~~

Alternatively if you have docker install on either os,
the simplest way to spin up the db is to run:

.. code-block:: bash

    docker-compose up -d

To tear it down (which will delete the data!)

.. code-block:: bash

    docker-compose down

Python
------
Install the python package by:

.. code-block:: bash

  python3 setup.py install

If you are planning on running the tests or building the docs below also run:

.. code-block:: bash

  pip install -r requirements_dev.txt

If you are using `conda` you may need to reinstall the following using conda:

  * Jupyter notebook
  * nbconvert

Tests
-----

Quickly test your installation by running:

.. code-block:: bash

  pytest

This will run a series of tests that create a small database and confirm
that samples of the data sets references in `./scripts/upload` folder can be
uploaded seamlessly. These tests can serve as a nice way to see how to
interact with the database but also serve to confirm our reproduciblity.

The goal of this project is to have high fidelity in data
interpretation/submission to the database. To see the current
test coverage run:

.. code-block:: bash

  make coverage


Documentation
-------------

There is a whole host of resources for users in the documentation. It has been
setup for you to preview in your browser.

In there you will find:

* Database structure
* API to the python package snowex_db
* Links to other resources
* Notes about the data uploaded
* Info on populating the database
* And more!

To see the documentation in your browser:

**Warning**: To see the examples/gallery you will need to populate the
database before running this command. Otherwise they will be left with the
last image submitted to GitHub.

.. code-block:: bash

  make docs
