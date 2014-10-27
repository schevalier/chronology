The Chronology Project
======================

Introduction
------------
This repository contains three loosely coupled projects:

 * :ref:`Kronos <kronos>` is a time series storage engine that allows you to store and retrieve timestamped JSON blobs from various backends (e.g., Cassandra, S3). It's a way friendlier logging API than your filesystem.
  
 * :ref:`Metis <metis>` is a HTTP compute service over streams of data stored in Kronos. It's currently implemented as a thin wrapper around the Spark data processing engine.

 * :ref:`Jia <jia>` is a visualization, dashboarding, and data exploration tool. It can speak with Kronos and Metis. It answers questions for humans, rather than just developers.

Get running in 5 minutes
------------------------
Each of these projects has a "Get running in 5 minutes" section. If you can't get started with one of these systems in five minutes, contact us and we'll make sure you can!

 * :ref:`Getting started with Kronos <kronos-getting-started>`
 * :ref:`Getting started with Metis <metis-getting-started>`
 * :ref:`Getting started with Jia <jia-getting-started>`

Contents
--------

.. toctree::
   :maxdepth: 2

   kronos/index
   metis/index
   jia/index

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

