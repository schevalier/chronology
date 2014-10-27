.. _kronos:

Kronos
======

Kronos is a time series storage engine.  It can store streams of data
(e.g., user click streams, machine cpu utilization) and retrieve all
events you've stored over a given time interval.  Kronos exposes an
HTTP API to send, retrieve, and delete events.  We've written Kronos
backends for memory (testing),
`Cassandra <http://www.datastax.com/dev/blog/advanced-time-series-with-cassandra>`_
(low latency), and `S3 <http://aws.amazon.com/s3/>`_ (high throughput),
with more to come.

GoDaddy's Locu team currently uses it to store all user and machine
data that power our reports and analyses.

.. toctree::
   :maxdepth: 3

   Getting started <getting-started>
   Demployment <deployment>
   Configuration <configuration>
   Design goals & more <design-goals>
   PyKronos <pykronos>
   Kronos.js <kronosjs>
   GoKronos <gokronos>
   HTTP API <http>
