.. _kronos-configuration:

Configuring Kronos
==================

Take a look at `settings.py.template <https://github.com/Locu/chronology/blob/master/kronos/kronos/conf/default_settings.py>`_. We tried to document all of the settings pretty thoroughly. If anything is unclear, `file an issue <https://github.com/Locu/chronology/issues/new>`_ and we'll clarify!

Backends
--------

Memory
~~~~~~
The in-memory backend is mostly used for testing. Here's a sample storage configuration for it:

.. code-block:: python

    storage = {
      'memory': {
        'backend': 'memory.InMemoryStorage',
        'max_items': 50000
      },
    }

There's only one parameter, max_items, which specifies the largest number of events you can store before it starts acting funny.

Cassandra
~~~~~~~~~
Our Cassandra backend is targeted at low-latency event storage, but internally we use it even for relatively bursty high-throughput streams. Here's a sample ``storage`` configuration:

.. code-block:: python

    storage = {
      'cassandra': {
        'backend': 'cassandra.CassandraStorage',
        'hosts': ['127.0.0.1'],
        'keyspace_prefix': 'kronos_test',
        'replication_factor': 3,
        'timewidth_seconds': 60 * 60, # 1 hour.
        'shards_per_bucket': 3,
        'read_size': 1000
      }
    }

Our design for the Cassandra backend is heavily influenced by `a great blog post <http://www.datastax.com/dev/blog/advanced-time-series-with-cassandra>`_ with illustrations on the topic. Here are what the parameters above mean:

  * ``hosts`` is a list of Cassandra nodes to connect to.

  * ``keyspace_prefix`` is a prefix that is applied to each `KeySpace <http://www.datastax.com/documentation/cql/3.0/cql/cql_using/create_keyspace_c.html>`_ Kronos creates. A KeySpace is created for each configured ``namespace``.

  * ``replication_factor`` is the number of Cassandra nodes to replicate
    each data item to.  Note that this value is set at the
    initialization of a keyspace, so if you change it, existing
    keyspaces will be at their previous replication factor.  You'll
    have to manually change the replication factor of existing
    keyspaces.

  * ``timewidth_seconds`` is the number of seconds worth of data
    to bucket together.  The Cassandra backend stores all events that
    happened within the same time span (e.g., one hour) together.  Set
    this to a small timewidth to reduce latency (you won't have to
    look through a bunch of irrelevant events to find a small
    timespan), and to a high timewidth to increase throughput (you
    won't have to look through a bunch of buckets to aggregate all of
    the data of a large timespan).  We tend to go for an hour as a default.

  * ``shards_per_bucket`` makes up for the white lie we told in
    the last bullet point.  Rather than storing all events that happen
    during a given timespan in a single bucket (which will put
    pressure on a single Cassandra row key/nodes storing that key), we
    can shard each bucket across ``default_shards_per_bucket`` shards
    for each timewidth.  As an example, setting
    ``default_shards_per_bucket`` will store each hour worth of data in
    one of three shards, decreasing the pressure on any one Cassandra
    row key.

  * ``read_size`` is probably not something you will play around with
    too much.  If a single time width's shard contains a lot of
    events, the Cassandra client driver will transparently iterate
    through them in chunks of this size.

ElasticSearch
~~~~~~~~~~~~~
Our ElasticSearch backend is designed to work well with `Kibana <http://www.elasticsearch.org/overview/kibana/>`_.
Most implementations of a time-series storage layers on top of ElasticSearch
will create a new index per day (or some time interval); e.g. `Logstash <http://logstash.net/>`_
does this. Our approach is a little different. We keep on writing events to an
index till the number of events in it exceeds a certain limit and then rollover
to a new index. In order to keep track of what indices contain data for what
time ranges, we use ElasticSearch's `aliasing <http://www.elasticsearch.org/guide/xen/elasticsearch/reference/current/indices-aliases.html>`_
feature to assign an alias for each day that the index might contain data for.
This approach lets us be compatible with Kibana while at the same time
controlling the number of indices being created over time. Here's a sample
``storage`` configuration:

.. code-block:: python

    storage = {
      'elasticsearch': {
        'backend': 'elasticsearch.ElasticSearchStorage',
        'hosts': [{'host': 'localhost', 'port': 9200}],
        'index_template': 'kronos_test',
        'index_prefix': 'kronos_test',
        'shards': 1,
        'replicas': 0,
        'force_refresh': True,
        'read_size': 10,
        'rollover_size': 100,
        'rollover_check_period_seconds': 2
      }
    }


Here are what the parameters above mean:

  * ``hosts`` is a list of ElasticSearch nodes to connect to.

  * ``index_template`` is the name of the `template <http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-templates.html>`_
    Kronos creates in ElasticSearch. `This template <https://github.com/Locu/chronology/blob/master/kronos/kronos/storage/elasticsearch/index.template>`_
    is applied to all indices Kronos creates.

  * ``index_prefix`` is a name prefix for all indices Kronos creates.

  * ``shards`` is the number of shards Kronos creates for each index.

  * ``replicas`` is the number of replicas Kronos creates for each index.

  * ``force_refresh`` will flush the index being written to at the end of each
    ``put`` request. This shouldn't be enabled for production environments; it
    probably will hose your ElasticSearch cluster.

  * ``read_size`` is the `scroll <http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/scan-scroll.html>`_
    size when retrieving events from ElasticSearch. It amounts to the number of
    events fetched from ElasticSearch per request.

  * ``rollover_size`` is the number of events after which Kronos will create a
    new index and start writing events into the new index. This size is merely
    a hint. Kronos periodically checks the number of events in the index it is
    writing to and rolls it over when the number exceeds ``rollover_size``.

  * ``rollover_check_period_seconds`` is the interval after which a Kronos
    instance checks to see if the index needs to be rolled over.

S3
~~

*Under construction -- check back later.*

