.. _kronos-deployment:

Deploying Kronos
================

.. code-block:: bash

    git clone https://github.com/Locu/chronology.git
    cd chronology/kronos
    sudo python setup.py install
    sudo /etc/init.d/kronos start


You can also call ``stop``, ``restart``, or ``force-reload`` on the ``init.d/kronos`` command.

The ``setup.py`` script deploys Kronos with
`uWSGI <http://uwsgi-docs.readthedocs.org/en/latest/>`_.  To see our
starter scripts for doing this, check out
`install_kronosd.py <https://github.com/Locu/chronology/blob/master/kronos/scripts/install_kronosd.py>`_,
`uwsgi.ini <https://github.com/Locu/chronology/blob/master/kronos/scripts/uwsgi.ini>`_, and
`kronosd.init.d <https://github.com/Locu/chronology/blob/master/kronos/scripts/kronos.init.d>`_.  If anything is unclear,
`file an issue <https://github.com/Locu/chronology/issues/new>`_ and we'll clarify!
