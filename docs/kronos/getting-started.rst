.. _kronos-getting-started:

Get running in 5 minutes
========================

First, check out Kronos, add some default settings, and launch it locally:

.. code-block:: bash

    git clone https://github.com/Locu/chronology.git
    cd chronology/kronos
    sudo make installdeps
    python runserver.py --port 8151 --config settings.py.template --debug


Then, from the ``chronology/pykronos`` directory, run a ``python`` shell, and insert/retrieve some data:

.. code-block:: python
    :linenos:

    from datetime import datetime
    from datetime import timedelta
    from dateutil.tz import tzutc
    from pykronos.client import KronosClient
    kc = KronosClient('http://localhost:8151', namespace='kronos')
    kc.put({'yourproduct.website.clicks': [
      {'user': 35, 'num_clicks': 10}]})
    for event in kc.get('yourproduct.website.clicks',
                        datetime.now(tz=tzutc()) - timedelta(minutes=5),
                        datetime.now(tz=tzutc())):
      print event

On the first line, we created a Kronos client to speak with the server we just started. On the second line, we've put a single event on the ``yourproduct.website.clicks`` clickstream. Finally, we retrieve all ``yourproduct.website.clicks`` events that happened in the last five minutes.

If you wish to see a more detailed example of the Kronos API, check out the more detailed :ref:`pykronos example <pykronos>`.

If you would like to run kronos as a daemon, run ``setup.py`` to install ``kronosd``.

.. code-block:: bash

    sudo python setup.py install

Configure your settings in ``/etc/kronos/settings.py`` and ``/etc/kronos/uwsgi.ini``. Logs can be found in ``/var/log/kronos``. When everything is configured to your liking, run

.. code-block:: bash

    sudo /etc/init.d/kronos start

You can also call ``stop``, ``restart``, or ``force-reload`` on that command.


