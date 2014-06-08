import hashlib

from datetime import datetime
from datetime import timedelta
from inspect import getsource
from pykronos.client import TIMESTAMP_FIELD
from pykronos.utils.time import datetime_to_epoch_time
from pykronos.utils.time import epoch_time_to_kronos_time
from pykronos.utils.time import kronos_time_to_datetime
from pykronos.utils.time import kronos_time_to_epoch_time
from dateutil.tz import tzutc

class QueryCache(object):
  """A Kronos-based cache for expensive time series query results.

  This cache is designed for situations where a large number of events
  are converted into a small number of events for each bucket.  If a
  hundred thousand page views happen per 10-minute interval,
  retrieving 14400000 total events to provide a count of events in a
  24-hour period would be slow, but returning 144 events per day (one
  for each 10-minute interval) would be cheap.  This cache will
  retrieve and compute the events once per bucket, caching the
  resulting computation for the future.

  Given an opaque function that retrieves and computes time series
  based on a start and end time, calculates and saves the resulting
  computed series results in a Kronos scratch namespace.  Future
  queries to similar time ranges will be retrieved from the cache
  rather than computed again.

  Design considerations:
  - We store one Kronos event per time bucket, which is fine if a
    large number of events is aggregated into a small number of
    results, but will be a poor match for computations that do not
    reduce the size of the computed data.
  - The cache semantics assume that older data is stable, whereas data
    closer to the current time is still in flux (e.g., events are
    delayed, nodes are down).  The caller is asked to provide an
    untrusted datetime.  For events before that datetime, it is
    assumed that running the `query_function` on data twice will give
    the same result, and thus these results can be cached.  For data
    after that datetime, computations are considered untrustable
    still, and any computed results will be returned but not cached.
  - A QueryCache exposes two interfaces: `cached_results` and
    `compute_and_cache`.  `compute_and_cache` is most useful in
    scenarios where a caller wants to benefit from previously computed
    results while caching any that haven't been computed yet.
    `cached_results` is useful in scenarios where a caller wants to
    precompute and cache results asynchronously using
    `compute_and_cache`, and then quickly retrieve any previously
    computed results without performing any new computation.
  """
  QUERY_CACHE_VERSION = 1
  CACHE_KEY = 'cached'

  def __init__(self, client, query_function, bucket_width, scratch_namespace):
    """
    Initializes the query cache.
    
    :param client: A Kronos client for reading/storing computed streams.
    :param query_function: A function that, given a `start_date` and
    `end_date` that align on `bucket_width` boundaries, returns an
    iterable over the computed events on those time windows.
    :param bucket_width: A timedelta object specifying how to
    temporally group events.  For example, if `query_function`
    computes the 10-minute rolling average of some stream of floats,
    `bucket_width` should be `timedelta(minutes=10)` so that the
    QueryCache knows to call `query_function` with 10-minute intervals
    of data.
    :param scratch_namespace: The namespace under which to store
    cached events.
    """

    self._client = client
    self._query_function = query_function
    self._bucket_width = int(bucket_width.total_seconds())
    if self._bucket_width != bucket_width.total_seconds():
      raise ValueError('bucket_width can not have subsecond granularity')
    self._scratch_namespace = scratch_namespace
    self._scratch_stream = self._scratch_stream_name()

  def _scratch_stream_name(self):
    """
    A unique cache stream name for this QueryCache.

    Hashes the necessary facts about this QueryCache to generate a
    unique cache stream name.  Different `query_function`
    implementations at different `bucket_width` values will be cached
    to different streams.
    """
    query_details = [
      str(QueryCache.QUERY_CACHE_VERSION),
      str(self._bucket_width),
      getsource(self._query_function),
      ]
    return hashlib.sha512('$'.join(query_details)).hexdigest()[:20]


  def _sanity_check_time(self, start_time, end_time):
    assert isinstance(start_time, datetime)
    assert isinstance(end_time, datetime)
    assert start_time < end_time
    # TODO(marcua): turn these into a value error
    if (((datetime_to_epoch_time(start_time) % self._bucket_width) != 0) or
        ((datetime_to_epoch_time(end_time) % self._bucket_width) != 0)):
      raise ValueError('start_time and end_time must be on bucket_width boundaries')

  def _bucket_time(self, event_time):
    """
    The seconds since epoch that represent a computed bucket.

    An event bucket is the time of the earliest possible event for
    that `bucket_width`.  Example: if `bucket_width =
    timedelta(minutes=10)`, bucket times will be the number of seconds
    since epoch at 12:00, 12:10, ...  on each day.
    """
    event_time = kronos_time_to_epoch_time(event_time)
    return event_time - (event_time % self._bucket_width)

  def _bucket_events(self, event_iterable):
    """
    Convert an iterable of events into an iterable of lists of events
    per bucket.
    """
    
    current_bucket_time = None
    current_bucket_events = None
    for event in event_iterable:
      event_bucket_time = self._bucket_time(event[TIMESTAMP_FIELD])
      if current_bucket_time == None or current_bucket_time < event_bucket_time:
        if current_bucket_events != None:
          yield current_bucket_events
        current_bucket_time = event_bucket_time
        current_bucket_events = []
      current_bucket_events.append(event)
    if current_bucket_events != None and current_bucket_events != []:
      yield current_bucket_events


  def _cached_results(self, start_time, end_time):
    """
    Retrieves cached results for any bucket that has a single cache entry.

    If a bucket has two cache entries, there is a chance that two
    different writers previously computed and cached a result since
    Kronos has no transaction semantics.  While it might be safe to
    return one of the cached results if there are multiple, we
    currently do the safe thing and pretend we have no previously
    computed data for this bucket.
    """
    cached_buckets = self._bucket_events(
      self._client.get(self._scratch_stream, start_time, end_time,
                       namespace=self._scratch_namespace))
    for bucket_events in cached_buckets:
      # If we have multiple cache entries for the same bucket, pretend
      # we have no results for that bucket.
      if len(bucket_events) == 1:
        first_result = bucket_events[0]
        yield (kronos_time_to_epoch_time(first_result[TIMESTAMP_FIELD]),
               first_result[QueryCache.CACHE_KEY])

  def _compute_and_cache_bucket(self, bucket, untrusted_time):
    bucket_start = kronos_time_to_datetime(
      epoch_time_to_kronos_time(bucket))
    bucket_end = kronos_time_to_datetime(
      epoch_time_to_kronos_time(bucket + self._bucket_width))
    bucket_events = list(self._query_function(bucket_start, bucket_end))
    # If all events in the bucket happened before the untrusted
    # time, cache the query results.
    if bucket_end < untrusted_time:
      caching_event = {TIMESTAMP_FIELD: bucket_start,
                       QueryCache.CACHE_KEY: bucket_events}
      self._client.delete(self._scratch_stream, bucket_start,
                          bucket_start + timedelta(milliseconds=1),
                          namespace=self._scratch_namespace)
      self._client.put({self._scratch_stream: [caching_event]},
                       namespace=self._scratch_namespace)
    return bucket_events

  def cached_results(self, start_time, end_time):
    """
    Retrieve all previously computed results.

    Retrieves all results from the cache between `start_time` and
    `end_time` (datetimes), which must align to `bucket_width`
    boundaries.  Performs no computation.  Useful for
    asynchronously/periodically calling `compute_and_cache` to
    precompute results and quickly retrieving all cached results in
    interactive time.

    :param start_time: A datetime for the beginning of the range,
    aligned with `bucket_width`.
    :param end_time: A datetime for the end of the range, aligned with
    `bucket_width`.
    """
    self._sanity_check_time(start_time, end_time)
    for bucket, bucket_events in self._cached_results(start_time, end_time):
      for event in bucket_events:
        yield event

  def compute_and_cache(self, start_time, end_time, untrusted_time,
                        force_compute=False):
    """
    Return the results for `query_function` on every `bucket_width`
    time period between `start_time` and `end_time`.  Look for
    previously cached results to avoid recomputation.  For any buckets
    where all events would have occurred before `untrusted_time`,
    cache the results.

    :param start_time: A datetime for the beginning of the range,
    aligned with `bucket_width`.
    :param end_time: A datetime for the end of the range, aligned with
    `bucket_width`.
    :param untrusted_time: A datetime after which to not trust that
    computed data is stable.  Any buckets that overlap with or follow
    this untrusted_time will not be cached.
    :param force_compute: A boolean that, if True, will force
    recompute and recaching of even previously cached data.
    """
    self._sanity_check_time(start_time, end_time)
    if not untrusted_time.tzinfo:
      untrusted_time = untrusted_time.replace(tzinfo=tzutc())

    # Generate a list of all cached buckets we need to see data for.
    required_buckets = xrange(int(datetime_to_epoch_time(start_time)),
                              int(datetime_to_epoch_time(end_time)),
                              self._bucket_width)

    # Get any cached results, grouped by bucket.
    if force_compute is True:
      cached_bucket_iterator = [].__iter__()
    else:
      cached_bucket_iterator = (self._cached_results(start_time, end_time)
                                .__iter__())

    # Use either the cached results or compute any uncached buckets.
    # Cache previously uncached results if they are guaranteed to have
    # happened before the untrusted time.
    current_cached_bucket = None
    for required_bucket in required_buckets:
      if current_cached_bucket == None:
        try:
          current_cached_bucket = cached_bucket_iterator.next()
        except StopIteration:
          pass
      emit_events = None
      if current_cached_bucket != None and (
        current_cached_bucket[0] == required_bucket):
        emit_events = current_cached_bucket[1]
        current_cached_bucket = None
      else:
        # We don't have cached events, so compute the query.
        emit_events = self._compute_and_cache_bucket(required_bucket,
                                                     untrusted_time)
      for event in emit_events:
        yield event
    self._client.flush()
