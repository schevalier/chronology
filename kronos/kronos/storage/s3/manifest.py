from collections import defaultdict
from banyan import SortedSet
from banyan import OverlappingIntervalsUpdator
from boto.s3.prefix import Prefix

from kronos.storage.s3.sstable import SSTable


class Manifest(object):
  def __init__(self, bucket):
    self.bucket = bucket
    self.refresh()

  def refresh(self):
    sstables = defaultdict(dict)
    itrees = {}
    all_sst_keys = set()
    # Look at key structure in sstables.py to understand this parsing.
    # http://docs.aws.amazon.com/AmazonS3/latest/dev/ListingKeysHierarchy.html
    prefixes = (prefix
                for prefix in self.bucket.list(prefix='sstables/',
                                               delimiter='/')
                if isinstance(prefix, Prefix))
    for prefix in prefixes:
      stream = prefix.name.split('/')[1]
      sst_keys = self.bucket.list(prefix=prefix.name + 'sst_')
      for key in sst_keys:
        all_sst_keys.add(key.name)
        sstable = SSTable(self.bucket, key.name)
        sstables[stream][(sstable.start_id, sstable.end_id)] = sstable
    for stream in sstables:
      itrees[stream] = SortedSet(sstables[stream].iterkeys(),
                                 updator=OverlappingIntervalsUpdator)
    self.sstables = sstables
    self.itrees = itrees
    self.all_sst_keys = all_sst_keys

  def overlapping_ssts(self, stream, start_id, end_id):
    for _range in self.itrees[stream].overlap((start_id, end_id)):
      yield self.sstables[stream][_range]
