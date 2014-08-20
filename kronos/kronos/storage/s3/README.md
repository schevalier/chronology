S3 Storage
==========

**TODO(usmanm)**: Revise this document once the an initial implementation is complete.

The S3 storage engine is inspired by the architecture of [LevelDB](https://code.google.com/p/leveldb/). The following discussion deals with how a single stream is managed by the backend. A simple layer of indirection is used at most places to support multiple streams.

Log
---

There is an on-disk (log backed by LevelDB) which keeps track of recent updates. Periodically or after the log hits a certain size threshold, it is flushed to S3 as an [SSTable](#sstable) and a new log is created for future updates. LevelDB was chosen rather then keeping an in memory log (the memtable) to reduce memory pressure.

The advantages of using LevelDB rather than a simple append-only log is that it supports range scans which can be used during read operations to reflect all locally logged updates.

SSTable
-------

An SSTable is an immutable S3 file that stores a sequence of key value pairs sorted by key. The value is either a user supplied value for the key or a deletion marker specifying the range of keys to be deleted. Deletion markers are present to hide stale values present in older SSTables.

An SSTable can belong to either level 0 or level 1. Level 0 SSTables are created from log flushes, while level 1 SSTables are created by merging overlapping SSTables from level 0 and/or level 1. SSTables in level 0 may contain overlapping keys, however SSTables in level 1 will always store non-overlapping key ranges. This invariant is preserved by the [compaction](#compactions) process.

### File Format

Each SSTable file (.sst) has an accompanying index file (.idx) which is an index of the keys stored in the SSTable.

The index file contains a sequence of key, offsets pairs sorted by key. The offset points to the offset in the file that the key starts at. All keys stored in the SSTable are not stored in the index, instead we index keys at offset increments of 1mb.

Each object (key, value, offset, whatever) is serialized using a simple protocol:
- 4 byte header.
- Serialized object.
- 4 byte footer (copy of the header).

The header stores the length of the serialized object. Storing a footer identical to the header let's us iterate in both directions. For the *value* object, the most significant bit of the header (and footer) is reserved to indicate whether the value is a deletion marker or not. If it is a deletion marker then the corresponding key points to the start key for the delete range operation, while the value is the end key for it.

Each .sst file key has attached metadata which includes:
- Start and end key in the SSTable.
- Level.
- List of SSTables that were merged while creating this SSTable (ancestors).
- List of SSTables that were created along with this SSTable during the merge operation (siblings).

A level 1 .sst file is >= 512mb and <= 1gb in size.

### Compression

The following tricks are employees to reduce the size of both the idx and the sst files:
- Since we never need to iterate the idx file backwards, the footer is omitted from all entries in the idx file.
- [Delta encoding](http://en.wikipedia.org/wiki/Delta_encoding) is used to store keys.
- The indexed blocks in the .sst file are compressed as a unit so the entire block needs to be loaded in memory and decompressed which is okay because the size of a block is pretty small (1mb).

Manifest
--------

The manifest keeps track of all the SSTable files in S3 with all their metadata. It is consulted for every read operation. When a node starts, it reads in all the metadata from the S3 bucket and constructs the manifest before allowing any read/write operations.

We use the S3 bucket event notification feature to receive *push* events when the keys in the configured bucket change (see [this](http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html)). This lets us react to changes made by any kronos node and keep the manifest up to date.

Compaction
----------

Level 0 files may overlap with each other, but level 1 files can only overlap with level 0 files. Level 0 files will typically be small in size. Their size is directed by the frequency of flushes.

A level 0 compaction will keep on merging level 0 files together, till the resulting level 0 file produced is ~512mb. Once that happens, the compaction will pick that file and all overlapping files in level 0 and level 1, merge them, and output a sequence of level 1 files. All level 1 files created from a compaction are ~768mb.

Level 1 compaction only touch files in level 1. If there is a level 1 file <512mb, it is compacted by coalescing it with the next file in level 1. If the resulting file size exceeds 1gb in size, it is split into two files. Level 1 compactions always compact non-overlapping files which means that we're just appending one file to the other and then writing out 1 or more files. We use the [Multipart Upload Copy](http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html) feature of S3 to accomplish this without ever having to read the files being compacted.

All compactions create objects in a temp directory and copy them to the working directory after all the data, metadata and index data has been written.

Recovery
--------

Since each SSTable includes metadata about files which were merged to create it and files which were created with it, it's easy to detect incomplete merges and perform the necessary clean up. The following clean ups can happen:
- If all siblings are present, delete all ancestors.
- If some siblngs are missing, delete all siblings. Since ancestors are deleted after all siblings are created, we're guaranteed no data loss.
