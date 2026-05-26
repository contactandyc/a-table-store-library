# Architectural Blueprint: Cloud-Scale LSM Tree & WAL

At a high level, an LSM Tree paired with a WAL optimizes for massive write throughput while maintaining strict durability and efficient reads. The system absorbs random writes into memory, sequences them for concurrency control, strictly logs them for durability, and flushes them to immutable disk files in the background.

Building this at "cloud scale" requires surviving multi-tenant resource contention, hardware degradation, disk I/O bottlenecks, and sudden power failures without data loss or system deadlocks.

---

## 1. The Write-Ahead Log (WAL) & Durability

Before any data is exposed to the database's memory structures, it is sequentially appended to the persistent WAL. The WAL acts as the absolute source of truth for crash recovery.

**Functionality:**

* **Sequential Append-Only Logging:** Incoming mutations are serialized into continuous log frames on disk.
* **Segment Rotation & Standby Pools:** Because WAL files grow indefinitely, the log is broken into "segments." Once a segment hits a size threshold, it rotates. Instead of deleting old segments, they are recycled into a "standby pool" of pre-allocated files.
* **Global Sequence/LSN Tracking:** A unified Logical Sequence Number (LSN) allows multiple tables or database shards to safely multiplex their writes into a single shared WAL, drastically reducing disk seeks.

**Challenges & Edge Cases Addressed:**

* **I/O Bottlenecks via Group Commit:** Forcing an `fsync` to disk for every concurrent write limits throughput to the physical hardware's IOPS. Instead, concurrent writers queue up. A "leader" thread batches all pending writes into a single contiguous super-batch, issues one sequential disk write, and wakes up all followers simultaneously.
* **Torn Writes and Bit Rot:** A sudden power loss can leave a WAL frame partially written. Every frame is wrapped with an exact byte-length and a CRC32 checksum. During recovery, partial frames or mismatched checksums are safely detected and truncated rather than crashing the system.
* **Cross-Table Garbage Collection:** The WAL can only safely purge old segments when *all* multiplexed tables have successfully flushed their memory to disk. This is managed via global LSN checkpoint watermarks to ensure no table loses un-flushed data.

---

## 2. In-Memory Store (MemTable) & Allocation

Once a write is secure in the WAL, it is inserted into an in-memory data structure (typically a lock-free SkipList) called a MemTable. This keeps data perfectly sorted and available for immediate querying in $O(\log N)$ time.

**Functionality:**

* **Multi-Version Concurrency Control (MVCC):** Reads must not block writes. Every write is tagged with a monotonically increasing Sequence Number and an Operation Type (Put vs. Delete). Updates do not overwrite old data; they are appended.
* **Active vs. Immutable States:** When a MemTable hits its memory limit, it is frozen into an "Immutable" state. A new Active MemTable takes over incoming writes while a background thread safely flushes the immutable one to disk.

**Challenges & Edge Cases Addressed:**

* **Allocation Fragmentation (Arena Allocator):** Calling standard `malloc` for millions of individual key-value rows heavily fragments RAM and destroys CPU cache locality. The system uses a Geometric Arena Allocator—requesting massive contiguous chunks from the OS (e.g., doubling from 128KB up to 4MB) and dispensing memory via a blazing-fast atomic pointer increment.
* **Tombstone Shadowing:** Deleting a record inserts a "Tombstone" marker rather than removing memory. The search logic must correctly interpret tombstones to mask older values on disk, preventing "ghost reads."
* **Snapshot Isolation:** Long-running queries bind to a specific snapshot sequence. The read path must cleanly ignore keys with a sequence number strictly greater than the requested snapshot, guaranteeing repeatable reads.

---

## 3. Persistent Storage Format (SSTables)

When an Immutable MemTable is flushed to disk, it is serialized into a Sorted String Table (SSTable). These files are strictly immutable, meaning readers never need to acquire locks to scan them.

**Functionality:**

* **Block-Based Compression:** Data is clustered into blocks (e.g., 16KB). Each block compresses its contents (e.g., via LZ4) and appends a checksum.
* **Prefix Compression:** Because keys are sorted, adjacent keys often share a prefix (e.g., `user/123`, `user/124`). The SSTable stores only the differing bytes. "Restart points" are embedded periodically to allow fast binary searching without sequentially decompressing the whole block.
* **Probabilistic Filters:** SSTable metadata includes Bloom Filters (or Bitmaps for integer keys) to mathematically prove a key's absence, allowing the system to bypass expensive disk reads.

**Challenges & Edge Cases Addressed:**

* **Malicious or Corrupted Data Attacks (OOM Defense):** If a flipped bit on disk alters a "varint length" header to claim a payload is 2GB, naive readers will attempt to `malloc(2GB)` and instantly crash the server. Production readers strictly validate all parsed lengths and restart arrays against exact file boundaries, gracefully returning a "Corrupt Block" error instead of segfaulting.
* **Integer Underflows:** Mathematical filters (like Bitmaps relying on integer deltas) must be immune to unexpected key patterns or negative integer underflows that cause out-of-bounds array access.
* **Partial Disk Failures (ENOSPC):** Disks can fill up mid-flush. The builder must verify exact-byte returns on every I/O call. If an OS write returns fewer bytes than requested, the builder explicitly aborts and physically deletes the orphaned `.data` and `.meta` files to maintain contiguous durability.

---

## 4. The Read Path & Block Caching

Reading data requires querying the Active MemTable, Immutable MemTable, and then binary-searching the SSTables on disk from newest to oldest.

**Functionality:**

* **Block Cache:** Uncompressed disk blocks are kept in RAM using a Least Recently Used (LRU) eviction policy to drastically speed up repetitive queries.
* **Unified Iteration:** Iterators merge the in-memory SkipLists with the on-disk SSTable streams using a Min-Heap, presenting a single, unified, chronological view of the data to the user.

**Challenges & Edge Cases Addressed:**

* **Cache Lock Contention:** A single global lock on a cache destroys multi-core scalability. The LRU Cache must be heavily sharded (e.g., 64 independent mutexes and hash maps).
* **Cache Stampedes (Thundering Herd):** If 100 threads request the same missing disk block simultaneously, 100 expensive disk reads would trigger. The cache must deduplicate concurrent misses—the first thread issues the I/O, and racing threads resolve to the newly shared pointer.
* **Safe Eviction (Use-After-Free):** Evicting a block while an active iterator is scanning it causes fatal errors. Cache lookups must return a "Pinned" reference count. The LRU eviction algorithm strictly bypasses pinned blocks.

---

## 5. Background Compaction & Versioning

Because flushes continually create new SSTables, reading a single key might require searching dozens of overlapping files (Read Amplification). Compaction is a background process that merges overlapping SSTables, drops obsolete data, and writes out clean files to deeper, exponentially larger "Levels."

**Functionality:**

* **Leveled Architecture:** The disk is divided into Levels (L0 to L6). L0 contains overlapping files flushed directly from memory. L1+ contains strictly non-overlapping key ranges.
* **The Manifest:** Because files are immutable, compactions create new files and delete old ones. The database's layout is tracked via a Manifest log. File additions and deletions are grouped into an atomic `VersionEdit`.

**Challenges & Edge Cases Addressed:**

* **Compaction Starvation:** If the database only ever compacts the newest overlapping files, older stranded keys might never be compacted, causing tombstone bloat. "Compaction Pointers" record the last compacted key for each level, ensuring a fair round-robin progression across the keyspace.
* **Safe Tombstone Dropping:** A tombstone cannot be dropped just because it is old. It can only be permanently purged if it reaches the absolute bottom-most level of the LSM-tree (meaning no older shadowed data exists below it) AND there are no active MVCC Read Snapshots that might still need to see the deletion.
* **Crash-Safe State Transitions:** If a node crashes mid-compaction, the boot-up sequence compares physical disk files against the exact list in the secured Manifest log and safely deletes unreferenced garbage.

---

## 6. Global Concurrency & Resource Orchestration

A production engine must safely orchestrate hundreds of threads, memory limits, and state transitions without deadlocking or CPU thrashing.

**Functionality:**

* **Priority Thread Pooling:** Background tasks have strict priorities. A MemTable flush is **Urgent** (prevents database stalls). L0 compaction is **High** (clears hot local disk files), and deep-level compactions are **Low**.
* **Global Memory Accounting:** Memory usage across all tables is tracked globally to ensure the node stays within its provisioned cloud boundaries.

**Challenges & Edge Cases Addressed:**

* **Memory Exhaustion (Write Stalls):** If writes come in faster than the disk can flush, memory will eventually be exhausted. As memory approaches a "soft limit," writer threads are subjected to micro-stalls. If a hard limit is hit, writers are safely blocked via Condition Variables until background flushes catch up.
* **Priority Inversion:** If low-level compactions tie up all background threads, a critical MemTable flush might be delayed, causing a Write Stall. Strict priority queues ensure flushes always preempt compactions.
* **Graceful Teardown Deadlocks:** Shutting down a high-performance database is notoriously difficult. If the shutdown sequence holds a lock that a background flush thread needs to report completion, the system deadlocks forever. The architecture uses strict separation of "Operation Leases" (active reads) and "Ownership References" (structural pointers). A shutdown safely stops new traffic, waits via Condition Variables for active jobs to drain, dumps final memory synchronously, and only then tears down the pointers.
