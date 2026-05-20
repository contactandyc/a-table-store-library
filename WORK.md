Here is a comprehensive summary of everything we accomplished in Phases 1 and 2. We successfully transitioned the engine from a prototype into a thread-safe, high-throughput system capable of handling extreme concurrency and power-loss recovery.

### Phase 1: Object Lifecycle & Concurrency Hardening

This phase focused on eliminating Time-of-Check to Time-of-Use (TOCTOU) races, use-after-free vulnerabilities, and recursive deadlocks during database teardown.

* **Decoupled Memory Ownership from Execution State:** Replaced the monolithic `ref_count` with a three-pillar tracking system: `owner_refs` (memory lifetime), `active_ops` (public API readers/writers/iterators), and `active_jobs` (background flushes/compactions).
* **Safe Operation Leases (`db_try_acquire_op`):** Eliminated the TOCTOU race where a database could be closed and freed immediately after a thread checked the `is_closing` flag but before it acquired a lock. All public APIs now acquire a strict operation lease under the `state_mutex` before proceeding.
* **Idempotent & Deadlock-Free Teardown:** Fixed `lsm_env_destroy` so it no longer takes a self-retaining lock that blocks the database from closing. `lsm_db_close` now safely stalls until all `active_ops` and `active_jobs` drain to zero before destroying mutexes and reclaiming memory.
* **Fail-Safe Background Pool Submissions:** Modified `lsm_pool_submit` to return a boolean instead of `void`. If the background pool is shutting down, the database now gracefully catches the rejection, rolls back its `is_flushing`/`is_compacting` flags, and safely executes final jobs synchronously.
* **Recursive Mutex Resolution:** Identified and resolved a recursive mutex deadlock where background jobs attempted to acquire the `state_mutex` while it was already held by the teardown sequence.

### Phase 2: High-Throughput Group Commit & WAL Integration

This phase focused on unblocking the I/O pipeline, guaranteeing write durability, and plugging in a dedicated Write-Ahead Log engine.

* **Queue Admission Decoupling:** Replaced the monolithic `write_mutex` with a dedicated `queue_mutex` for Group Commit admission. The leader now builds the super-batch and instantly drops the queue lock, allowing concurrent threads to continue enqueuing while disk I/O happens in the background.
* **Baton-Pass Deadlock Resolution:** Fixed a critical bug where a leader capping its super-batch at 1MB would leave remaining writers asleep forever. The leader now explicitly signals the successor (`writer_queue_head->cv`) immediately after detaching its batch.
* **Strict I/O Atomicity:** Enforced perfect sequence ordering by holding the `write_mutex` only during the sequential assignment, WAL appending, and MemTable application. If a WAL append fails at the OS level, the sequence number is rolled back and the MemTable remains untouched.
* **Fair & Transparent Backpressure:** Moved `stall_for_memory` to execute strictly *after* the commit phase and entirely outside of database locks. This forces both leaders and fast-returning followers to pay the memory stall tax equitably, and prevents the system from falsely returning a failure for a write that was successfully committed to disk.
* **Pluggable `pool_wal` Integration:** Replaced the generic WAL stub with the native `pool_wal_t` engine. We embedded the `pool_to_lsm_wal` translation layer directly into the WAL library, creating a bridge that securely multiplexes `table_id` tracking and safely coordinates garbage collection (purging) across multiple active databases.
* **Sequence Metadata Embedding & Pull Iteration:** Injected explicit 8-byte sequence metadata (`start_seq` and `total_count`) into the binary WAL payloads. Transitioned the WAL replay system from a callback model to a native pull-based iterator (`pool_wal_iter_t`) to seamlessly align with the LSM environment's multi-table recovery logic.

### Phase 3: Harden Recovery and Disk Parsing

This phase ensures the database cannot be crashed or exploited by corrupted, torn, or malicious disk data.

* **WAL Validation:** Parse and validate entire WAL batches before replaying them. If a batch is malformed, reject it entirely rather than applying a partial prefix.
* **Manifest Caps:** Add strict record size caps and per-field bounds checks during manifest replay.
* **SSTable Bounds Checks:** Enforce rigid bounds checks for SSTable metadata and index parsing (e.g., ensuring `idx->size >= 9` and `block_size >= 4`).
* **Secure Decompression:** Verify the restart-table, compression-flag, CRC, and decompression sizes safely before acting on the data.
* **Bitmap Filter Fix:** Correct the bitmap filter so that out-of-range queries are treated as a "maybe" (proceeding to the index) rather than a false negative.
* **Cache Pointer Safety:** Replace the iterator's unsafe `uint64_t cached_offset` cast with a strongly typed `lsm_cache_handle_t *` pointer.

### Phase 4: Make Flush/Compaction Transactional

This phase guarantees that background I/O operations are fully atomic and gracefully recover from storage failures.

* **Strict Return Checking:** Check the result of every builder, append, finish, manifest edit, and fsync operation.
* **Delayed Checkpoints:** Only checkpoint the WAL *after* SSTable data, SSTable metadata, and the manifest edit are fully synced and guaranteed durable.
* **Orphan Cleanup:** Automatically delete partial or corrupted files upon any I/O failure.
* **Compaction Backoff:** Break out of or back off compaction loops on failure to prevent the background thread from endlessly spinning on a bad disk sector.
* **Accurate Pointers:** Move the compaction pointer update so it occurs *after* the final input expansion, ensuring it accurately represents the compacted range.

### Phase 5: Comprehensive Testing

Before moving on to optimizations, the new architecture must be proven stable under extreme concurrency and corruption.

* **TSAN Validation:** ThreadSanitizer tests verifying `close` against concurrent `write`/`get`/`iterator` ops, and `env_destroy` while the DB has active background jobs.
* **Group Commit Tests:** Verify that batch caps successfully leave a successor writer, WAL append failures back out cleanly, and backpressure timeouts don't trigger false failures post-commit.
* **Lifecycle Tests:** Ensure rejected flush/compaction jobs from pool shutdowns are handled safely.
* **Fuzz Testing:**
* *WAL:* Ensure malformed batches replay zero entries.
* *Manifest:* Test against huge `record_len`, massive `num_add`, and bad key lengths.
* *SSTable:* Test against tiny `idx->size`, bad restart counts, and bad LZ4 compression sizes.


* **Filter & Cache Tests:** Verify the bitmap filter's out-of-range behavior and ensure the cache can be destroyed safely even if pinned handles remain.