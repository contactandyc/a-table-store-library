# a-table-store-library

`a-table-store-library` is an embedded key-value storage engine implemented in C. It is built upon a Log-Structured Merge-Tree (LSM-Tree) architecture, designed to handle write-heavy workloads and provide a persistent storage layer for higher-level execution engines (such as a SQL Virtual Machine).

Because it is embedded, it runs directly within your application's process and manages its own directory of files on disk.

## Architecture & Features (Why use it)

LSM-Trees trade sequential disk write performance against read amplification. Rather than updating records in-place (like a B-Tree), all writes are buffered in memory and sequentially flushed to immutable files.

This library implements standard industry techniques to manage this data lifecycle:

* **MVCC (Multi-Version Concurrency Control):** The engine does not overwrite data. Keys are packed with a 56-bit sequence number and an 8-bit operation type (PUT or DELETE). This allows for point-in-time snapshots and non-blocking reads.
* **MemTable:** Active writes are absorbed into a lock-free SkipList in RAM. Once a memory limit is reached, it is flushed to disk.
* **SSTables (Sorted String Tables):** Data is flushed to disk as immutable, strictly sorted files. The files are divided into 16KB data blocks utilizing **LZ4 compression** and **prefix delta-encoding** to minimize storage footprints.
* **Read Optimizations:** Each SSTable contains an embedded **Bloom Filter** (using xxHash) and a sparse Index Block. Point lookups can often reject missing keys with zero disk I/O, or locate the exact compressed block with a single read.
* **Leveled Compaction:** A background process monitors file sizes and counts. When thresholds are crossed, it performs an N-Way Merge to push data into lower levels (L0 → L6), dropping deleted records (tombstones) and older versions to reclaim space and maintain read performance.
* **Global Merging Iterator:** The library provides an iterator that merges the active MemTable, L0, and L1+ files in real-time using a Priority Min-Heap. It automatically strips MVCC metadata, yielding a contiguous, strictly sorted stream of the newest valid records.

## Usage (How to use it)

The primary interface is exposed through `lsm_db.h`. The engine treats both keys and values as opaque byte arrays (`void*`), meaning you must serialize your structures before insertion.

### 1. Initialization and Cleanup

Open the database by providing a directory path and a maximum memory limit for the MemTable (e.g., 64MB). If the directory does not exist, you must create it prior to calling `lsm_db_open`.

```c
#include "a-table-store-library/lsm_db.h"

// Open DB with a 64MB MemTable limit
lsm_db_t *db = lsm_db_open("/var/data/my_database", 64 * 1024 * 1024);

// ... perform operations ...

// Safely flushes remaining memory to disk and cleans up
lsm_db_close(db);

```

### 2. Point Operations (CRUD)

Standard key-value operations. Note that `lsm_db_delete` does not immediately remove the data from disk; it inserts a tombstone marker that shadows older records until they are garbage collected during compaction.

```c
int user_id = 42;
const char *user_data = "{\"name\": \"Alice\", \"age\": 30}";
uint32_t val_len = strlen(user_data) + 1; // Include null terminator

// Insert or Update
lsm_db_put(db, &user_id, sizeof(int), user_data, val_len);

// Delete
lsm_db_delete(db, &user_id, sizeof(int));

// Retrieve
uint32_t out_len;
void *result = lsm_db_get(db, &user_id, sizeof(int), &out_len);
if (result) {
    printf("Found: %s\n", (char *)result);
    free(result); // Caller is responsible for freeing the returned memory
}

```

### 3. Iteration (Table Scans)

The iterator provides a unified view of the database. It is essential for operations like `SELECT * FROM table`, range scans, or bridging the storage layer to a SQL execution engine.

```c
lsm_db_iter_t *iter = lsm_db_iter_init(db);

const void *key;
const void *val;
uint32_t klen, vlen;

// lsm_db_iter_next automatically hides deleted records and older versions
while (lsm_db_iter_next(iter, &key, &klen, &val, &vlen)) {
    int current_id;
    memcpy(&current_id, key, sizeof(int));
    
    printf("ID: %d | Data: %s\n", current_id, (const char *)val);
}

lsm_db_iter_destroy(iter);

```

## Internal Module Layout

If you wish to modify the engine or understand its internals, the codebase is separated into strict boundaries:

* **`memtable.c`**: In-memory SkipList. Formats user keys into Internal Keys (appending SeqNum and OpType).
* **`sstable_builder.c`**: Handles block chunking, compression, index generation, and bloom filter creation. Splits output files when they reach `TARGET_FILE_SIZE` (default 2MB).
* **`sstable_reader.c`**: Memory-maps or caches SSTable footers, performing point-lookups and sequential iteration over compressed blocks.
* **`lsm_compaction.c`**: Contains the Database Manifest (tracking which files exist in which level). Manages overlap detection and executes the N-Way Merge algorithm.
* **`lsm_db.c`**: The orchestrator. Assigns sequence numbers, routes writes to the MemTable, triggers background flushes/compactions, and manages the unified Min-Heap iterator.
