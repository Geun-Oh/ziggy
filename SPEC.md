# **Project ziggy: Exhaustive Production-Grade LSM-Tree Storage Engine Specification**

## **1\. Executive Summary and Architectural Philosophy**

The modern data processing landscape demands embedded storage engines capable of seamlessly balancing high-throughput data ingestion with predictable, low-latency read operations, all while maintaining absolute data integrity under highly concurrent access and catastrophic system failures. Project ziggy is a production-grade, high-performance embedded Key-Value storage engine formulated entirely in the Zig programming language. It leverages a tightly optimized Log-Structured Merge-Tree (LSM-Tree) architecture heavily grounded in the concept of "Mechanical Sympathy"—the alignment of software algorithms with the underlying physical realities of processor caches, memory allocators, and non-volatile storage media.

Unlike conventional or hobbyist storage layers, ziggy synthesizes proven industry-standard practices from leading architectures such as RocksDB 1, WiredTiger 2, LevelDB 4, and TigerBeetle.6 It strictly adheres to operational rigor, offering deterministic memory management without the unpredictable pauses of garbage collection, rigorous Write-Ahead Log (WAL) archiving for true Point-In-Time Recovery (PITR), and sophisticated multi-version concurrency control. The architecture deliberately eschews global locks, virtual function overhead, and implicit memory allocations, instead capitalizing on Zig's compile-time (comptime) generics, explicit custom allocators, and deterministic error-set propagation. This specification establishes the exhaustive blueprint for ziggy's implementation, systematically detailing transaction managing, lock managing, access protocols, buffer pooling, and disaster recovery capabilities required for an engine to be deemed production-grade.

## **2\. The Durability Paradigm: Write-Ahead Logging and Atomic Manifests**

Absolute durability is the foundational pillar of any production database. The storage engine must guarantee that once a write operation acknowledges success to the client, the data will survive arbitrary process terminations, kernel panics, or power failures. To achieve this, data must transition through meticulously designed disk and memory structures, relying on the Write-Ahead Log (WAL) for immediate durability and an atomic Manifest file for structural consistency.

### **2.1 Write-Ahead Log (WAL) Serialization and Framing**

The Write-Ahead Log serves as the absolute, single source of truth for the storage engine.8 Before any write operation or transaction commit acknowledges success, the payload must be serialized, appended to the WAL, and explicitly synchronized to non-volatile storage via fdatasync or written utilizing O\_DIRECT semantics to bypass the volatile operating system page cache.10

To prevent data corruption resulting from partial writes during catastrophic power loss (torn pages), the WAL is divided into fixed-size physical blocks, typically 32 KB in size, echoing the structural implementations of LevelDB and RocksDB.4 A logical record that exceeds the remaining physical space in a 32 KB block is automatically fragmented across multiple consecutive blocks. Each block consists of a sequence of these record fragments.

To ensure verifiable data integrity upon read and recovery, every record fragment is prefixed with a strict 7-byte binary header prior to the payload.4

| Offset | Size (Bytes) | Field Name | Functional Description |
| :---- | :---- | :---- | :---- |
| 0 | 4 | Checksum | A CRC32C cryptographic checksum calculated over the record type and the payload, ensuring bit-rot or sector corruption is detected immediately upon log replay.4 |
| 4 | 2 | Length | The byte length of the specific payload fragment contained within the current block, allowing the parser to advance pointers precisely.4 |
| 6 | 1 | Type | A categorical marker indicating the fragment's position: FULL for self-contained records, FIRST for the beginning of a split record, MIDDLE for intermediate fragments, and LAST for the concluding fragment.4 |
| 7 | Variable | Payload | The actual serialized transaction data, consisting of batch headers, sequence numbers, operation types, keys, and values. |

The payload itself is encapsulated within a broader batch format. A batch begins with a 12-byte header containing an 8-byte SequenceNumber—which dictates the isolation visibility of the batch—and a 4-byte Count representing the total number of distinct key-value operations contained within the payload.4 By chunking records into checksum-protected 32 KB blocks, ziggy ensures that physical hardware sector boundaries (typically 512 bytes or 4 KB) align cleanly with logical engine boundaries, rendering torn writes mathematically detectable and safely ignorable during disaster recovery routines.

### **2.2 The Manifest File and Atomic State Transitions**

While the WAL protects the actual user data, the internal structural integrity of the LSM-Tree is equally critical. A database crash during a background compaction cycle can leave the storage engine with an inconsistent topology of overlapping, partially written, or orphaned Sorted String Table (SSTable) files. To enforce the durability rule across metadata, all changes to the LSM-Tree structure are recorded in an atomic MANIFEST file.4

The Manifest operates as a specialized transactional log dedicated solely to metadata mutations, formalized as VersionEdits.13 As the state of the database evolves, the engine appends VersionEdit records to the Manifest.

| VersionEdit Field | Operational Purpose |
| :---- | :---- |
| NewFiles | Records the addition of newly generated SSTables to specific compaction levels, including file sizes and bounding key ranges (minimum and maximum keys). |
| DeletedFiles | Records the logical deletion of obsolete SSTables from specific levels following a successful compaction merge.12 |
| NextSequence | Captures the next available global SequenceNumber to ensure transactions resume correctly after a restart.13 |
| NextFileNumber | Tracks the monotonically increasing file identifier used to generate unique filenames for new WALs and SSTables.13 |
| CompactionPointers | Stores the last compacted key for each level, ensuring that subsequent background compactions cycle evenly through the key space. |

To guarantee absolute crash consistency upon startup, ziggy employs a discrete CURRENT file, which acts as a static pointer containing the exact filename of the currently active MANIFEST.13 When the storage engine initializes, it reads the CURRENT file, parses the designated MANIFEST, and sequentially replays the chain of VersionEdits to reconstruct the exact VersionSet—the comprehensive layout of all files across all levels—that was present at the exact moment of the last successful operation.13

Creating a new Manifest file requires a strict, atomic filesystem protocol. The engine writes the initial VersionSet snapshot to a new Manifest file, explicitly calls fsync to flush the file metadata to disk, creates a temporary file containing the new Manifest's filename, and ultimately executes an atomic filesystem rename operation to overwrite the CURRENT file.17 Because POSIX rename operations are guaranteed to be atomic at the kernel level, the database can never observe a partially updated CURRENT file.

## **3\. Memory-Resident Structures: The Concurrent SkipList MemTable**

Data successfully appended and synchronized to the WAL is subsequently inserted into the MemTable. The MemTable functions as an in-memory buffer that ingests raw, unordered writes and organizes them sequentially to facilitate efficient range scans and eventual sequential flushing to persistent disk structures.18 To permit highly concurrent ingestion workloads without forcing reader threads to block or stall, ziggy implements the MemTable as a completely lock-free Concurrent SkipList.20

### **3.1 Lock-Free Mechanics and Atomic Memory Ordering**

Achieving true thread-safety in a lock-free SkipList necessitates precise, granular control over hardware memory ordering to prevent processor instruction reordering and multi-core cache-coherency anomalies. Zig provides low-level atomic primitives, such as @atomicLoad, @atomicStore, and @cmpxchgWeak, which map directly to the underlying CPU architecture's synchronization instructions.22

The skip-list is built upon multiple probabilistic layers of linked lists, where higher layers act as express lanes for rapid traversal.21 New nodes are constructed with randomized tower heights. The insertion protocol operates strictly through Compare-And-Swap (CAS) operations combined with Release memory ordering semantics.25 When a writer thread inserts a node, it first fully populates the node's key, value, and internal pointers. Only then does it execute a CAS operation to swing the predecessor's pointer to the newly allocated node.

Reader threads traverse the skip-list asynchronously using atomic loads with Acquire memory ordering.25 This strict Acquire-Release semantic pairing is critical: it mathematically guarantees that once a reader thread observes a pointer to a newly inserted node, all initialization data (the key and the value) written to that node by the writer thread is unconditionally visible to the reader.25 Without these explicit memory barriers, out-of-order execution could lead a reader to traverse a valid pointer but read uninitialized or garbage memory, resulting in catastrophic database corruption.

### **3.2 Comptime Generics for Zero-Cost Comparisons**

A critical performance bottleneck in traditional C++ storage engines arises from the requirement to process generic Key and Value types, which is conventionally handled via abstract classes and virtual functions (e.g., Comparator::Compare). Virtual function dispatches disrupt CPU branch predictors, invalidate instruction pipelines, and fundamentally prevent the compiler from inlining small, highly repetitive comparison logic.

ziggy eliminates this overhead entirely by utilizing Zig's comptime (compile-time) parameters to specialize the entire LSM-Tree logic for specific Key and Value memory layouts and comparator functions.6 By injecting the comparator logic at compile time, the Zig compiler is capable of heavily inlining the comparison calls during K-Way Merges, SkipList traversals, and binary searches. If the key is a simple 64-bit unsigned integer (u64), the entire comparison reduces to a single, branchless CPU instruction, achieving raw, bare-metal performance identical to handwritten, type-specific C code.6

## **4\. Immutable Disk Structures: SSTables and Slotted Pages**

When the concurrent MemTable reaches its predefined capacity threshold—managed dynamically by the Write Buffer Manager—it is frozen into an immutable state and flushed sequentially to disk as a Sorted String Table (SSTable).13 The structural format of the SSTable dictates the physical boundaries of Read Amplification and Space Amplification within the database. To achieve extreme performance, ziggy utilizes a highly structured block layout combined with aggressive compression techniques.

### **4.1 The Slotted Page Block Architecture**

Each SSTable file is partitioned into a sequence of fixed-size physical blocks, typically ranging from 4 KB to 16 KB. To achieve ![][image1] point-lookups within a block without resorting to linear scans, ziggy utilizes the Slotted Page architecture, a proven paradigm in relational database page layouts adapted here for key-value payloads.29

The slotted page layout physically separates the logical ordering of records from their actual physical placement within the block.

| Block Region | Placement | Technical Description |
| :---- | :---- | :---- |
| **Page Header** | Top of Block | Contains critical metadata including the block offset, a CRC32C checksum, the total number of active slots, and byte offsets indicating the start and end of the unallocated free space gap.29 |
| **Tuple Data** | Grows Downward | The variable-length key-value pairs, which are appended sequentially from the header downwards toward the center of the block. |
| **Free Space** | Center | The contiguous gap of unallocated memory separating the downward-growing tuple data from the upward-growing slot array.33 |
| **Slot Array** | Grows Upward | An array of 2-byte or 4-byte integers located at the very end of the block, growing upwards. Each element contains the exact physical byte offset of a specific tuple.29 |

When a binary search is executed within a block to find a specific key, the search algorithm iterates strictly over the Slot Array.30 Because the slot array elements are fixed-size integers, the engine can compute memory addresses instantaneously. The slot array pointers are then dereferenced to inspect the actual variable-length keys. Furthermore, because the slot array abstracts the physical offsets, the engine can transparently perform internal block defragmentation—compacting deleted or updated tuples and shifting live data—without altering the logical index ordering.30

### **4.2 Prefix Compression and Delta Encoding Serialization**

Because keys in an LSM-Tree SSTable are strictly sorted lexicographically, adjacent keys frequently share extensively long byte prefixes. To drastically optimize physical space and minimize disk I/O bottlenecks, ziggy enforces Prefix Compression (also known as Front Compression) combined with Delta Encoding for timestamps and numerical metadata.34

Instead of storing full, redundant keys, each key-value entry mathematically encodes only the differential data. The binary representation of a compressed entry consists of:

1. Shared Prefix Length: Encoded as a Variable-Length Integer (VarInt).  
2. Unshared Suffix Length: Encoded as a VarInt.  
3. Value Length: Encoded as a VarInt.  
4. Unshared Suffix Bytes: The actual raw bytes that differ from the preceding key.  
5. Value Bytes: The raw payload.

However, unrestrained prefix compression introduces a severe performance penalty: resolving the full value of a key at the end of a block would require a strictly linear scan from the very first key to reconstruct the accumulated prefixes. To prevent this, ziggy establishes "Restart Points" at fixed intervals, typically every 16 keys.36 At a restart point, the prefix compression state is intentionally broken; the Shared Prefix Length is forced to 0, and the entire raw key is written out sequentially.36 The slotted page footer contains a dedicated array of offsets pointing directly to these restart points. During a read operation, the binary search algorithm jumps to the closest preceding restart point and performs a rapid, bounded linear scan forward, decoding at most 15 compressed entries to find the exact target.

## **5\. Transaction Management and Multi-Version Concurrency Control (MVCC)**

A production-grade storage engine cannot force read operations to wait for write operations to complete, nor can it allow heavy write workloads to stall analytical queries. To provide high-throughput, non-blocking operations, ziggy implements strict Snapshot Isolation (SI) using a robust Multi-Version Concurrency Control (MVCC) framework.37

### **5.1 Snapshot Isolation and Global Sequence Numbers**

When a transaction initializes, it requires a mathematically consistent view of the database at that exact microsecond. In ziggy, similar to the implementations found in WiredTiger and RocksDB, this snapshot is intrinsically tied to a monotonically increasing, 64-bit global SequenceNumber.2 Every distinct mutation appended to the WAL and MemTable—whether an insertion, update, or deletion—is indelibly tagged with the system's current SequenceNumber at the time of commit.

When a transaction ![][image2] requests a read snapshot, it records the highest globally committed sequence number ![][image3]. From that point forward, the transaction is completely immune to any subsequent database modifications.

| Scenario | Encountered Key Sequence (SK​) | Visibility Resolution |
| :---- | :---- | :---- |
| **Historical Version** | **![][image4]** | The mutation was committed before or exactly when the transaction started. The engine accepts this as the valid state of the key.38 |
| **Future Mutation** | **![][image5]** | The mutation was committed after the transaction started. The engine actively ignores this record and continues searching deeper for an older, valid version.2 |
| **Tombstone Marker** | **![][image4]** (Type: Delete) | The most recent valid version is a deletion marker. The engine yields a KeyNotFound response, masking any older valid data that might still exist on disk.38 |

This MVCC design guarantees that readers never acquire locks, drastically improving concurrency. However, Snapshot Isolation is susceptible to a specific concurrency anomaly known as Write Skew. To address varying workload constraints, ziggy exposes dual transaction execution modes: Optimistic and Pessimistic concurrency control.1

### **5.2 Optimistic and Pessimistic Concurrency Models**

Engineers integrating ziggy can programmatically tune the transaction behavior based on the expected level of data contention within their specific application domains.

**Optimistic Transactions** are engineered for read-heavy workloads with sparse write conflicts. In this mode, the transaction acquires absolutely no locks during its read or write phases.39 Instead, it maintains a private workspace—a local write batch buffer—alongside a tracking set of all keys it has read, capturing their exact SequenceNumber at the moment of access.39 When the application issues a Commit() command, the engine enters a critical validation phase. It compares the sequence numbers of the tracked read set against the current global state of the database. If any key has been mutated by another transaction in the interim, the validation fails, and the optimistic transaction is aborted immediately, returning a conflict error to the client to trigger a retry.39

**Pessimistic Transactions** are designed for environments experiencing heavy, localized write contention where optimistic retries would cause unacceptable CPU thrashing. In this paradigm, row-level locks are acquired dynamically as the transaction progresses and interacts with keys.41 The transaction modifies keys within its local batch, but forcefully acquires an exclusive lock for each targeted key via the Lock Manager. During the final Commit() sequence, the write batch is atomically flushed to the WAL and MemTable, and the held locks are subsequently released, guaranteeing execution success provided the database has not exhausted its storage capacity.39

## **6\. Lock Managing and Deadlock Resolution Algorithms**

The integration of Pessimistic transactions necessitates a highly optimized Lock Manager to orchestrate and serialize concurrent access to individual keys without becoming a global system bottleneck.

### **6.1 Sharded Row-Level Locking Architecture**

A naive lock manager utilizing a single global mutex to track lock states would instantly paralyze a many-core processor. To mitigate thread contention and false sharing at the CPU cache level, ziggy's Lock Manager utilizes a heavily sharded hash table architecture mapping keys to Lock Objects.39 The hash table is divided into multiple independent stripes (e.g., 256 or 1024 discrete zones), where each stripe is independently protected by its own lightweight mutex and condition variable (CondVar).39

When a transaction attempts to modify a key, it computes a cryptographic hash of the key and maps it to a specific stripe. It acquires the mutex exclusively for that stripe. If the targeted key is currently locked by a rival transaction, the requesting transaction appends its internal Transaction ID to the specific lock's wait-list and voluntarily suspends execution via the CondVar.39 When the rival transaction commits and releases the lock, it signals the CondVar, awaking the suspended transaction to claim the lock.

### **6.2 Deadlock Detection via Wait-For Graphs**

Because pessimistic transactions acquire locks incrementally over their lifespan rather than atomically upfront, the system is inherently vulnerable to circular dependencies, commonly known as deadlocks.42 For example, Transaction A holds Lock X and suspends while waiting for Lock Y, while Transaction B holds Lock Y and suspends while waiting for Lock X. Under these conditions, neither transaction can ever proceed, causing catastrophic application hangs.

To resolve this programmatically, the Lock Manager implements an active cycle detection algorithm. Whenever a transaction is forced to suspend and wait on a lock, the Lock Manager constructs a directed Wait-For Graph (WFG) representing the entire system's current locking dependencies.42 The algorithm executes a strict Breadth-First Search (BFS) graph traversal starting directly from the blocked transaction's node.39 If the traversal path loops back and encounters the starting transaction ID, a circular dependency is confirmed. Upon detecting the cycle, the Lock Manager immediately acts as a terminator, aborting the most recent requesting transaction and returning a Status::Deadlock error. This aggressive intervention breaks the cycle, rolling back the victim's local state and allowing the surviving transactions to proceed to completion.43

## **7\. Access Management and Iterator Ecosystem**

Retrieving the correct version of a key across a deeply nested hierarchy of in-memory buffers and on-disk files requires a sophisticated Access Management layer. This layer is responsible for aggressively minimizing excessive Disk I/O and CPU utilization during queries.

### **7.1 Tiered Search Strategy and Bloom Filter Mathematics**

When an application processes a Get(key) request under a specific snapshot, the access manager must probe the storage structures in strict chronological order of recency to guarantee the newest valid version is returned.

1. **Active MemTable:** The in-memory Concurrent SkipList currently absorbing live writes.38  
2. **Immutable MemTables:** SkipLists that have reached capacity, are closed for writing, and are queued for background flushing.13  
3. **Level 0 (L0) SSTables:** Files flushed directly from memory. Crucially, L0 files possess overlapping key ranges. The engine must identify and search all L0 files whose minimum and maximum key boundaries encapsulate the target key.44  
4. **Level 1 to N (L1..LN) SSTables:** Deeper levels are strictly partitioned. Files within a single level are mutually exclusive and non-overlapping, meaning only a maximum of one file per level needs to be interrogated.44

To severely mitigate Read Amplification—the severe latency penalty of performing physical disk I/O for keys that do not actually exist within a given SSTable—ziggy mandates the generation of Bloom Filters for every data block.45 A Bloom filter is a probabilistic space-efficient data structure comprising an array of bits and a set of cryptographic hash functions used to test set membership.

When searching an SSTable, the key is hashed. If the Bloom filter yields a "Negative" result, the mathematics guarantee the key is absolutely absent, allowing the engine to entirely bypass the disk seek for that file. A "Positive" result indicates the key *might* exist, triggering a block lookup.45 The mathematical equation governing the false positive rate (![][image6]) is ![][image7], where ![][image8] is the number of hash functions, ![][image9] is the number of inserted elements, and ![][image10] is the bit array size. By default, ziggy allocates roughly 10 bits per key and utilizes optimal hash function counts to drive the false positive rate down to approximately 1%, ensuring 99% of useless disk seeks are aborted in memory.44

### **7.2 The K-Way Merging Iterator and Min-Heaps**

Range scans (e.g., retrieving all keys between "user:1000" and "user:2000") and background compaction processes require iterating over multiple overlapping sorted runs simultaneously in perfect lexicographical order. To accomplish this complex traversal, the Access Manager deploys a K-Way Merging Iterator.46

The K-Way Merge algorithm utilizes a Min-Heap (priority queue) data structure to continuously surface the smallest key across ![][image11] files.47 The initialization protocol opens a cursor for each overlapping structure and pushes the first element of every cursor into the Min-Heap. The heap organizes the elements by Key. If multiple cursors yield the exact same key—representing historical revisions of that data point—the heap's comparison function tie-breaks using the SequenceNumber, strictly prioritizing the most recent version to the top.47

When the client calls Next(), the iterator pops the absolute smallest element from the heap. It then advances the specific underlying cursor that sourced that element and pushes the newly retrieved element back into the heap. This continuous abstraction isolates the immense complexity of multi-file resolution, allowing point-in-time range queries to execute with optimal ![][image12] algorithmic complexity.47

## **8\. Buffer Management and The HyperClockCache Architecture**

Because Disk I/O operates orders of magnitude slower than RAM access, the Buffer Manager is the most critical component dictating read latency. It is responsible for intelligently deciding which disk blocks reside in memory and which are evicted. Efficient Buffer Management relies on algorithms that delicately balance recency of access, frequency of access, and—crucially for modern CPUs—multithreading lock contention.27

### **8.1 Eviction Algorithms and Lock-Free Innovations**

Traditional database buffer pools utilize a Least Recently Used (LRU) eviction policy, typically implemented via a doubly-linked list paired with a hash map.50 While logically sound, in highly concurrent multi-core environments, standard LRU implementation becomes a catastrophic throughput bottleneck. Every single read operation, even for data already safely cached, requires a thread to acquire an exclusive global mutex simply to unlink the block's node and move it to the head of the LRU list to update its recency.1

To achieve uncompromised production-grade performance, ziggy adopts a Lock-Free Clock Cache architecture, heavily inspired by the HyperClockCache (HCC) pioneered by RocksDB.1 The eviction algorithm operates conceptually as a massive circular buffer with a sweeping, continuous "clock hand".49 To entirely eliminate mutexes on the critical read path, the Buffer Manager compresses all required operational metadata for a block slot into a single, atomic 64-bit integer.20

| Bit Range | Metadata Field | Architectural Function |
| :---- | :---- | :---- |
| **Bits 0-29** | Acquire Count | Tracks the number of active reader threads currently pinning the block in memory to prevent premature eviction. |
| **Bits 30-59** | Release Count | Tracks reader threads that have finished processing. The true live reference count is calculated instantly via Acquire Count \- Release Count. |
| **Bits 60-62** | State | Encodes the slot's lifecycle phase (Empty, Under Construction, Occupied, Invisible), ensuring threads don't read partially loaded blocks.20 |
| **Bit 63** | Hit / Priority | Set atomically to 1 when a cached block is accessed, granting it immunity during the next eviction sweep.20 |

When the cache reaches its memory limit, the clock hand algorithm sweeps sequentially through the 64-bit slots. If it inspects a block with a reference count of 0 and the Hit bit set to 1, it atomically clears the Hit bit (aging the block) and advances. If it discovers a block with a reference count of 0 and the Hit bit already at 0, the block is selected for immediate eviction.1 This elegant design allows Get operations to update cache priority with a single, wait-free atomic hardware instruction, drastically elevating read throughput on highly parallel processors.

### **8.2 Strict Reference Counting and Explicit Handle Release**

Because the Zig language fundamentally relies on explicit memory management rather than automated garbage collection, the Buffer Manager enforces a Handle-based reference counting paradigm.52 When an Access Manager requests a specific block, the Buffer Pool returns a CacheHandle. The issuance of this handle automatically increments the block's atomic Acquire Count.

The utilizing function is explicitly responsible for calling Cache.Release(handle) to increment the Release Count. In idiomatic Zig, this is typically enforced by wrapping the release call in a defer statement immediately after acquisition.53 Failure to release the block prevents the clock sweep algorithm from ever evicting it, leading directly to Out of Memory (OOM) fatal conditions. This strict methodology perfectly aligns with ziggy's core philosophy of explicit, deterministic control flows and programmatic accountability.10

## **9\. Compaction Strategies and The RUM Conjecture**

The theoretical foundation of all storage engines is governed by the RUM Conjecture, which mathematically asserts that a storage system can optimize at most two of the following three overheads simultaneously: Read amplification (R), Update/Write amplification (U), and Memory/Space amplification (M).1 B-Trees traditionally optimize for Reads and Space at the heavy cost of random Write amplification. LSM-Trees inherently trade higher Read Amplification and Space Amplification in favor of exceptional, sequential Update performance.55

Background Compaction is the critical maintenance process designed to pay down the technical debt accumulated by these rapid writes, reigning in the expanded Space and Read amplification.27

### **9.1 Leveled Compaction Dynamics**

ziggy utilizes a strict Leveled Compaction strategy, modeled closely on RocksDB's default algorithm, to impose rigid bounds on Space Amplification.1 The disk layout is stratified into distinct tiers.

| Tier | Structure Type | Amplification Characteristics |
| :---- | :---- | :---- |
| **Level 0 (L0)** | Overlapping Files | Files are flushed directly from MemTables. Because files overlap, queries must interrogate multiple files, increasing Read Amplification.13 |
| **Level 1 (L1)** | Partitioned Files | Files are mutually exclusive. Key ranges do not overlap. |
| **Level N (LN)** | Partitioned Files | Each subsequent level is exponentially larger than the preceding level.13 |

A deterministic target size is established for each level. Conventionally, each level is ten times larger than the preceding level (a ![][image13] multiplier).13 For example, if Level 1 is constrained to 10 MB, Level 2 will be 100 MB, and Level 3 will be 1 GB. When the total byte size of Level ![][image14] breaches its target threshold, a background thread is dispatched. It selects an SSTable from Level ![][image14] and merges it with all overlapping SSTables located in Level ![][image15].46 This merge-sort process produces new, strictly ordered SSTables in Level ![][image15], subsequently deleting the old input files. By enforcing strict size multipliers, ziggy mathematically bounds the maximum space overhead to roughly 1.11x the size of the raw data.

### **9.2 Tombstone Garbage Collection**

During the merge process—powered by the aforementioned K-Way Merge Iterator—the compactor actively executes critical Garbage Collection.27 If the compactor encounters multiple historical versions of a single key, it discards the older variants, retaining only the version with the highest SequenceNumber that remains visible to the oldest active transaction snapshot.

Crucially, if a Tombstone marker (a deletion record) represents the most recent version of a key, the compactor attempts to reclaim the physical space. If the compactor can algorithmically prove that no older versions of the key exist in any deeper, older levels (![][image16]), the Tombstone is safely dropped entirely, permanently reclaiming the disk space.28 By adhering to these deterministic compaction triggers, ziggy ensures that high-churn workloads and long-lived deleted data do not consume unbounded storage resources.7

## **10\. Restore Management and Point-In-Time Recovery (PITR)**

While crash consistency protocols (WAL and Manifest recovery) protect against abrupt power loss or kernel panics, they offer zero protection against catastrophic human or application errors, such as executing an accidental DROP TABLE or widespread logical DELETE command. Robust disaster recovery relies on continuous WAL Archiving and Point-In-Time Recovery (PITR) infrastructure.60

### **10.1 Continuous WAL Archiving Infrastructure**

To facilitate PITR, the MANIFEST and the WAL cooperate to maintain an unbroken, immutable chain of transaction history extending indefinitely into the past. ziggy exposes programmatic hooks and configuration parameters conceptually identical to PostgreSQL's archive\_command.61

Whenever a 32 KB WAL block or a complete WAL segment file is filled and rotated out of active service, the storage engine triggers an asynchronous archive process.61 This dedicated process safely copies the immutable WAL segment to a highly durable, remote object storage location (e.g., an AWS S3 bucket, Google Cloud Storage, or an enterprise NFS mount) before the local node is permitted to delete or overwrite the file.61 This ensures that every mutation ever committed to the database exists safely off-site.

### **10.2 Executing Roll-Forward Recovery**

In the event of a catastrophic logical failure, administrators must recover the database to a precise microsecond before the error occurred (e.g., stopping replay exactly at Tuesday, 3:01:05 AM).61 The Restore Manager executes the following rigid protocol:

1. **Base Backup Restoration:** The corrupted system is completely halted. The administrator restores the core data directory from the last known good Base Backup (e.g., a filesystem snapshot generated the preceding Sunday).61  
2. **Manifest Verification:** The MANIFEST file embedded within the base backup is parsed to identify the valid starting global SequenceNumber and the requisite WAL LogNumber required to begin replay.13  
3. **WAL Retrieval and Verification:** The Restore Manager reaches out to the remote archive and sequentially downloads all WAL segments generated subsequent to the base backup. It reads the blocks, strictly verifying the CRC32C checksums to guarantee the archive itself has not suffered bit-rot.4  
4. **Targeted Replay:** The validated WAL payloads are replayed into memory and flushed to disk. Crucially, before applying any transaction batch, the batch's SequenceNumber and timestamp are scrutinized. The replay process abruptly and safely halts the exact moment it encounters a transaction timestamp matching or exceeding the user-provided recovery\_target\_time.61

By mandating continuous WAL retention and enabling programmatic replay boundaries, ziggy guarantees zero data loss up to the specific microsecond of administrative failure, a mandatory requirement for financial and enterprise-tier deployments.63

## **11\. Zig-Specific Constraints, Safety, and Observability**

The architectural decision to implement ziggy in Zig dictates highly performant paradigms that fundamentally distance it from C++ based engines like RocksDB or garbage-collected engines like BadgerDB. Zig provides syntax and language constraints that force developers to confront hardware realities.

### **11.1 Explicit Memory Management and Arena Allocators**

Zig forces explicit memory allocation. The language contains no hidden global allocators, malloc wrappers, or invisible garbage collection routines.10 In ziggy, every structural component—from the Buffer Pool to the SSTable Builder—requires an explicit Allocator interface passed as an argument during instantiation.10

To prevent heap fragmentation during complex read operations, ziggy utilizes Zig’s std.heap.ArenaAllocator.10 When a large SCAN request or a multi-key GET transaction begins, a temporary Arena is instantiated. All intermediate nodes, iterator states, and block decompression buffers are allocated linearly and consecutively within this Arena. Upon the request's completion, the entire Arena is deallocated in a single, instantaneous ![][image1] operation. This architecture guarantees zero memory fragmentation and completely avoids the massive CPU overhead associated with tracking and executing thousands of individual free calls.10 For persistent global structures, a standard std.heap.GeneralPurposeAllocator is utilized, which tracks allocation bounds and provides built-in, un-bypassable memory leak detection during execution.54

### **11.2 The No-Panic Policy and Explicit Error Sets**

A core tenet of production daemon systems is that the database must never crash unexpectedly. Out of Memory (OOM) conditions, disk exhaustion (ENOSPC), and file input/output errors (EIO) are realities of physical hardware and must be gracefully handled.66

In Zig, Panics are generally unrecoverable and reserved exclusively for catastrophic programmer logic errors (e.g., failing an invariant assertion).66 For all runtime and environmental errors, ziggy employs Zig's explicit Error Sets.68 If the operating system rejects a write to the WAL due to a full disk, the error error.DiskFull is explicitly bubbled up through the entire call stack utilizing the try keyword. Any partially constructed in-memory state is safely reverted using Zig's errdefer syntax, which guarantees that scoped cleanup logic executes only if the function exits prematurely via an error.54 The error reaches the highest client API boundary cleanly, allowing the application layer to throttle writes, alert monitoring systems, or attempt retries without the storage engine daemon crashing.67

### **11.3 Direct I/O (O\_DIRECT) and Compile-Time Alignment**

To achieve predictable tail latency, a high-performance database must bypass the Operating System's volatile Page Cache.11 Double-caching—where data is cached redundantly in the OS and again in the application's Buffer Pool—wastes critical memory and introduces massive latency spikes when the OS arbitrarily flushes dirty pages to disk.

ziggy opens all SSTable and WAL files using the Linux O\_DIRECT flag.10 However, operating systems strictly mandate that memory buffers used for O\_DIRECT I/O be aligned flawlessly to the physical block device's sector size (typically 4 KB or 512 bytes). Zig’s explicit pointer alignment syntax (e.g., align(4096)) allows the memory allocator to guarantee the required memory alignment for MemTable flush buffers and Block Cache slots natively at compile-time. This eliminates the need for complex, error-prone runtime alignment arithmetic or wasteful memory copying prior to execution.

### **11.4 Validation Methodologies and Observability**

A storage engine's reliability relies entirely on the rigor of its test coverage.

1. **Crash Consistency Validation:** Following methodologies pioneered by LevelDB and TigerBeetle, simulated testing environments utilize strict fault injection.5 The database process is randomly terminated (SIGKILL) during intensive, highly concurrent write phases. Upon restart, the WAL and Manifest recovery protocols are audited mathematically to verify that no partially written transactions exist, and no acknowledged data was lost.  
2. **Jepsen-Style Fuzzing:** The engine is subjected to linearizability checkers to verify that Snapshot Isolation and MVCC invariants hold flawlessly under extreme concurrency. Interleaving operations must exhibit perfectly predictable transaction causality.70  
3. **Descriptor Leak Detection:** Utilizing std.testing.allocator, every unit test suite automatically validates that all memory allocations and open file descriptors are properly disposed of, preventing the insidious resource leaks that plague long-running daemon deployments.54

Through strict adherence to these implementation details, ziggy establishes a new standard for embedded storage engines, combining the proven algorithmic foundations of LSM-Trees with the exacting mechanical sympathy inherent in the Zig language, resulting in a system fully equipped to handle production-grade workloads.

## **12\. Integrated Engine API and CLI Runtime**

The current component-level architecture must now be assembled into an operator-usable key-value runtime. This section defines the top-level `Engine` API and command-line interface (CLI) required to make ziggy practical as an embedded database and as a local administration tool.

### **12.1 Engine API Contract**

A stable programmatic API must be exposed from `ziggy.engine` with explicit lifecycle boundaries.

| API | Description |
| :---- | :---- |
| `open(config)` | Opens (or initializes) a database directory, acquires process lock, loads MANIFEST/CURRENT, replays WAL, starts background workers. |
| `close()` | Flushes and fsyncs pending durability state, stops background workers, releases file/process locks deterministically. |
| `put(key, value)` | Appends mutation to WAL, updates active MemTable, advances sequence number, returns only after configured durability policy is satisfied. |
| `get(key)` | Resolves latest visible version using MemTable + immutable tables + SSTables with Bloom-guided lookup. |
| `delete(key)` | Persists tombstone mutation with the same durability semantics as `put`. |
| `scan(range, options)` | Returns ordered iterator with snapshot semantics and bounded resources. |
| `checkpoint(path)` | Produces a consistent on-disk checkpoint suitable for backup/restore workflows. |

All API methods must return explicit Zig error sets; panics remain forbidden for runtime failures.

### **12.2 Open/Recovery/Runtime Orchestration**

`open(config)` is responsible for deterministic bootstrap and recovery sequencing:

1. Validate configuration and filesystem paths.
2. Acquire exclusive database lock (prevent multi-writer corruption).
3. Load MANIFEST via `CURRENT` pointer (or fallback recovery if needed).
4. Discover and replay WAL segments in sequence order.
5. Reconstruct active MemTable and sequence counters.
6. Start background services (flush scheduler, compaction worker, archive hook).
7. Expose ready state only after successful replay and metadata synchronization.

If any step fails, partial runtime state must be rolled back and no lock leakage is permitted.

### **12.3 CLI Scope and Operational Semantics**

A first-class CLI binary (e.g., `ziggy`) must provide:

- `ziggy open --path <db_dir>` (diagnostic open/health check)
- `ziggy put --path <db_dir> --key <k> --value <v>`
- `ziggy get --path <db_dir> --key <k>`
- `ziggy delete --path <db_dir> --key <k>`
- `ziggy scan --path <db_dir> --prefix <p>` or range flags
- `ziggy stats --path <db_dir>` (WAL size, level sizes, compaction counters)
- `ziggy doctor --path <db_dir>` (integrity checks for WAL/MANIFEST continuity)

CLI output requirements:

- Human-readable default mode
- Optional machine-readable mode (`--json`) for scripts
- Non-zero exit code on all failures
- Error messages must be explicit and actionable

### **12.4 Consistency and Durability Guarantees for CLI/API**

The integrated runtime must preserve the same durability invariants already established at module level:

- A successful `put/delete` must be recoverable after process crash.
- `get/scan` must never observe partially applied mutations.
- `scan` must provide snapshot consistency for a single command invocation.
- `close()` must be idempotent and safe to call during shutdown paths.

### **12.5 Compatibility and Migration Policy**

To support future engine evolution, disk format/version policy is required:

- Persist format/version marker in metadata.
- Reject unknown major formats with explicit error.
- Allow controlled minor upgrades when backward compatible.
- Provide offline migration command when a breaking upgrade is introduced.

### **12.6 Platform Capability Model and Support Tiers**

ziggy must expose an explicit host capability model rather than relying on implicit POSIX assumptions buried across modules. The engine must classify the active target by operating system, CPU architecture, pointer width, and storage-related runtime capabilities.

The capability surface must include at minimum:

- Operating system tag (for example `linux`, `macos`, `freebsd`)
- CPU architecture (for example `x86_64`, `aarch64`)
- Pointer width / address-size assumptions
- Availability of exclusive file locking for single-writer protection
- Availability of atomic no-replace checkpoint rename semantics
- Availability of a direct-I/O or page-cache-bypass path for WAL durability mode

Support must be grouped into operational tiers:

- **Tier 1:** officially supported and continuously verified targets
- **Tier 2:** expected to work with reduced CI coverage
- **Experimental:** compile-time allowed but not yet operationally endorsed
- **Unsupported:** explicit fail-fast at engine open

The capability model must be queryable through the runtime property API so host DBMS layers and operators can inspect platform assumptions programmatically.

### **12.7 Comptime-Specialized Decode Utilities**

ziggy must consolidate repeated little-endian integer parsing logic into comptime-specialized helpers. The goal is both correctness and performance: reduce duplicated manual parsing, allow compile-time specialization for exact integer widths, and keep hot decode paths structurally simple.

This utility layer must:

- Accept an unsigned integer type as a comptime parameter
- Reject unsupported type categories at compile time
- Validate bounds before decoding and return a typed decode error on overflow/out-of-range reads
- Be reused by WAL fragment parsing, engine WAL record parsing, and future binary metadata decoders where applicable

Recovery and parsing paths must translate decode bounds failures into corruption-style engine errors rather than panicking.

### **12.8 Platform-Aware Runtime Introspection**

The engine property surface must include platform introspection keys so integrators can branch on actual support at runtime. Representative properties include:

- `engine.platform.os`
- `engine.platform.arch`
- `engine.platform.tier`
- `engine.platform.pointer_bits`
- `engine.platform.supported`
- `engine.platform.checkpoint_atomic_noreplace`
- `engine.platform.checkpoint_portable_reservation`
- `engine.platform.direct_io_path`

These properties are part of the host-integration contract and must remain stable once published.

### **12.9 Portable Checkpoint Protocol for Non-Linux Hosts**

When Linux `renameat2(RENAME_NOREPLACE)` semantics are unavailable, ziggy must still offer a production-safe checkpoint flow instead of outright disabling checkpoint creation. The portable protocol must prioritize correctness and fail-closed behavior over invisibility during copy.

The portable checkpoint algorithm must:

1. Atomically reserve the destination directory name via directory creation.
2. Materialize a `CHECKPOINT_INCOMPLETE` marker before copying database files.
3. Copy WAL, CURRENT, MANIFEST, and SST files with file-level sync guarantees.
4. Emit a `CHECKPOINT_COMPLETE` marker after the copy finishes successfully.
5. Remove the incomplete marker only after completion is durable.
6. Cause `Engine.open()` to reject directories that still contain the incomplete marker.

This model permits visible-but-invalid intermediate directories after crashes while still preventing consumers from opening partially copied checkpoints.

### **12.10 CI/Release Automation Across Tier 1 Targets**

Production distribution must be automated through GitHub Actions. The repository must provide:

- A push/pull-request CI workflow for representative native runners
- A cross-target compile gate for Tier 1 OS/architecture pairs
- A release workflow triggered by version tags
- Per-target packaged binaries published to GitHub Releases

The minimum Tier 1 release artifact set is:

- `linux/x86_64`
- `linux/aarch64`
- `macos/x86_64`
- `macos/aarch64`

A target-specific build failure must fail the corresponding release matrix job and prevent silent partial releases.

## **13. Production Gap Closure Plan (RocksDB Parity-Oriented)**

This section formalizes the next-stage requirements needed to evolve ziggy from a module-rich engine core into a production storage engine comparable in operational behavior to RocksDB-class systems.

### **13.1 Unified LSM Runtime Integration (Must-have)**

Current runtime behavior must converge to a single canonical write/read path:

- `put/delete`: WAL append -> active MemTable insert -> immutable MemTable rotation -> SST flush -> leveled compaction.
- `get/scan`: active + immutable memtables -> L0 overlap search -> Ln single-run search.
- Engine state must no longer rely on an in-memory hash map as the source of truth after restart boundaries.

The implementation must guarantee that all query semantics are derived from LSM structures and sequence visibility rules, not side-channel caches.

### **13.2 Background Scheduling, Backpressure, and Stall Control (Must-have)**

To prevent runaway write amplification and L0 explosion under sustained ingestion, ziggy must provide:

- Dedicated flush and compaction workers with bounded queues.
- Deterministic write stall states (`normal`, `slowdown`, `stop`) driven by level pressure.
- Rate-limited compaction I/O to avoid foreground tail-latency collapse.
- Observable stall reasons and durations.

### **13.3 Crash-Consistency Hardening Matrix (Must-have)**

The durability model must explicitly define ordering guarantees for:

- WAL write + fdatasync/Direct I/O completion.
- SST temp file write + fsync + atomic rename.
- MANIFEST/CURRENT update ordering with directory fsync where required by platform semantics.

A repeatable fault-injection matrix is mandatory: process-kill, power-loss simulation points, disk-full, and partial-write corruption.

### **13.4 Observability and Runtime Introspection (Should-have)**

Production operations require first-class metrics and diagnostics:

- Counters: write/read ops, WAL bytes, flushes, compactions, tombstones dropped.
- Gauges: memtable bytes, pending compaction bytes, L0 file count, cache usage.
- Histograms: `get`, `put`, `scan`, flush, compaction latency.
- Property API/CLI for live engine internals and health snapshots.

### **13.5 Configurability and Safe Defaults (Should-have)**

A structured option surface is required to tune workload behavior safely:

- Per-engine options: write buffer size, bloom bits/key, block size, compression, level multipliers, max background jobs.
- Runtime validation for incompatible option sets.
- Versioned config schema with explicit default profiles (`dev`, `balanced`, `ingest`, `read-heavy`).

### **13.6 API Contracts and Concurrency Semantics (Should-have)**

The public API must specify strict ownership/lifetime and concurrency guarantees:

- Snapshot/iterator lifetime semantics and invalidation rules.
- Thread-safety contract (single-writer/multi-reader vs widened model).
- Checkpoint behavior under concurrent write/compaction load.
- Forward-compatible error taxonomy for automation.

### **13.7 Long-Run Verification and Compatibility (Must-have)**

Before claiming production readiness, ziggy must pass:

- Multi-hour/day soak tests with bounded memory and stable latency envelopes.
- Randomized crash/restart and corruption-recovery campaigns.
- On-disk format compatibility tests across versions.
- Deterministic replay equivalence (same WAL stream -> same visible state).

## **14. Delivery Roadmap for Production Readiness**

### **14.1 Milestone P1 (Engine Path Unification)**

- Replace map-centric read/write path with full LSM path.
- Enable immutable memtable flush and L0 ingestion in main runtime.
- Gate completion on restart/crash consistency tests.

### **14.2 Milestone P2 (Operational Control Plane)**

- Add background scheduler, backpressure states, and rate controls.
- Add property/metrics endpoints and CLI diagnostics.
- Validate under sustained write pressure.

### **14.3 Milestone P3 (Hardening & Compatibility)**

- Complete fault-injection matrix and long-run soak.
- Finalize format/version migration and compatibility suite.
- Publish production readiness checklist with pass/fail evidence.

#### **References**

1. RocksDB Wiki \- Augment Code, accessed March 4, 2026, [https://www.augmentcode.com/open-source/facebook/rocksdb](https://www.augmentcode.com/open-source/facebook/rocksdb)  
2. Transactions (Architecture Guide) \- WiredTiger, accessed March 4, 2026, [https://source.wiredtiger.com/10.0.0/arch-transaction.html](https://source.wiredtiger.com/10.0.0/arch-transaction.html)  
3. Snapshot (Architecture Guide) \- WiredTiger \- MongoDB, accessed March 4, 2026, [https://source.wiredtiger.com/10.0.0/arch-snapshot.html](https://source.wiredtiger.com/10.0.0/arch-snapshot.html)  
4. Hang on\! That's not SQLite\! Chrome, Electron and LevelDB \- CCL, accessed March 4, 2026, [https://www.cclsolutionsgroup.com/post/hang-on-thats-not-sqlite-chrome-electron-and-leveldb](https://www.cclsolutionsgroup.com/post/hang-on-thats-not-sqlite-chrome-electron-and-leveldb)  
5. LevelDB \- Riak Documentation, accessed March 4, 2026, [https://docs.riak.com/riak/kv/latest/setup/planning/backend/leveldb/index.html](https://docs.riak.com/riak/kv/latest/setup/planning/backend/leveldb/index.html)  
6. tigerbeetle/docs/ARCHITECTURE.md at main \- GitHub, accessed March 4, 2026, [https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/ARCHITECTURE.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/ARCHITECTURE.md)  
7. A Database Without Dynamic Memory Allocation \- TigerBeetle, accessed March 4, 2026, [https://tigerbeetle.com/blog/2022-10-12-a-database-without-dynamic-memory](https://tigerbeetle.com/blog/2022-10-12-a-database-without-dynamic-memory)  
8. This is the documentation for TigerBeetle: the financial transactions database designed for mission critical safety and performance to power the next 30 years of OLTP., accessed March 4, 2026, [https://docs.tigerbeetle.com/single-page/](https://docs.tigerbeetle.com/single-page/)  
9. Write Ahead Log File Format · facebook/rocksdb Wiki \- GitHub, accessed March 4, 2026, [https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format)  
10. Manual Memory Management in Zig: Allocators Demystified \- DEV Community, accessed March 4, 2026, [https://dev.to/hexshift/manual-memory-management-in-zig-allocators-demystified-46ne](https://dev.to/hexshift/manual-memory-management-in-zig-allocators-demystified-46ne)  
11. rocksdb\_wiki/Block-Cache.md at master \- GitHub, accessed March 4, 2026, [https://github.com/EighteenZi/rocksdb\_wiki/blob/master/Block-Cache.md](https://github.com/EighteenZi/rocksdb_wiki/blob/master/Block-Cache.md)  
12. SLM-DB: Single-Level Key-Value Store with Persistent Memory \- USENIX, accessed March 4, 2026, [https://www.usenix.org/system/files/fast19-kaiyrakhmet.pdf](https://www.usenix.org/system/files/fast19-kaiyrakhmet.pdf)  
13. Basic Understanding of LevelDB \- Medium, accessed March 4, 2026, [https://medium.com/@hetong07/basic-understanding-of-leveldb-5d3d8e5389c5](https://medium.com/@hetong07/basic-understanding-of-leveldb-5d3d8e5389c5)  
14. MANIFEST · facebook/rocksdb Wiki \- GitHub, accessed March 4, 2026, [https://github.com/facebook/rocksdb/wiki/MANIFEST](https://github.com/facebook/rocksdb/wiki/MANIFEST)  
15. Leveldb format \- \- Forensics Wiki, accessed March 4, 2026, [https://forensics.wiki/leveldb\_format/](https://forensics.wiki/leveldb_format/)  
16. Reviewing LevelDB: Part XVI–Recovery ain't so tough? \- Ayende @ Rahien, accessed March 4, 2026, [https://ayende.com/blog/161708/reviewing-leveldb-part-xvi-recovery-aint-so-tough](https://ayende.com/blog/161708/reviewing-leveldb-part-xvi-recovery-aint-so-tough)  
17. Manifest Design \- SlateDB, accessed March 4, 2026, [https://slatedb.io/rfcs/0001-manifest/](https://slatedb.io/rfcs/0001-manifest/)  
18. How do LSM Trees work? \- Anirudh Rowjee, accessed March 4, 2026, [https://rowjee.com/blog/lsmtrees](https://rowjee.com/blog/lsmtrees)  
19. Write Ahead Log (WAL) · facebook/rocksdb Wiki \- GitHub, accessed March 4, 2026, [https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-%28WAL%29](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-%28WAL%29)  
20. Blog | RocksDB, accessed March 4, 2026, [https://rocksdb.org/blog/](https://rocksdb.org/blog/)  
21. shreyas-gopalakrishna/Concurrent-Skip-list: The concurrent skip list is a multithreaded implementation of the skip list data structure where the insert, delete, get and range operations can be performed together across multiple threads. The implementation uses hand-over-hand locking to access nodes for writing and atomic variables while reading \- GitHub, accessed March 4, 2026, [https://github.com/shreyas-gopalakrishna/Concurrent-Skip-list](https://github.com/shreyas-gopalakrishna/Concurrent-Skip-list)  
22. Atomics And Concurrency \- by Zaid Humayun \- Medium, accessed March 4, 2026, [https://medium.com/@redixhumayun/atomics-and-concurrency-70b57ce50d27](https://medium.com/@redixhumayun/atomics-and-concurrency-70b57ce50d27)  
23. Zig in Depth: Atomics \- YouTube, accessed March 4, 2026, [https://www.youtube.com/watch?v=grMBeLJw7DM](https://www.youtube.com/watch?v=grMBeLJw7DM)  
24. Zig Language Reference \- Documentation \- The Zig Programming Language, accessed March 4, 2026, [https://ziglang.org/documentation/master/](https://ziglang.org/documentation/master/)  
25. Understanding Atomics and Memory Ordering \- DEV Community, accessed March 4, 2026, [https://dev.to/kprotty/understanding-atomics-and-memory-ordering-2mom](https://dev.to/kprotty/understanding-atomics-and-memory-ordering-2mom)  
26. Understanding Atomics and Memory Ordering : r/Zig \- Reddit, accessed March 4, 2026, [https://www.reddit.com/r/Zig/comments/mn5m9u/understanding\_atomics\_and\_memory\_ordering/](https://www.reddit.com/r/Zig/comments/mn5m9u/understanding_atomics_and_memory_ordering/)  
27. Breaking Down Memory Walls: Adaptive Memory Management in LSM-based Storage Systems \- VLDB Endowment, accessed March 4, 2026, [https://vldb.org/pvldb/vol14/p241-luo.pdf](https://vldb.org/pvldb/vol14/p241-luo.pdf)  
28. SSTables : The secret sauce that behind Cassandra's write performance.. | by Abhinav Vinci, accessed March 4, 2026, [https://medium.com/@vinciabhinav7/cassandra-internals-sstables-the-secret-sauce-that-makes-cassandra-super-fast-3d5badac8eaf](https://medium.com/@vinciabhinav7/cassandra-internals-sstables-the-secret-sauce-that-makes-cassandra-super-fast-3d5badac8eaf)  
29. Page and Extent Architecture Guide \- SQL Server | Microsoft Learn, accessed March 4, 2026, [https://learn.microsoft.com/en-us/sql/relational-databases/pages-and-extents-architecture-guide?view=sql-server-ver17](https://learn.microsoft.com/en-us/sql/relational-databases/pages-and-extents-architecture-guide?view=sql-server-ver17)  
30. Slotted Pages \- Data Structures for Databases: Bit Sets, accessed March 4, 2026, [https://siemens.blog/posts/database-page-layout/](https://siemens.blog/posts/database-page-layout/)  
31. Lecture Notes \- 05 Database Storage (Part II), accessed March 4, 2026, [https://15445.courses.cs.cmu.edu/spring2026/notes/05-storage2.pdf](https://15445.courses.cs.cmu.edu/spring2026/notes/05-storage2.pdf)  
32. Database Storage Part 1 \- CMU 15-445/645, accessed March 4, 2026, [https://15445.courses.cs.cmu.edu/spring2024/slides/03-storage1.pdf](https://15445.courses.cs.cmu.edu/spring2024/slides/03-storage1.pdf)  
33. Data representation in memory Slotted pages Records Compression \- ETH Zürich, accessed March 4, 2026, [https://ethz.ch/content/dam/ethz/special-interest/infk/inst-cp/inst-cp-dam/education/courses/2020-fall/data-management-systems/slides/DSM-HS20-Pages\_Blocks.pdf](https://ethz.ch/content/dam/ethz/special-interest/infk/inst-cp/inst-cp-dam/education/courses/2020-fall/data-management-systems/slides/DSM-HS20-Pages_Blocks.pdf)  
34. SSTables 3.0 Data File Format | ScyllaDB Docs, accessed March 4, 2026, [https://docs.scylladb.com/manual/master/architecture/sstable/sstable3/sstables-3-data-file-format.html](https://docs.scylladb.com/manual/master/architecture/sstable/sstable3/sstables-3-data-file-format.html)  
35. Delta encoding \- Wikipedia, accessed March 4, 2026, [https://en.wikipedia.org/wiki/Delta\_encoding](https://en.wikipedia.org/wiki/Delta_encoding)  
36. SSTable \- Pratyush, accessed March 4, 2026, [https://pratyusv.github.io/blog/2022/sstable/](https://pratyusv.github.io/blog/2022/sstable/)  
37. Snapshot Isolation in SQL Server \- ADO.NET \- Microsoft Learn, accessed March 4, 2026, [https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/snapshot-isolation-in-sql-server](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/snapshot-isolation-in-sql-server)  
38. Understanding LSM Trees in 5 minutes | https://zoubingwu.com, accessed March 4, 2026, [https://zoubingwu.com/2025-05-07/understanding-lsm-trees-in-5-minutes/](https://zoubingwu.com/2025-05-07/understanding-lsm-trees-in-5-minutes/)  
39. Transaction Internals: RocksDB | Flaneur2020, accessed March 4, 2026, [https://flaneur2020.github.io/posts/2021-08-14-rocksdb-txn/](https://flaneur2020.github.io/posts/2021-08-14-rocksdb-txn/)  
40. How to Implement Serializable Snapshot Isolation for Transactions \- DEV Community, accessed March 4, 2026, [https://dev.to/justlorain/how-to-implement-serializable-snapshot-isolation-for-transactions-4j38](https://dev.to/justlorain/how-to-implement-serializable-snapshot-isolation-for-transactions-4j38)  
41. Transactions · facebook/rocksdb Wiki · GitHub, accessed March 4, 2026, [https://github.com/facebook/rocksdb/wiki/Transactions](https://github.com/facebook/rocksdb/wiki/Transactions)  
42. How to Create Deadlock Detection \- OneUptime, accessed March 4, 2026, [https://oneuptime.com/blog/post/2026-01-30-deadlock-detection/view](https://oneuptime.com/blog/post/2026-01-30-deadlock-detection/view)  
43. uwiger/locks: A scalable, deadlock-resolving resource locker \- GitHub, accessed March 4, 2026, [https://github.com/uwiger/locks](https://github.com/uwiger/locks)  
44. Hybrid Transactional/Analytical Processing Amplifies IO in LSM-trees \- IEEE Xplore, accessed March 4, 2026, [https://ieeexplore.ieee.org/iel7/6287639/6514899/09940292.pdf](https://ieeexplore.ieee.org/iel7/6287639/6514899/09940292.pdf)  
45. LSM-Trees Under (Memory) Pressure \- CS-People by full name, accessed March 4, 2026, [https://cs-people.bu.edu/mathan/publications/adms22-mun.pdf](https://cs-people.bu.edu/mathan/publications/adms22-mun.pdf)  
46. rsarai/lsm\_tree: Simple proof of concept implementation \- GitHub, accessed March 4, 2026, [https://github.com/rsarai/lsm\_tree](https://github.com/rsarai/lsm_tree)  
47. Building an LSM-Tree Storage Engine from Scratch \- DEV Community, accessed March 4, 2026, [https://dev.to/justlorain/building-an-lsm-tree-storage-engine-from-scratch-3eom](https://dev.to/justlorain/building-an-lsm-tree-storage-engine-from-scratch-3eom)  
48. Merge Iterator \- LSM in a Week, accessed March 4, 2026, [https://skyzh.github.io/mini-lsm/week1-02-merge-iterator.html](https://skyzh.github.io/mini-lsm/week1-02-merge-iterator.html)  
49. Caching algorithms (LIFO vs LRU vs CLOCK) \- YouTube, accessed March 4, 2026, [https://www.youtube.com/watch?v=ofoz6wwz2p0](https://www.youtube.com/watch?v=ofoz6wwz2p0)  
50. Clock page replacement policy vs LRU page replacement policy, is Clock more efficient?, accessed March 4, 2026, [https://cs.stackexchange.com/questions/98364/clock-page-replacement-policy-vs-lru-page-replacement-policy-is-clock-more-effi](https://cs.stackexchange.com/questions/98364/clock-page-replacement-policy-vs-lru-page-replacement-policy-is-clock-more-effi)  
51. RocksDB internals: LRU \- Small Datum, accessed March 4, 2026, [http://smalldatum.blogspot.com/2022/05/rocksdb-internals-lru.html](http://smalldatum.blogspot.com/2022/05/rocksdb-internals-lru.html)  
52. Atomic reference counting (with Zig code samples) \- RavenDB NoSQL Database, accessed March 4, 2026, [https://ravendb.net/articles/atomic-reference-counting-with-zig-code-samples](https://ravendb.net/articles/atomic-reference-counting-with-zig-code-samples)  
53. Rzig: Zig reference-counted pointers inspired by Rust's Rc and Arc \- Reddit, accessed March 4, 2026, [https://www.reddit.com/r/Zig/comments/13sft6b/rzig\_zig\_referencecounted\_pointers\_inspired\_by/](https://www.reddit.com/r/Zig/comments/13sft6b/rzig_zig_referencecounted_pointers_inspired_by/)  
54. Comparing error handling in Zig and Go \- Reddit, accessed March 4, 2026, [https://www.reddit.com/r/Zig/comments/1l7wlc5/comparing\_error\_handling\_in\_zig\_and\_go/](https://www.reddit.com/r/Zig/comments/1l7wlc5/comparing_error_handling_in_zig_and_go/)  
55. LSM-tree Managed Storage for Large-Scale Key-Value Store \- UTA, accessed March 4, 2026, [https://ranger.uta.edu/\~jiang/publication/Conferences/2017/2017-SOCC-LSM-tree%20Managed%20Storage%20for%20Large-scale%20Key-value%20Store.pdf](https://ranger.uta.edu/~jiang/publication/Conferences/2017/2017-SOCC-LSM-tree%20Managed%20Storage%20for%20Large-scale%20Key-value%20Store.pdf)  
56. Revisiting the Design of LSM-tree Based OLTP Storage Engine with Persistent Memory \- VLDB Endowment, accessed March 4, 2026, [http://vldb.org/pvldb/vol14/p1872-yan.pdf](http://vldb.org/pvldb/vol14/p1872-yan.pdf)  
57. Implement LSM Tree in Zig | Implement Data Structures in Programming Languages \- SSOJet, accessed March 4, 2026, [https://ssojet.com/data-structures/implement-lsm-tree-in-zig](https://ssojet.com/data-structures/implement-lsm-tree-in-zig)  
58. Increase Merge Efficiency in LSM Trees Through Coordinated Partitioning of Sorted Runs \- Computer Science and Engineering, accessed March 4, 2026, [https://www.cs.ucr.edu/\~vagelis/publications/LSMBigData2023.pdf](https://www.cs.ucr.edu/~vagelis/publications/LSMBigData2023.pdf)  
59. 0032: undroppable tombstones, forest fuzzer, manifest race, hash\_log, zig coercions, zig pointer hops, zig object notation, domain knowledge, built from broken, database internals, papers, accessed March 4, 2026, [https://www.scattered-thoughts.net/log/0032/](https://www.scattered-thoughts.net/log/0032/)  
60. Backup and Recovery \- CloudNativePG, accessed March 4, 2026, [https://cloudnative-pg.io/documentation/1.19/backup\_recovery/](https://cloudnative-pg.io/documentation/1.19/backup_recovery/)  
61. Understanding WAL and WAL Archiving in PostgreSQL \- Practical Guide for Large Databases \- Shridhar Khanal, accessed March 4, 2026, [https://shridharkhanal.medium.com/understanding-wal-and-wal-archiving-in-postgresql-practical-guide-for-large-databases-f075bb64de0a](https://shridharkhanal.medium.com/understanding-wal-and-wal-archiving-in-postgresql-practical-guide-for-large-databases-f075bb64de0a)  
62. 18: 25.3. Continuous Archiving and Point-in-Time Recovery (PITR) \- PostgreSQL, accessed March 4, 2026, [https://www.postgresql.org/docs/current/continuous-archiving.html](https://www.postgresql.org/docs/current/continuous-archiving.html)  
63. PostgreSQL WAL Archiving and Point-In-Time-Recovery \- Highgo Software Inc., accessed March 4, 2026, [https://www.highgo.ca/2020/10/01/postgresql-wal-archiving-and-point-in-time-recovery/](https://www.highgo.ca/2020/10/01/postgresql-wal-archiving-and-point-in-time-recovery/)  
64. Curious about best practices for memory allocation (when working with data structures in std) : r/Zig \- Reddit, accessed March 4, 2026, [https://www.reddit.com/r/Zig/comments/ufdfop/curious\_about\_best\_practices\_for\_memory/](https://www.reddit.com/r/Zig/comments/ufdfop/curious_about_best_practices_for_memory/)  
65. Zig in Depth: Memory Management \- YouTube, accessed March 4, 2026, [https://www.youtube.com/watch?v=I\_ynVBs66Oc](https://www.youtube.com/watch?v=I_ynVBs66Oc)  
66. The article pretty much says this as well, but a concise way I saw Andrew Kelley... | Hacker News, accessed March 4, 2026, [https://news.ycombinator.com/item?id=44050193](https://news.ycombinator.com/item?id=44050193)  
67. What is the best way to handle unrecoverable errors like OutOfMemory in an application? \- Ziggit Dev, accessed March 4, 2026, [https://ziggit.dev/t/what-is-the-best-way-to-handle-unrecoverable-errors-like-outofmemory-in-an-application/3009](https://ziggit.dev/t/what-is-the-best-way-to-handle-unrecoverable-errors-like-outofmemory-in-an-application/3009)  
68. Comparing error handling in Zig and Go | by Alex Pliutau \- ITNEXT, accessed March 4, 2026, [https://itnext.io/comparing-error-handling-in-zig-and-go-9f638d485f69](https://itnext.io/comparing-error-handling-in-zig-and-go-9f638d485f69)  
69. HYTRADBOI '22 — TigerBeetle's LSM-Forest \- YouTube, accessed March 4, 2026, [https://www.youtube.com/watch?v=yBBpUMR8dHw](https://www.youtube.com/watch?v=yBBpUMR8dHw)  
70. Snapshot Isolation \- Jepsen.io, accessed March 4, 2026, [https://jepsen.io/consistency/models/snapshot-isolation](https://jepsen.io/consistency/models/snapshot-isolation)

[image1]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACgAAAAYCAYAAACIhL/AAAABv0lEQVR4Xu2VTStFURSGX98MiAFlgIHfIDKRj/ADFAO5GShzJfkJSkkG/oOfYMJEMhITXWWADDCgFPK5Vnsf93jv3nfvm0sG96m3zn3W2vuuTufsA5T5f4yyCNAuqWQZS5dkU7IhaaKai3nJEssIPliEWINZNGN/d0quJU9fHfl0SK5YpmiGf5BayTtLF3qrdZNdLlhe4d9I19WTa5Wc21oSH3uSVZaMbnDGMsUQTM8w+X7JMzkmNGAVCtdxiUADcnd4i/wLws9eaEBF6yMslQGY4g55pgWm7468ugZyTMyAJ5IDloreAV3MzxAzDdN3mHKN1oWIGXAZnp6YxUoWpk+Pk4RB60LE/McUHD1tVuYVHLj6Zh3OhWst0wtHT/L2PHKBmIDp4yMoY32ImAF74OmJWezr6YPbM771aSbh6bmHp2BJDtsaLiD3ZoeIGVCPKm+PFo5YCjcwb3khdK1+rgoRM+Axvp8QedzCbLIP80zqtT64IbRvgaVFz0z9Rl/Y6DWfowm6zxjLUrAoeWBZJBUI3+EfoZtXsyyCbck6y1IyLjllGYl+499Y/gYrkjmWEfzJcAkZFgG6JXUsy5SKT7BCf4Wmd65tAAAAAElFTkSuQmCC>

[image2]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAYCAYAAADKx8xXAAAAmElEQVR4XmNgGDlgMxD/JwHDAYgThiwAFUNRBAQayGJCDBAbkQETA0TBBTRxEHgEY2wFYkYkCRAoYIBo9EcTZwPiPhgnH0kCBt4zYDoTBASAWBxdEBlg8x9BwMwA0XQGXYIQKGeAaPRGlyAEPjOQ4UwQIMt/oOAmy3+zGSAaE9DEsYIgIP7GAIm7t1AM8ucvBjKcPAoGBAAAiastbKanIo0AAAAASUVORK5CYII=>

[image3]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABcAAAAYCAYAAAARfGZ1AAABB0lEQVR4XmNgGAUkAjUgngnEvkhiJUhssgArEP8D4tlAzAfEdkD8H4hrgPgzkjqyAMggG3RBBoh4FbogKWABA8QQbAAkDvIV2QBkAD7DKQIww3vRJagBuhkQFsDwDBQVFII8BkwLbqGooBJwYcCMh8lQ/lEgPgNl7wHiI1B2AEIpAgSjC0DBYgZUw5HTejIDqpwzELMg8cHAD4gL0AWhoJQBYQAjEGsiyT0B4lVI/AwkNhycBeJ16IJQ8JcBd6SCLEW2DCuAhSsPmvhaBvxZHjlIcAKQ95iA+AMDRMN7KL0ASQ06AAUBUYaTA14C8Qp0QWoBosKbVPAHiL8wQOLiBwMNg2YUkA4AcMFEb2MEsiwAAAAASUVORK5CYII=>

[image4]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEEAAAAWCAYAAACffPEKAAABlUlEQVR4Xu2WTysFURjGXyIb4QPIwsJXUJJSdmLDl7CRBYps2IoFKaLcssbWxpaFlIUs5O4oSSiFjT/P0zmTc9+Zy0xmjtu986tfd+Z5T/fec6bzzhHJycmJQRfcgENONulcVzWN8ANuwhbYBz/hHHx2xlU1nHCvDsXkszr0AB/KBSzqQlYUxEw2Cub8Q75og3fwGNapWqZwoj8tgg864Rvc1QVfBIuwpAse6BHTi1Z1wTeL8r0QgeslI9JnVMzvzOjCfzIu4YW4LBmRLu9wTYeVxICU7xMrYnI2rnknZ8Y9vexkv9EBX+C+LvhmRAeWHYleBKLzMYl+vcaF55JbeCSe3whkGE7o0DIl4cmS4BAVcAobnPu/wO85F3M2aFK1zOAE9nRo4Z6Nao6H8EzME+OYrDiA97BVF9Im2PfNKue7utxRmeN5lJ621+zyWbKlg7S5hvXwScyEHu1nwRmjYb3dXm/b+5qiX8KT5j0bW83A7n2isgd4pbI4DCawYngV0yd4Frix2YKYrcSFSNokuxOYk5MhX3djY5DG8mycAAAAAElFTkSuQmCC>

[image5]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEEAAAAWCAYAAACffPEKAAABiUlEQVR4Xu2Xuy8FQRTGP0IntAq1QoR/QCQelYSGUi0RiShQiIZKIhQqj4SbqCl1WgqRqEVJi1CQeH4nMxOzZ/eKa19s9pf8cne/M8neObk7MxcoKSn5Ae10iw572ax3XWga6Tvdoc20l37QRfrojSs0MuEeHcLkCzosIhWYyUYhufxKkqZFB3kjE/2uCWmwQl9pty7khWvCmi5kwDzMs/2FOBdW8dUI52ZgRPqMwzx3SheyZBrhRlwGRmTDAMyz5XXJlUFUXyc2YPJTuuTlkj3TdS+LQyd9odu6kAajOrDsI7oJgs4nEb29xqGVPtAjXUiaETqjQ8scwpMV3CHKcU4bvPu4dMD8AnZ1IS1kAoc6tLwhenE8phe0DmZMUvTDNHdZF9LGvfdNKj9A9aOyjJejtNvaxoLlmnG7woQuZMU1raf3MF/kzn5WvDEaqbfZ6z17/xvcbjSkC3+dPoQnLffyh6tWunTwXzihZyq7pVcqKyxPMOuEnAVubCaLmLxK0ogkF8mSkgz5BA2/XcZD4D9MAAAAAElFTkSuQmCC>

[image6]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAoAAAAXCAYAAAAyet74AAAAp0lEQVR4XmNgGAXUBOpAPB2IBaB8YyDeAMSmcBVAwAjEl4DYCYj/A/FDIA6Cyv0G4gVQNsNqIGYCYl8GiEIlmAQQdEDFwKAGSp9AFoSCNVjEwAIgd6KLoSjkhgpIIomxQ8XykcQY2qGCyOAREH9DEwP7DqTwAwPE9I1A/ApFBRSAFM0CYmYgDgNiflRpCAAJghTKokugg0kMmO7DCmBBAMLmaHKDAQAA1WwkZEfq36MAAAAASUVORK5CYII=>

[image7]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAJMAAAAYCAYAAAD+ks8OAAAEXElEQVR4Xu2aWYgURxjHP433LZ6I6CqeMSbxAn3QRRHBW7yJICZgHgyYvJhA8EV8EcELBRVB1xPBA0UfPBAfDCqaRDEmRh8kIUZMSLzwvr8/VbXzzbfdPT0z27PdY//gz1b9q6q76+iuY5YopVwYoY2U949erN9Zv+iEPLmnjQj4ivWWNUMnJIk+2igzMJDmaDNP0NHF8qk2PMBgSiyJfviQFFvHxdooglzPkis9tvzD6qBNwU5Wf20mENdBGBTbWX1ZA1nHWF+y9rC2sv62+TT3VRzl8LWbwNrPemP9n1kHWGds3Ise5D9lzmJdt+GTFHydWDGf9a82mR/INA46APowOzlxoINusLbYOOr0MesvVj8bd3h9FeqxKkV8t/2LvO5FxEB8aMMuLYjHrMnaZH5lfcY6a+O5rhMb8KCdtSkYTPEeTE9zyHGVTD0mCs+xi/W9iHt1HgaKBmtMDAjHc1aFiHtdR9KbvPPAe83qqRNqmyGs9toskC7kXRlJ3AdTWFw917JOyQQyaY1sGO37v0hzXNQGmWlxqYjLtlzF2izifqBMMw8PYMYYKRMkmKM3strYOB78EGtYdQ5/RpO5yQvWSxuuzMqRwfcBFDvIXC+IuAymrmQ65xsyU06+uA6ayjrNasdqrNLAZTJ5qoTXkbzXlCjX0Iab2rhMa0BmPRoE8q0Ucdz7mg1fItPHazLJBjTAFdYYMhf4kzXdpmFwVNmwHw+0wdxk3WW1UL6sVBD4lLq534+6HkzodNx/NZk2bG7j+YC1kfwaoc2O23AT1k8ibSaZRbU8JrkgwhL5ImLt+a2IbyOzRnODzY8TZKZHx0HKfCQwDeJsbHwm2bCPVZ/MgguNIefDFdbzoxurtTYtH5EpKxW0BpIg73JtKtxgGqATSgDebNx7g/KD2ioK3KI9CjZRAfVxc+t5qlkY20rtlQLc83NtKtxgwhY6DEPzUC7+oOx2wbIAC94pwoua4VTYtBqW76iIvkdBNydKL8wFD1Mm7zqVJsGn9QNteoDrLNCmwg0mTBVhQEeHVa5nxH0xlWDRvITCrS1rG78zp9oC9QrT956goD7Sh3dbeZpHrIU2jE44QqbcJ9U5MhzVhg8ov0ybCjeYwvwEUNvgvmHrklSwIStoMI2jmgXnWQ8LQT+wbZ2kTct/ZMpjATiXzO5hb1YOf1AOp9tBuME0SCeUANx3vTbJ/GhbLuAU/Yk2w3COTAO5gYEFOeKzq3N400obigoy664fyewWw4JzkmfaVOCnAvnMpQQ7G/3yfcH6TXlJBvXDNJ43KIjtIraeCGPqwsFhXVFBNTvLgQMzfOVukfnJAX/h4UijlOB0Gs8IYf3SPTs58aBebbUZBhTEtBYnCq5MStHgINbvZQ4Eu5eCCkbM12S+PCmlB/814A6u8wL/voDBtIjVSaXVNa9YLbWZEikYA3e0GZaxZHZz0yjaQ7BCieNXs5wp6/bGAMdvQSnRM0obKSkpKcnmHYYzAQiHCaKNAAAAAElFTkSuQmCC>

[image8]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAsAAAAXCAYAAADduLXGAAAAoElEQVR4XmNgGJSAEYhV0QWxgadA/B+KiQJXGEhQDFJ4DV0QFwApjkAXxAaiGDCd0ATE/mhiYHCTAaGYC4jvAzEfEH+Dq0ACIIW3gVgQiDdCxX5CxTEASHAnEM9El0AHMxgQJsyGslUQ0qgAPTJA7INQdj6SOBiAJKeh8VuQ2HDACRUQRRL7CMQbgLgHiA2RxMHAE10ACDyAmANdcBTAAACQdCSKrBERiwAAAABJRU5ErkJggg==>

[image9]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAwAAAAXCAYAAAA/ZK6/AAAAlUlEQVR4XmNgGAWDCegC8Twg5obyeYG4AYgnADETVAwO2IF4KxBHA/F/IG4G4gVQuXqoGArYC6VhGhqR5EA2YWgohdLXGDAls7GIwQFIoh2L2GU0MTCQYIBIgpwAA3xQMQUofypCCsJBtxpZrBqIlZDkGP4C8VdkASAoYIBo0AfiS2hyDBZAzIouyACJHwN0wVFACAAA3qgdBAlcrcAAAAAASUVORK5CYII=>

[image10]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABEAAAAYCAYAAAAcYhYyAAAAzklEQVR4Xu2QsQqBYRSGj2SSG8BoYnEBymIwKZtRLkGySJJLMCmDW5FyAyarndWieN/vPz+n4xvM+p968nvO1+f8RDIyfqcJN7Co30twDscwlx4CU7iDFdMCBXiAPfiEK0kOkoW2OjzDPCxrq+qZwF4/h5IMl59R2IjtYhphm9jAtQl/iUPLKNK4DVvD9QAHR9e4gb+Em/r2hoNupKWva9vVtcBA4reztSOto893OzjJ9yWx/6Nl2gzWzEwecG0D2MKba4Svwov6fpDxt7wA+DAuJEA6mfoAAAAASUVORK5CYII=>

[image11]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABIAAAAYCAYAAAD3Va0xAAAA0klEQVR4XmNgGAWkgtlA/AmI/yPhVygqGBi+IMmBsDeqNCqAKcIGmoD4PLogNsDIADHkFroEEFwGYl90QVwgmwFiUDiSGBMQ/wNiLiQxguAlA6q3DIH4KRKfaIAcPtOg7GMIaeIBSOMFBojLtKB8XAGPE8DC5w+S2BKoWD6SGEHwmgG77SS7CpeGtwwQcUV0CWyAmQGi+DS6BBCoMkDk3qNLYAP9DBDFoegSUABzrSC6BAwsY4Dkr3dQ/JUBkvhgQIYB4hJQWnrMAFF7D0l+FIwCALDWPUOqr0VdAAAAAElFTkSuQmCC>

[image12]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAGEAAAAYCAYAAADqK5OqAAAEFUlEQVR4Xu2YW8hVVRDHJ0spIS8lat4CQaEgER80etGKQgsMxOhBURIS6yUpENG3CI2ehBJ8LUIyMUXpIviQBhkUIoJ4C4WuloWVloqXmr+z5nzz/V1r761853DA84Ph7PWf2WutvfY+a2ZvkR49erSHp1m4zblL7T4Wb4YH1Tapvas2jHw5VqitZrENTGahy/lT7V4W69ig9p/aktSepPab2sVWxI1MVPuZxcAZsT7d/lZbEPyvkv9U8Dl/SJ+/0/yqdln6xh/U3y3rg8+vL9J4zugYwXvZkbiido3FBM67m0Xic7G4CexQhor5prCDwAPR+IIGmOlq+8TG300+pzS3dWrfspgDHZxkMfCEWMyTpD+mdom0HP6UMPPUDrNYYLTk++gE28X2+NJ13KH2DYsBnIOYIj9KvuOI/1O2ko6/aZNckJv8LrXXSKsCSY776BQ+7sZ0/ErwAeTEZ0iLXFV7k0VntlinX5DOjBSLQ6KJQLuHNOZhsThcgPOP2rjQboLPIceLaj+pvSf2j8nxqNpmsRwEPhTbYoe3IsrE6849UFU5EaDIwZaexRNO3Z6+WCzuYNCQ9XkyObaIxY1SeyAdNzmPyd2EIUmbG7QLau+ENvhXbVs6xlOLcwaLPQzLPajANLXXQ/uU2PnIUQ7Pi8G2XYxpuiDHxeJwAc7jSavDx1iptlNtT2rjxt4MuZuAh+gIab5344YDbAN8HtqNkqXysdqdoe3bIqo+56twnGOs3DiH63iiyzqJXBy2ANZy+LlPpTYuKNdfHXwTsA2i/VzQHOjH0jHyGI+FNv4xTeBzQZz/y9J3bVXk+mktRt1kForFcfm6NOlVPCIW8xbpeO+APoL0Kvgm4F0G7dwCxEW6Px3jF3iR8Wxq18F1P3herI8PxN6jmlBcqzjZEqUYJLqcHtkhFsNvjTOT/h3pVfBNQO2ONv6RDM/599Q+m35fCL4qZki5+vMx6tYAjJGKuL+kwql8L+ZHEmN4UXJUTbLKl8MvJNbbaKPUZaCvScd4CUR1cit8Jpb8c7wvNg7vEDlqH1g4D7EolniQ+KrAuaVJeuVSGvyomI/fPUo8JBaPKsuZn7T4T0OS5C0WMQfUvlT7VKx6wueWKqaKnYffEvDPYTEDxiutQwv/u34tdgE4ntUvIg/iYvnmnBerrfHN55zYzfTvUQBv2b+o/SA2drGGTiAetTjiT4v16+BTiM8f9kbwAVRL8dtPNNyYHChdff7ICfv7u1tgp2gCxsc3prawSmzBuxks9mwWlfHS4OkcINo+DgbA09atYH6lKqzti6Oslfx2P6DgbfUEi12El9iLgoZEiS0Cny7aTSdu9HXeVnuJxS5jmdpHap+IPZ2dAHmvybepAWMpC7c5eKOvq8B69Ogh/wNAZysdVhXL/QAAAABJRU5ErkJggg==>

[image13]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAB4AAAAXCAYAAAAcP/9qAAABIElEQVR4Xu2VwUoCURSGT+DGTST4Jq3busl1L9BGi6KgbU+g4NKNW3HhI4hLIVwphD5AbaJVBaVB6H+4M3LnnznmKLkwP/iR853L/RnEUWTHqCFTZILUkVx0/bfMkCLLbaDFqbhDyiw97pF35BM5p13IiaxY3EK+xR3WXETXC0ZIx5sfkZ43h3SRB5a/YRUfSvJTqDtKcKfk2jTHsIoHYhc3EhzTZMFYxeHXwLAv0Ky8IRlyMTYp1t/uj+c0On8F+6Xo4UuWslrxRuglVyzFLrB8avSSa5ZiF1g+NXrJDUvwIckF6sYs10EvumUJzsQuPmaZlry4i6q8CNBdyZsrgVsbfau8Is/IU/D5Iu416pMVV9RHhuL+9g4iJ/bs+bfMAeguWp36DU0LAAAAAElFTkSuQmCC>

[image14]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA0AAAAYCAYAAAAh8HdUAAAAjklEQVR4XmNgGP5gHhB/AuL/SPgjEPchK8IFYBqIBowMEA1n0SXwgWwGiCYvdAl84CUDiU4DAZL9AwIgDSfQBfEBQv5xQhcAgdcM+J0GijMMgM8/NUDsiC7IzADRcBFdAghkGXAY1s8AkQhEE58BFb+ALLgYiH8B8V8g/gdVAMMg/h8g/g7EMjANo4DuAAC9SCmctvS58wAAAABJRU5ErkJggg==>

[image15]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAC8AAAAYCAYAAABqWKS5AAABH0lEQVR4XmNgGAWjYGSDeUD8CYj/I+GPQNyHrGgAwGIg1kQXxAVgDh9IcASI/zEg3KKFKo0dMDJAFJ9FlyATbEUXIBEYMZDg+GwGiGIvdAkyAV0d/5KBukmGro6ndnqnu+NPoAtSAKjleG10CXRAKL07oQsgARMc+CgWMRBmgmgjCGCO10WXQAevGfAnGVCZjwv44cBnsIiBMDtEG0EAc7weugQ6wJfea4DYEV2QCECtZGOALoEMmBkgii6iSwCBLANuTxEC1HK8IboEMuhngCgKRBOfARW/gCZOLKDU8aD8B7LfB10CBEDthl9A/JcBtToGYRD/DxB/B2IZmAYSAbmOf8UAqXOeAPFjKA0Se4isiNaAXMePglEwCkbBIAEAUaBQup5YREIAAAAASUVORK5CYII=>

[image16]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEMAAAAYCAYAAAChg0BHAAABlklEQVR4Xu2XO0oEQRRFnwaimYmgO1DQTAwNDMRIIwOXICaGLkADV2AkLkMXoKCggZEIioKIqPgHBf/3UV04c6nuqa6paQapAweG+/o3r/vV9IgkEolEe3EDv+BPpn6+rduiegbhNYdVsSSmEbNcqJBJMTfB3hS9SU7GOIjMnZgLiMEOBwEUNqMLnsNt2FFfioK9GzHY5SCAwmZYOuEhPIM9VGsGPfkeh4HscxCAVzNq2YTPsJ8LJZkXc/IpLgRywEEApZth2YCfcIQLnuiqHWtElFjNaOoXbUXMQca50IBG68UABxk6pqMOjxyZ1Re9Hl3Ug1kQc5A5LhSgi7Hukzfna7Cbw4xeOO3wxJFZfdFruufQh2UxO09wwYNFMfvOcCGj6InJI9aYPHJYxDr8gENcKIGeMO8LH8NVDj2I1YwnDl1swQfYx4UAXOuFzrZeCOe+xGrGG4cWnW2d61PJn+EyvIp5qr7lryFW/W/yLoEzK+HNGBZzzkt4kXkFX2o3UvR1vBVvnq0gtBn/En1LTiQSiUQ78QvV1miYSAm5HwAAAABJRU5ErkJggg==>
