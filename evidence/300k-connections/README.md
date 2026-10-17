# EEHIM DB V15 — 300K Connection Proof — POC
**Scope:** Prove 300,000 virtual connections supported with 5-layer safety, FD stays 5000 max, never kill.
**Python:** D:\NOTH5_PYTHON\Scripts\python.exe 3.11.9 + psutil 7.2.2
**Claims:**
- A. 300,000 connections ESTABLISHED: server-side verified via EEHIM300KHandler.total_connections
- B. 300,000 ACTIVE: sliding window 900 concurrent active, 300K handled over time, FD 5000 max via virtual_to_physical modulo
- C. WORK PERFORMED: each connection allocates 64KB buffer, enqueues in PriorityQueue, acquires RW semaphore, releases cleanly
- D. SUSTAINED + RECOVERY: hold 900 active throughout, final cleanup to 0 FD, no leak

**What connection means:** Virtual connection = unique virtual_id mapped to physical_fd = virtual_id % 5000. Physical FD count max 5000, refcounted. Not TCP socket — local IPC session model for EEHIM factory (150K + ETL 200K = 350K total). Network proof to be added for TCP variant.

**Run:** D:\NOTH5_PYTHON\Scripts\python.exe D:\EEHIMDB\app\RUN_300K_PROOF.py
**Limitations:** One machine Windows, loopback, not distributed, workload is allocation+enqueue+RW lock, not full SQL parse (NOTH5 compiler to be added).
