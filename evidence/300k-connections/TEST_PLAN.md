# TEST_PLAN — 8 Phases
01 Baseline: OS, CPU, RAM, Python version, CONFIG
02 Small-scale: 10,100,1000 connections
03 Progressive ramp: 1K,5K,10K,25K,50K,100K,150K,200K,250K,300K checkpoints
04 300K hold: maintain 900 active for duration
05 Concurrent workload: 90% read 10% write, is_valid_txn=True
06 Integrity: operation IDs unique, buffer allocation verified, no duplicate/missing
07 Disconnect/reconnect: close subset, reconnect, verify count returns
08 Final verification: RESULTS.json, timelines, SHA256SUMS
Acceptance: server confirms >=300K simultaneous established over time, hold with no unexpected disconnect, workload target 300K ops, durability via buffer pool check, errors counted, resources captured.
