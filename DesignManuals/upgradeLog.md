1. Coordinator.go: After checkpoint reset(). WAL replay duplicates issue

2. parallel merge is not correct globally for momentum, for: 
return MergeResults(job, parts)
but for now:
parallel execution uses chunk-local computation,
and global consistency requires boundary-aware merge (future work)
(by now, lab4 completed)

3. (not implemented) true scaling, where parallel coordinator features are used

4. (not tested) checksum

5. (not implemented) minimal replay engine and python strategy plug-in design