Example 5 (space sharing on computing nodes):

- AleaNG runs 1 workload (14 days, 34408 jobs) where jobs do not necessarily allocate whole nodes. As a result, one or more jobs may run side-by-side on a single computing node (multi-tenant mode). It uses no preemption and/or checkpointing.
-- workloads: Example5-load_85%-urgent_10%-instance_0.swf

- Uses multi-node jobs with possible space-sharing on a single node.
- All three algorithms are tested (Strict ordering, Aggressive Backfill, EASY Backfill), and charts are shown (showing EASY beats all other schedulers)


