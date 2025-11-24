Example 6 (large experiment with space sharing on computing nodes):

- AleaNG is used to run a very long experiment (773151 jobs). The duration of the experiment is 365 days (simulated arrival time period). AleaNG runs 1 workload where jobs do not necessarily allocate whole nodes. As a result, one or more jobs may run side-by-side on a single computing node (multi-tenant mode). It uses Aggressive and EASY backfilling with preemption and checkpointing to speed-up urgent workloads.
-- workloads: Example6-load_85%-urgent_5%-instance_0.swf

- Uses multi-node jobs with possible space-sharing on a single node.