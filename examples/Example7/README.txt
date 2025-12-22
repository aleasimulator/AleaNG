Example 7 (large supercomputer-like system with exclusive node allocation on computing nodes):

- AleaNG is used to simulate 5 days of supercomputer-like workload (very large jobs, exclusive node allocation). 
  The system is exascale-like, featuring over 1 million CPU cores and more than 9,400 computing nodes. 
  AleaNG runs 1 workload and uses Strict Ordering, Aggressive backfilling, and EASY backfilling without preemption and checkpointing. 
  While all jobs by default allocate whole node(s) (via "enforce_exclusive_node_allocation_if_requested = true"), some jobs actually require less than the whole node. 
  By setting "enforce_exclusive_node_allocation_if_requested = false", you can see the difference when space-sharing allocation is enabled.
-- workloads: Example7-5days-exclusive_100%-load_90%-urgent_5%-instance_0.swf

- Simulation uses multi-node jobs, 5% of the workload is depicted as "urgent" (but no additional policy is applied here to prioritize it besides the priority queue).
- 7 users are simulated, their node requirements and CPU consumption are not equal. Users have the following relative CPU-hour demands: 
	urgent_tiny_U1:		 5%
	normal_large_U1:	16%
	normal_small_U1:	14%
	normal_small_U2:	12%
	normal_small_U3:	10%
	normal_tiny_U1:		 5%
	normal_tiny_U2:		38%
