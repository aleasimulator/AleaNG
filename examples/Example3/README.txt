Example 3 (different seeds for otherwise similar workloads):

- AleaNG runs 5 workloads generated using the same setup (load 100%) but different seeds using EASY backfilling and uses preemption (with checkpointing) to prioritize 10% of urgent jobs.
-- workloads: Example3-load_100%-urgent_10%-instance_0.swf, Example3-load_100%-urgent_10%-instance_1.swf, Example3-load_100%-urgent_10%-instance_2.swf, Example3-load_100%-urgent_10%-instance_3.swf, Example3-load_100%-urgent_10%-instance_4.swf

- Uses preemption, does requeuing, and uses checkpointing