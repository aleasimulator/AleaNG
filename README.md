# AleaNG
### Job Scheduling Simulator for Batch Systems
This tool emulates the behavior of classical batch scheduling system such as PBS or Slurm resource managers.
It allows you to simulate and visualize the behavior of computing clusters.

#### Main Features
The simulator uses SWF workload format. It provides several implementation of jobs scheduling algorithms such as FCFS, Aggressive backfilling or EASY backfilling.

The data sets in SWF format are available at https://jsspp.org/workload/ and http://www.cs.huji.ac.il/labs/parallel/workload/logs.html. Sample data set is provided within the distribution (see ./data-set directory) but only serve for demonstration purposes. 

##### Software licence:
This software is provided as is, free of charge under the terms of the LGPL licence. It uses jFreeChart library.

##### Important
When using AleaNG in your paper or presentation, please use the following citations as an acknowledgement. Thank you!
- Dalibor Klusáček. Fair-Sharing Simulator for Batch Computing Systems. In proceedings of the 15th International Conference on Parallel Processing & Applied Mathematics (PPAM 2024), Springer, 2024.
