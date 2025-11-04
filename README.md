# AleaNG

## Job Scheduling Simulator for Batch Systems

This tool emulates the behavior of classical batch scheduling system such as PBS or Slurm resource managers.
It allows you to simulate and visualize the behavior of computing clusters.

### Main Features

* The simulator uses SWF workload format. It provides several implementations of job scheduling algorithms such as FCFS, Aggressive backfilling, or EASY backfilling.
* The data sets in SWF format are available at https://jsspp.org/workload/ and http://www.cs.huji.ac.il/labs/parallel/workload/logs.html. 
* Sample data sets are provided within the distribution (see `./data-set/` and `./example/` directories) but only serve for demonstration purposes.
* The simulator supports both textual (`csv`) and graphical outputs (`png` and `pdf`).
* Simulation results are stored in `./results/` directory 

### Documentation
Documentation can be found in the https://github.com/aleasimulator/AleaNG/blob/master/readme.pdf file. A **quick start guide is shown below**:

## Compiling and Running AleaNG
AleaNG is distributed as **Netbeans project**, but can be easily compiled using just standard Java (JDK) package commands. 

### Compilation on Linux systems
On Linux-like systems, extract the downloaded zip folder and run the following commands:
```
mkdir -p out
javac -encoding UTF-8 -cp "lib/simjava.jar:lib/gridsim.jar:lib/*" -d out $(find src -name "*.java")
```
The `mkdir -p out` will create `./out` folder for the compiled bytecode. Next, `javac...` command will compile all java classes of AleaNG using the modified `simjava.jar` and `gridsim.jar` libraries (**their ordering is important, so keep it this way**).

### Compilation on Windows systems
On Linux-like systems, extract the downloaded zip folder and run following commands (in **PowerShell**: `powershell.exe`):
```
mkdir out
$files = Get-ChildItem -Recurse -Filter *.java src | ForEach-Object { $_.FullName }
javac -d out -cp "lib\simjava.jar;lib\gridsim.jar;lib\*;." $files
```
It is important to **run these commands in PowerShell**, since the standard `cmd.exe` uses a different syntax. 

### Running AleaNG on Linux systems
On Linux-like systems, navigate to the folder where you have AleaNG (and the newly created `./out` folder) and execute following command (i.e., call java with the correct classpath and your main class `ExperimentSetup`):
```
java -cp "out:lib/simjava.jar:lib/gridsim.jar:lib/*" xklusac.environment.ExperimentSetup
```
### Running AleaNG on Windows systems
To run your program, you just need to call java with the correct classpath and your main class name (`ExperimentSetup`).
```
java -cp "out;lib\simjava.jar;lib\gridsim.jar;lib\*" xklusac.environment.ExperimentSetup
```
### Memory Requirements
If you plan to run large simulations, make sure to specify enough Java heap space for the JVM. To do so, just add, e.g., ` -Xms1024m -Xmx4096m` to your `java ...` command.
Example for Linux system:
```
java -Xms1024m -Xmx4096m -cp "out:lib/simjava.jar:lib/gridsim.jar:lib/*" xklusac.environment.ExperimentSetup
```

### Simulation Start
If sucessfull, java command shown above will start the AleaNG simulator. AleaNG will read `configuration.properties` file to setup the simulation. By default, it will read **Example 1** stored in the `./examples/Example1/` folder. You should see something like this in the output:
```
-----------------------------------
|    Alea NG (Next Generation)    |
-----------------------------------
Working directory: /home/klusacek/AleaNG-compile
result root: results/2025-11-04-10-58-06
Initialising SimJava2 modified by Dalibor Klusacek (xklusac@fi.muni.cz).
--------------------------------------
Starting Queue Loader ...
...
--------------------------------------
Creating cluster cl_dedicated with 4 nodes. Each node has 1 CPUs and 4 GPUs.
Creating cluster cl_normal_1 with 64 nodes. Each node has 1 CPUs and 4 GPUs.
--------------------------------------
...
Starting simulation using Alea NG (Next Generation)
Starting GridSim version 5.0
Entities started.
...
Start time of workload is UnixStartTime: 1735689600, [01:00 01-01-2025]

================== !!! WARNING !!! ==================
AleaNG will read all 1943 jobs from workload file.
(Shortening the number specified in config: 10000 jobs)
================== !!! WARNING !!! ==================

>>> 10 arrived, in queue/schedule 4 jobs, requiring 61 CPUs, held jobs 0 running 6 jobs, free CPUs 3, free RAM 17408 GB, free GPUs 272, tried jobs 0, Day: 01-01-2025 [01:00] SimClock: 19
...
```

## Simulation Setup
AleaNG will read `configuration.properties` file to setup the simulation. By default, it reads **Example 1** stored in the `./examples/Example1/` folder. **If you want to try Example 2, 3 or Example 4**, all you need to do is rewrite the `configuration.properties` file with the one provided in each `./examples/ExampleXY/` directory.

Once more familiar, you can **edit the configuration file**, changing the algorithms, workloads and simulation setup according to your needs. Each parameter is explained in the `configuration.properties` file, which looks like this:
```ini
#--------------------------------------
# DATA_SETS (workloads). Use a comma to separate two or more different workloads
data_sets=Example1-load_50%-urgent_10%-instance_0.swf, Example1-load_75%-urgent_10%-instance_0.swf, Example1-load_100%-urgent_10%-instance_0.swf
# Data set directory
data_set_dir=/examples/Example1/
# number of gridlets (jobs) to be expected/simulated from the workload file (specify for each data set written above)
total_gridlet=10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000

#--------------------------------------
# SCHEDULING ALGORITHMS
# 0 = fair_Strict_Ordering (FCFS-like, no backfill)
# 1 = fair_Aggressive_Backfilling (no reservation)
# 2 = fair_EASY_Backfilling
algorithms = 1

#--------------------------------------
# CHART SETUP (how often is the state of the system sampled)
# how often the results are sampled for charts
sample_tick=600
# if true, charts are shown at the end of the experiment
draw_chart=true
...
```

##### Software licence:

This software is provided as is, free of charge under the terms of the LGPL licence. It uses jFreeChart and itextpdf libraries to generate charts. AleaNG is an extension of more general toolkits GridSim and SimJava.

##### Important

When using AleaNG in your paper or presentation, please use the following citation as an acknowledgement. 
* Dalibor Klusáček and Václav Chlumský. "*Fair-sharing simulator: Toward fair scheduling in batch computing systems*". In The International Journal of High Performance Computing Applications, Sage, 2025. https://doi.org/10.1177/10943420251385673

(To acknowledge GridSim and SimJava, please consult their project webpages for proper references: https://clouds.cis.unimelb.edu.au/gridsim/ and https://www.icsa.inf.ed.ac.uk/research/groups/hase/simjava/). Thank you!


