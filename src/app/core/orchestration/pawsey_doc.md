 4


Setonix uses the Slurm workload manager to schedule user programs for execution. To learn the generalities of using Slurm to schedule programs in supercomputers, visit the Job Scheduling page. In addition, please read the following subsections discuss the peculiarities of running jobs on Setonix, together with the Example Slurm Batch Scripts for Setonix on CPU Compute Nodes and Example Slurm Batch Scripts for Setonix on GPU Compute Nodes and 
Example Slurm Batch Scripts for Setonix-Q on GH200 Compute Nodes .

Important

It is highly recommended that you specify values for the --nodes, --ntasks, --cpus-per-task and --time options that are optimal for the job and for the system on which it will run. Also, use --mem if the job will not use all the resources in the node: shared access; or --exclusive for allocation of all resources in the requested nodes: exclusive access.

Overview
Each compute node of Setonix share its resources by default to run multiple jobs on the node at the same time, submitted by many users from the same or different projects. We call this configuration shared access and, as mentioned, is the default for Setonix nodes. Nevertheless, users can use slurm options to override the default and explicitly request for exclusive access to the requested nodes.

Partitions
Nodes are grouped in partitions. Each partition is characterised by a particular configuration of its resources and it is intended for a particular workload or stage of the scientific workflow development. Tables below show the list of partitions present on Setonix and their available resources per node.

Submitting jobs to the GPU partitions

You will need to use a different project code for the --account/-A option. More specifically, it is your project code followed by the -gpu suffix. For instance, if your project code is project1234, then you will have to use project1234-gpu.

 Table 1. Slurm partitions for production jobs and data transfers on Setonix

Name

N. Nodes

Cores per node

Available node-RAM for jobs

GPU chiplets per node

Types of jobs supported

Max Number of Nodes per Job

Max Wall time

Max Number of Concurrent Jobs per User

Max Number of Jobs Submitted per User

work

1376

2x 64

230 GB

n/a

Supports CPU-based production jobs.

-

24h

256

1024

long

8

2x 64

230 GB

n/a

Long-running CPU-based production jobs.

1

96h

4

96

highmem

8

2x 64

980 GB

n/a

Supports CPU-based production jobs that require a large amount of memory.

1

96h

2

96

gpu

134

1x 64

230 GB

8

Supports GPU-based production jobs.

-

24h

64

1024

gpu-highmem

38

1x 64

460 GB

8

Supports GPU-based production jobs requiring large amount of host memory.

-

24h

8

256

copy

7

1x 32

115 GB

n/a

Copy of large data to and from the supercomputer's filesystems.

-

48h

4

500

askaprt

180

2x 64

230 GB

n/a

Dedicated to the ASKAP project (similar to work partition)

-

24h

8192

8192

casda

1

1x 32

115 GB

n/a

Dedicated to the CASDA project (similar to copy partition)

-

24h

30

40

mwa

10

2x 64

230 GB

n/a

Dedicated to the MWA projects (similar to work partition)

-

24h

1000

2000

mwa-asvo

10

2x 64

230 GB

n/a

Dedicated to the MWA projects (similar to work partition)

-

24h

1000

2000

mwa-gpu

10

1x 64

230 GB

8

Dedicated to the MWA projects (similar to gpu partition)

-

24h

1000

2000

mwa-asvocopy

2

1x 32

115 GB

n/a

Dedicated to the MWA projects (similar to copy partition)

-

48h

32

1000

quantum

4

4x72

857 GB

4 

Dedicated to Setonix-Q merit allocation scheme and for running quantum computing simulation and hybrid quantum-classical workflows

-

24h

8

256

Table 2. Slurm partitions for debug and development on Setonix

Name

N. Nodes

Cores per node

Available node-RAM for jobs

GPU chiplets per node

Types of jobs supported

Max Number of Nodes per Job

Max Wall time

Max Number of Concurrent Jobs per User

Max Number of Jobs Submitted per User

debug

8

2x 64

230 GB

n/a

Exclusive for development and debugging of CPU code and workflows.

4

1h

1

4

gpu-dev

10

1x 64

230 GB

8

Exclusive for development and debugging of GPU code and workflows.

2

4h

1

4

quantum

4

4x72

857

4

As GH200 noes have different CPU architecture, codes must also be developed on the GH200 nodes. We suggest running a single GPU job test. 

4

24h

8

256

Debug and Development Partitions Policy
To ensure the debug and development partitions are available for use by Pawsey researchers, they are strictly reserved for the following activities:

Code porting

Code debugging

Code development

Job script/workflow management script porting, debugging and/or development

These partitions must not be used for the following activities:

Production runs (i.e., jobs that are intended to generate final results or data for publication, reporting, or use in further analysis)

Preparatory or test runs, including but not limited to:

Warm-up/generation of initial conditions for simulations

Testing configurations, searching for optimal/stabilitiy parameters, or setting up simulations, even if the results will not be used directly.

Running simulations or experiments to determine production parameters for AI/ML model training (e.g., hyperparameter tuning, configuration testing, validation of stability under different settings).

Testing code or scripts in ways that mimic production workloads, such as large-scale simulations or model training, that are not explicitly part of the development or debugging process.

Note: This restriction applies regardless of the execution time of the jobs. For instance, jobs that involve testing for numerical stability, parameter optimization, or early-stage simulations should not be conducted on the debug/development partitions, even if the run times are under the partition's walltime limit.


Quality of Service
Each job submitted to the scheduler gets assigned a Quality of Service (QoS) level which determines the priority of the job with respect to the others in the queue. Usually, the default normal QoS applies. Users can boost the priority of their jobs up to 10% of their allocations, using the high QoS, in the following way:

$ sbatch --qos=high myscript.sh

Each project has an allocation for a number of service units (SUs) in a year, which is broken into quarters. Jobs submitted under a project will subtract SUs from the project's allocation. A project that has entirely consumed its SUs for a given quarter of the year will run its jobs in low priority mode for that time period.

Table 3. Quality of Service levels applicable to a Slurm job running on Setonix

Name

Priority Level

Description

lowest

0

Reserved for particular cases.

low

3000

Priority for jobs past the 100% allocation usage.

normal

10000

The default priority for production jobs.

high

14000

Priority boost available to all projects for a fraction (10%) of their allocation.

highest

20000

Assigned to jobs that are of critical interest (e.g. project part of the national response to an emergency).

exhausted

0

Assigned to jobs from projects that have consumed significantly more than their allocation, which are prevented from running until the quarterly reset.

Job Queue Limits
Users can check the limits on the maximum number of jobs that users can run at a time (i.e., MaxJobs) and the maximum number of jobs that can be submitted (i.e., MaxSubmitJobs) for each partition on Setonix using the command:

$ sacctmgr show associations user=$USER cluster=setonix

Additional constraints are imposed on projects that have overused their quarterly allocation.

Executing large jobs
When executing large, multinode jobs on Setonix, the use of the --exclusive option in the batch script is recommended. The addition will result in better resource utilisation within each node assigned to the job.

Subpages in this section:


Example Slurm Batch Scripts for Setonix on CPU Compute Nodes
Example Slurm Batch Scripts for Setonix on GPU Compute Nodes

Related pages
Job Scheduling and Partitions Use Policies

Connecting to a Supercomputer

Setonix User Guide

Example Slurm Batch Scripts for Setonix on CPU Compute Nodes



By Cristian Di Pietrantonio
Sept 25, 2025

Add a reaction
Legacy editor
Getting started - a simple batch script
Multithreaded jobs (OpenMP, pthreads, etc ...)
Shared access to the node
Exclusive access to the node
MPI jobs
Shared access to the node
Exclusive access to the node
Hybrid MPI and OpenMP jobs
Hyper-threading jobs
Multiple parallel job steps in a single main job
Related pages
External links
Defining a batch script that correctly requests and uses supercomputing resources is easier if you are provided with a good starting point. This page collects examples of batch scripts for the most common scenarios for jobs in CPU compute nodes, like multi-node MPI jobs.

To better understand the code presented on this page, you should be familiar with the concepts presented in Job Scheduling.

Important

It is highly recommended that you specify values for the --ntasks, --cpus-per-task, --nodes (or --ntasks-per-node) and --time options that are optimal for the job.


Also use --mem for jobs that will share node resources: shared access. Or --exclusive for allocation of all node resources for a single job: exclusive access.

Important

In principle, jobs will get better performance when running on nodes with exclusive access, so it is recommended to plan for jobs to use a number of tasks multiple of 128. (Users may also consider to use --exclusive in jobs with less that 128 cores per node, this if the core count is not modifiable but it still allows for the better performance, even with the allocation charge for the full 128 cores in the exclusive nodes.)


If users can't run their jobs with exclusive access to the compute nodes and prefer to run in shared access (either because the number of tasks of their job is not adjustable or because there is not performance advantage vs  the allocation charges for the rest of the cores in the node), it is very important to request for the minimum number of nodes that can provide the needed resources. So, if the number of tasks for their job is less than 128 they should explicitly ask for --nodes=1. If 128 < ntasks < 256 , then use --nodes=2, etc. This to reduce network traffic when communication exist among tasks in different nodes, which will then allow for better performance for all jobs running in the cluster.

Getting started - a simple batch script
The first example batch script is a very simple one. It is meant to illustrate the Slurm options that are usually used when running jobs on Setonix. In this instance, the script executes the hostname  command,  which reports the hostname of the compute node executing it.

Listing 0. A very simple batch script
1
2
3
4
5
6
7
8
9
#!/bin/bash -l
#SBATCH --account=<project>
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1840M
#SBATCH --time=00:05:00
 
srun -N 1 -n 1 -c 1 hostname
Slurm batch scripts are BASH scripts. They must start with #!/bin/bash shebang line. Furthermore, note the -l (or --login) option. It must be there for the correct system modules to be loaded properly.

Following, the #SBATCH lines specify to Slurm the computational resources we want for our job. In this example,

The --account option tells the system which allocation to charge for the compute time.
The --ntasks option specifies the maximum number of processes (task is an MPI terminology to indicate a process) your program will execute in parallel at any time. This value is used to determine the physical resources needed to accommodate the job requirements.
The --ntasks-per-node option specifies how many tasks per node you want to run. With this information, Slurm calculates the number of nodes to be reserved for your job.
The --cpus-per-task option how many CPU cores per task you need.  The total number of requested CPU cores per node is then ntasks-per-node*cpus-per-task. On Setonix there are a maximum of 128 CPU cores available when simultaneous multithreading is not used (this is the default), 256 otherwise. In this case, only 1 CPU core per task was requested because hostname  is a serial program.
The --mem option specifies how much memory to use in each node allocated for the job and needs to be indicated for proper allocation of jobs sharing node resources: shared access. In this case, only one core of the total of 128 in a node are to be utilised, so it makes sense to share the resources of the node instead of reserving the whole node resources for this single core job. We are asking for the corresponding amount of memory available for a single core (total nodeRAM/128). Note that only integer values are recognised, so we use the integer 1840M value (instead of the wrong non-integer 1.84G value). We currently recommend the use of --mem over --mem-per-cpu as for the current version of Slurm on Setonix, the indication of memory per cpu is creating some allocation problems.
The --time option sets the maximum allowable running time for your job (that is, the wall-clock limit). This job is set to get cut-off by Slurm at the 5-minute mark.
Finally, the srun command launches the hostname executable, in what is known a job step:

-N 1 (or --nodes=1) indicates to srun command to use only 1 node.
-n 1 (or --ntasks=1) indicates to srun command to only spawn a single task.
-c 1 (or --cpus-per-task=1) indicates to srun command to assign one cpu (one core) per task.
The above configuration is the typical one to run a serial job, a job executing a serial program. A serial program is one that does not use multiple processes and/or multiple threads. Note that nodes on Setonix are shared by default, so the example job will run on a single cpu allocated specifically for this job, but the rest of the node shared with other jobs.

Multithreaded jobs (OpenMP, pthreads, etc ...)
Multithreaded jobs are the ones running a program that launches multiple threads to perform a computation, with each thread being assigned a different CPU core for execution. A program may use directives (OpenMP), frameworks (pthreads), or third-party libraries to take advantage of thread parallelism.

Shared access to the node
Shared access to the compute nodes is the default for Setonix and is the recommended use, unless sharing the node is affecting the performance of your code. Even if this is the default option, users still need to ask for the required memory (--mem) and take care of some specific Slurm options to reserve cores in a more packed form and promote better resources utilisation.

Listing 1 shows an example of a single process using OpenMP to distribute the work over only 32 cores of a compute node. The --cpus-per-task option assigns a number of CPU cores to each task. A value greater than one will allow threads of a process to run in parallel.

Listing 1. A single process using 32 cores on a node for multithreaded job.
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
#!/bin/bash --login
 
# SLURM directives
#
# Here we specify to SLURM we want an OpenMP job with 32 threads
# a wall-clock time limit of one hour.
#
# Replace [your-project] with the appropriate project name
# following --account (e.g., --account=project123).
 
#SBATCH --account=[your-project]
#SBATCH --partition=work
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=58880M      #Indicate the amount of memory per node when asking for share resources
#SBATCH --time=01:00:00
 
# ---
# Load here the needed modules
 
# ---
# OpenMP settings
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK   #To define the number of threads, in this case will be 32
export OMP_PLACES=cores     #To bind threads to cores
export OMP_PROC_BIND=close  #To bind (fix) threads (allocating them as close as possible). This option works together with the "places" indicated above, then: allocates threads in closest cores.
 
# ---
# Temporal workaround for avoiding Slingshot issues on shared nodes:
export FI_CXI_DEFAULT_VNI=$(od -vAn -N4 -tu < /dev/urandom)
# Run the desired code:
srun -N $SLURM_JOB_NUM_NODES -n $SLURM_NTASKS -c $OMP_NUM_THREADS -m block:block:block ./code_omp.x
Note the use of SLURM variables to avoid the repetition of numbers used in the settings, which is prone to errors.

Also note the explicit use of -N, -n, and -c options in the srun command, from which the -c option is strictly necessary for the correct allocation of multithreaded jobs.

The --mem option is needed for correct allocation of shared access jobs and specifies how much memory to use in each node allocated for the job. In this case the corresponding amount of memory for 1/4 of the available resources of the node is requested. Note that Slurm only accepts integer values, therefore the use of 58880M (instead of the wrong non-integer value of 58.88G). We currently recommend the use of --mem over --mem-per-cpu as for the current version of Slurm on Setonix, the indication of memory per cpu is creating some allocation problems.

Note the use of the -m block:block:block option of srun. This option is not very self explanatory, but it is used to ensure that threads are packed together in contiguous cores. Furthermore, our recommendation is to use as number of threads a multiple of 8 (number of cores per chiplet of the AMD processor) for best L3 cache utilisation.

Note that OMP_PLACES and OMP_PROC_BIND variables are used to control thread affinity in OpenMP jobs (settings above are recommended, but many other options for this variables are possible which may be tested to improve performance).

Exclusive access to the node
Exclusive access to the compute nodes is NOT the default for Setonix (contrary to previous Cray systems). Therefore the use of --exclusive is needed to warranty exclusive use by a single job whenever it is needed. Also, the request for exclusive use, will help slurm to place threads and/or processes onto cores with efficient mapping. The major drawback being the charge of the full node resources on your allocation balance (even if some cores remain idle during the job), so this option needs to be use with care (also taking into account that idle resources may impede other jobs to run in the supercomputer).

Listing 2 shows an example of a single process using OpenMP to distribute the work over all the cores on a node. The --cpus-per-task option assigns a number of CPU cores to each task. A value greater than one will allow threads of a process to run in parallel.

Listing 2. A single process using all cores on a node for multithreaded job.
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
#!/bin/bash --login
 
# SLURM directives
#
# Here we specify to SLURM we want an OpenMP job with 128 threads
# a wall-clock time limit of one hour.
#
# Replace [your-project] with the appropriate project name
# following --account (e.g., --account=project123).
 
#SBATCH --account=[your-project]
#SBATCH --partition=work
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=128
#SBATCH --exclusive
#SBATCH --time=01:00:00
 
# ---
# Load here the needed modules
 
# ---
# OpenMP settings
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK   #To define the number of threads, in this case will be 128
export OMP_PLACES=cores     #To bind threads to cores
export OMP_PROC_BIND=close  #To bind (fix) threads (allocating them as close as possible). This option works together with the "places" indicated above, then: allocates threads in closest cores.
 
# ---
# Run the desired code:
srun -N $SLURM_JOB_NUM_NODES -n $SLURM_NTASKS -c $OMP_NUM_THREADS ./code_omp.x
Note the use of SLURM variables to avoid the repetition of numbers used in the settings, which is prone to errors.

Also note the explicit use of -N, -n, and -c options in the srun command, from which the -c option is strictly necessary for the correct allocation of multithreaded jobs.

The --exclusive option is used to override the default shared access to the node into exclusive access. It is still needed for proper allocation of resources even when asking for the use of the 128 cores available per node.

Note that OMP_PLACES and OMP_PROC_BIND variables are used to control thread affinity in OpenMP jobs (settings above are recommended, but many other options for this variables are possible which may be tested to improve performance).

MPI jobs
This section presents examples of batch scripts designed to run parallel and distributed computations making use of MPI. In this scenario, the use of the srun command is critical for the creation of many tasks on multiple nodes.

Important

It is highly recommended that you set the following environment variables in your batch script when running multinode jobs:

MPI Environment variables
export MPICH_OFI_STARTUP_CONNECT=1
export MPICH_OFI_VERBOSE=1
Shared access to the node
Shared access to the compute nodes is the default for Setonix and is the recommended use, unless sharing the node is affecting the performance of your code. Even if this is the default option, users still need to ask for the required memory (--mem) and take care of some specific Slurm options to reserve cores in a more packed form and promote better resources utilisation.

Listing 3 shows an example where a total of 64 MPI tasks are created from an executable named code_mpi.x. The objective is to use all the cores of only one socket of the compute node; that is, 64 tasks per node and per socket and one MPI task per core.


Listing 3. Batch script requesting 64 cores in a single socket of a compute node for a pure MPI job.
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
#!/bin/bash --login
 
# SLURM directives
#
# Here we specify to SLURM we want to execute 64 tasks
# for an MPI job that will share the rest of the node with other jobs.
# The plan is to utilise fully 1 of the two sockets available (64 cores) and
# a wall-clock time limit of 24 hours
#
# Replace [your-project] with the appropriate project name
# following --account (e.g., --account=project123)
 
#SBATCH --account=[your-project]
#SBATCH --partition=work
#SBATCH --ntasks=64
#SBATCH --ntasks-per-node=64
#SBATCH --cpus-per-task=1
#SBATCH --mem=117G
#SBATCH --time=24:00:00
 
# ---
# Load here the needed modules
 
# ---
# Note we avoid any inadvertent OpenMP threading by setting
# OMP_NUM_THREADS=1
export OMP_NUM_THREADS=1
 
# ---
# Set MPI related environment variables. (Not all need to be set)
# Main variables for multi-node jobs (activate for multinode jobs)
#export MPICH_OFI_STARTUP_CONNECT=1
#export MPICH_OFI_VERBOSE=1
#Ask MPI to provide useful runtime information (activate if debugging)
#export MPICH_ENV_DISPLAY=1
#export MPICH_MEMORY_REPORT=1
 
# ---
# Temporal workaround for avoiding Slingshot issues on shared nodes:
export FI_CXI_DEFAULT_VNI=$(od -vAn -N4 -tu < /dev/urandom)
# Run the desired code:
srun -N $SLURM_JOB_NUM_NODES -n $SLURM_NTASKS -c $OMP_NUM_THREADS -m block:block:block ./code_mpi.x
Note the use of SLURM variables to avoid the repetition of numbers used in the settings, which is prone to errors.

Also note the explicit use of -N, -n, and -c options in the srun command.

The --mem option is needed for correct allocation of shared access jobs and specifies how much memory to use on each node allocated for the job. In this case the corresponding amount of memory for 1/2 of the available resources of the node is requested. Note that Slurm only accepts integer values, therefore the use of the rounded integer 117G (instead of the wrong non-integer value of 117.5G). We currently recommend the use of --mem over --mem-per-cpu as for the current version of Slurm on Setonix, the indication of memory per cpu is creating some allocation problems.

Note the use of the -m block:block:block option of srun. This option is not very self explanatory, but it is used to ensure that MPI tasks are placed on contiguous cores. Furthermore, our recommendation is to use as number of MPI tasks a multiple of 8 (number of cores per chiplet of the AMD processor) for best L3 cache utilisation.

Temporal workaround for avoiding issues with Slingshot

Note the use of the setting of the environment variable FI_CXI_DEFAULT_VNI before each srun.
This is to avoid a current problem we have identified with multiple jobs or srun-steps running at the same time on a compute node.
Please check further explanation in: Issues with Slingshot network

Exclusive access to the node
Exclusive access to the compute nodes is NOT the default for Setonix (contrary to previous Cray systems). Therefore the use of --exclusive is needed to warranty exclusive use by a single job whenever it is needed. Also, the request for exclusive use, will help slurm to place threads and/or processes onto cores with efficient mapping. The major drawback being the charge of the full node resources on your allocation balance (even it some cores remain idle during the job), so this option needs to be use with care (also taking into account that idle resources may impede other jobs to run on the supercomputer).

Listing 4 shows an example where a total of 512 MPI tasks are created from an executable named code_mpi.x. The objective is to use all the cores of each requested node; that is, 128 tasks per node and one MPI task per core.


Listing 4. Batch script requesting 512 cores on 4 nodes for a pure MPI job.
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
#!/bin/bash --login
 
# SLURM directives
#
# Here we specify to SLURM we want 512 tasks
# distributed by 128 tasks per node (using all available cores on 4 nodes)
# a wall-clock time limit of 24 hours
#
# Replace [your-project] with the appropriate project name
# following --account (e.g., --account=pawsey00XX)
 
#SBATCH --account=[your-project]
#SBATCH --partition=work
#SBATCH --ntasks=512
#SBATCH --ntasks-per-node=128
#SBATCH --exclusive
#SBATCH --time=24:00:00
 
 
# ---
# Load here the needed modules
 
# ---
# Set MPI related environment variables. (Not all need to be set)
# Main variables for multi-node jobs (activate for multinode jobs)
export MPICH_OFI_STARTUP_CONNECT=1
export MPICH_OFI_VERBOSE=1
#Ask MPI to provide useful runtime information (activate if debugging)
#export MPICH_ENV_DISPLAY=1
#export MPICH_MEMORY_REPORT=1
 
# ---
# Run the desired code:
srun -N $SLURM_JOB_NUM_NODES -n $SLURM_NTASKS ./code_mpi.x
Note the use of SLURM variables to avoid the repetition of numbers used in the settings, which is prone to errors.

Also note the explicit use of -N and -n options in the srun command.

The --exclusive option is used to override the default shared access to the node into exclusive access. It is still needed for proper allocation of resources even when asking for the use of the 128 cores available per node.

Also note the use of MPI variables to improve performance in multinode jobs, as recommended above in this section.


There are cases where you may not be able to use all cores of a node because you are limited by the amount of memory available. Each CPU-only node has 128 cores and 256GB of memory available (~235Gb in reality, as part of the memory is used by the system). If your MPI job requires, for instance, 3.5Gb of RAM  per task (that is 448Gb of RAM per every 128 tasks) it will not fit in the available resources in a compute node. Then you may need to distribute tasks across more nodes, leaving some cores unused on each one of them. To do that, you can modify the example above to use:

#SBATCH --ntasks-per-node=64

which will allocate 64 cores per node then allowing for more memory per core. In this case, will allocate 8 nodes instead of the 4 from the first example. You still need to use --exclusive to avoid sharing the node as your job will need all the memory available and cannot be shared, even if half of the cores remain idle. And, as mentioned above, charge for the job will be as for 128 cores per node, as the node is being allocated in exclusive access.

Hybrid MPI and OpenMP jobs
This is a mixed-mode job creating a MPI task for each socket and each task spawns 64 OpenMP threads to use all the cores within the assigned socket. The job spans 2 compute nodes, so 4 MPI tasks are created.

Listing 5. Hybrid MPI and OpenMP job using 2 nodes.
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
#!/bin/bash --login
 
# SLURM directives
#
# Here we specify to SLURM we want two nodes (--nodes=2) with
# a wall-clock time limit of twenty minutes (--time=00:20:00).
#
# Replace [your-project] with the appropriate project name
# following --account (e.g., --account=project123).
 
#SBATCH --account=[your-project]
#SBATCH --ntasks=4
#SBATCH --ntasks-per-node=2
#SBATCH --ntasks-per-socket=1
#SBATCH --cpus-per-task=64
#SBATCH --exclusive
#SBATCH --time=05:00:00
 
# ---
# Load here the needed modules
 
# ---
# OpenMP settings
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK   #To define the number of threads per task, in this case will be 64
export OMP_PLACES=cores     #To bind threads to cores
export OMP_PROC_BIND=close  #To bind (fix) threads (allocating them as close as possible). This option works together with the "places" indicated above, then: allocates threads in closest cores.
 
# ---
# Set MPI related environment variables. (Not all need to be set)
# Main variables for multi-node jobs (activate for multinode jobs)
export MPICH_OFI_STARTUP_CONNECT=1
export MPICH_OFI_VERBOSE=1
#Ask MPI to provide useful runtime information (activate if debugging)
#export MPICH_ENV_DISPLAY=1
#export MPICH_MEMORY_REPORT=1
 
# ---
# Run the desired code:
srun -N $SLURM_JOB_NUM_NODES -n $SLURM_NTASKS -c $OMP_NUM_THREADS ./code_hybrid.x
Note the use of SLURM variables to avoid the repetition of numbers used in the settings, which is prone to errors.

Also note the explicit use of -N, -n, and -c options in the srun command, from which the -c option is strictly necessary for the correct allocation of multithreaded jobs.

The --exclusive option is used to override the default shared access to the node into exclusive access. It is still needed for proper allocation of resources even when asking for the use of the 128 cores available per node.

With --ntasks-per-socket=1, a maximum of 1 MPI task will be allocated per socket.

Note that OMP_PLACES and OMP_PROC_BIND variables are used to control thread affinity in OpenMP jobs (settings above are recommended, but many other options for this variables are possible which may be tested to improve performance)

Note the MPI environment variables needed for multinode jobs.

Hyper-threading jobs
All codes that have significant fraction of their compute in the form of logic should benefit from hyperthreading. Gadget can, as can codes that use oct trees, binary trees, etc. For codes dominated by FLOPs performance gets worse due to contention of arithmetic units. Hyper-threading, or hardware threading, is disabled by default. You can enable it by using the sbatch option --threads-per-core=2.

Multiple parallel job steps in a single main job
You can run multiple job steps within a job, each of which may be a parallel computation. Furthermore, job steps can be ran sequentially or in parallel themselves.

Listing 6 shows a job encompassing multiple job steps. Each job step has to terminate before the next can start its execution. For this reason, each one of them can use all the allocated resources.

Listing 6. A batch script containing multiple job steps.
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
45
#!/bin/bash --login
 
# SLURM directives
# Replace [your-project] with the appropriate project name
# following --account (e.g., --account=project123)
 
#SBATCH --account=[your-account]
#SBATCH --ntasks=64
#SBATCH --ntasks-per-node=64
#SBATCH --cpus-per-task=1
#SBATCH --mem=117Gb
#SBATCH --time=10:00:00
 
# ---
# Load here the needed modules
 
 
# ---
# Set MPI related environment variables. (Not all need to be set)
# Main variables for multi-node jobs (activate for multinode jobs)
#export MPICH_OFI_STARTUP_CONNECT=1
#export MPICH_OFI_VERBOSE=1
#Ask MPI to provide useful runtime information (activate if debugging)
#export MPICH_ENV_DISPLAY=1
#export MPICH_MEMORY_REPORT=1
 
# ---
# Each of the sruns below will block the execution of the script 
# until current parallel job completes:
 
# (Temporal workaround for avoiding Slingshot issues on shared nodes:)
export FI_CXI_DEFAULT_VNI=$(od -vAn -N4 -tu < /dev/urandom)
# First srun-step:
srun -N $SLURM_JOB_NUM_NODES -n $SLURM_NTASKS -c $SLURM_CPUS_PER_TASK -m block:block:block ./code1.x
 
 
# Rest:
export FI_CXI_DEFAULT_VNI=$(od -vAn -N4 -tu < /dev/urandom)
srun -N $SLURM_JOB_NUM_NODES -n $SLURM_NTASKS -c $SLURM_CPUS_PER_TASK -m block:block:block ./code2.x
 
export FI_CXI_DEFAULT_VNI=$(od -vAn -N4 -tu < /dev/urandom)
srun -N $SLURM_JOB_NUM_NODES -n $SLURM_NTASKS -c $SLURM_CPUS_PER_TASK -m block:block:block ./code3.x
 
export FI_CXI_DEFAULT_VNI=$(od -vAn -N4 -tu < /dev/urandom)
srun -N $SLURM_JOB_NUM_NODES -n $SLURM_NTASKS -c $SLURM_CPUS_PER_TASK -m block:block:block ./code4.x
In this case we are assuming each of the steps run an MPI job (for further explanation of the settings see the listings above for pure MPI jobs with shared access, as this job does not use the full node).

Temporal workaround for avoiding issues with Slingshot

Note the use of the setting of the environment variable FI_CXI_DEFAULT_VNI before each srun.
This is to avoid a current problem we have identified with multiple jobs or srun-steps running at the same time in a compute node.
Please check further explanation in: Issues with Slingshot network

In listing 7, the ampersand symbol (&) is used to execute each srun command in a non-blocking way, so that the batch script is able to progress and launch all of them at about the same time. The wait command prevents the batch script from exiting its execution before all the simultaneous sruns commands are completed. Note that for all job steps to run in parallel, you must allocate a fraction of all resources to each one of them using srun command options. It is useful to do so when a single job step cannot use all the allocated resources, and there might be a limit on the number of jobs you can run on a node. This example is a type of job packing; for more versatile ways of job packing check Example Workflows.



Listing 7. Running multiple job steps in parallel.
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
45
46
47
48
49
50
51
52
53
#!/bin/bash --login
 
# SLURM directives
#
# Replace [your-project] with the appropriate project name
# following --account (e.g., --account=project123)
 
 
#SBATCH --account=[your-account]
#SBATCH --ntasks=128
#SBATCH --ntasks-per-node=128
#SBACTH --cpus-per-task=1
#SBATCH --exclusive
#SBATCH --time=02:00:00
 
# ---
# Load here the needed modules
 
# ---
# Set MPI related environment variables. (Not all need to be set)
# Main variables for multi-node jobs (activate for multinode jobs)
#export MPICH_OFI_STARTUP_CONNECT=1
#export MPICH_OFI_VERBOSE=1
#Ask MPI to provide useful runtime information (activate if debugging)
#export MPICH_ENV_DISPLAY=1
#export MPICH_MEMORY_REPORT=1
 
# ---
# "&" is used to execute multiple parallel jobs simultaneously
# "wait" is used to prevent natch script from exiting before all jobs complete
 
# Four jobs steps planned to run simultaneously:
export FI_CXI_DEFAULT_VNI=$(od -vAn -N4 -tu < /dev/urandom) #Avoinding slingshot issues when sharing the node with other jobsteps
srun -N 1 -n 32 -c 1 --mem=58G --exact -m block:block:block ./code1.x &
 
export FI_CXI_DEFAULT_VNI=$(od -vAn -N4 -tu < /dev/urandom)
srun -N 1 -n 32 -c 1 --mem=58G --exact -m block:block:block ./code2.x &
 
export FI_CXI_DEFAULT_VNI=$(od -vAn -N4 -tu < /dev/urandom)
srun -N 1 -n 32 -c 1 --mem=58G --exact -m block:block:block ./code3.x &
 
export FI_CXI_DEFAULT_VNI=$(od -vAn -N4 -tu < /dev/urandom)
srun -N 1 -n 32 -c 1 --mem=58G --exact -m block:block:block ./code4.x &
 
# Two more job steps planned to run simultaneously:
export FI_CXI_DEFAULT_VNI=$(od -vAn -N4 -tu < /dev/urandom)
srun -N 1 -n 64 -c 1 --mem=117G --exact -m block:block:block ./code5.x &
 
export FI_CXI_DEFAULT_VNI=$(od -vAn -N4 -tu < /dev/urandom)
srun -N 1 -n 64 -c 1 --mem=117G --exact -m block:block:block ./code6.x &
 
#Important wait command:
wait
In this case we are assuming each of the steps run an MPI job (for further explanation of the settings see the listings above for pure MPI jobs with exclusive access).

As mentioned above, the "&" sign allows steps to be launched simultaneously, and the wait command literally keeps the job script waiting for the completion of all the sub-steps.

The --exact option indicates that each step has access only to the resources requested in each srun command.

Note the use of the --mem option indicating the needed memory for each of the srun sub-steps, and that different units for the memory are allowed but the given number should be an integer. In this case the 235Gb of memory available for calculations in compute nodes has been divided evenly among the sub-steps that are to be executed concurrently. Each job step will start if there are resources available for it. In this case, the first 4 job steps will start at the same time (but will only end around the same time if their own workload is equal). The fifth job step could only start once two of the previous jobsteps are finished. The sixth jobstep could only start once two other previous jobsteps are finished. In an ideal world, the fifth and the sixth jobsteps would run concurrently until the end of the global allocation.

Note that the settings of MPI variables for multinode jobs are not needed in this single node job.

Temporal workaround for avoiding issues with Slingshot

Note the use of the setting of the environment variable FI_CXI_DEFAULT_VNI before each srun.
This is to avoid a current problem we have identified with multiple jobs or srun-steps running at the same time in a compute node.
Please check further explanation in: Issues with Slingshot network

Plan for balanced execution times between sruns

Be very aware that the whole allocated resources will remain allocated until the last srun command finishes its execution. No partial resources are liberated for other users when an individual job step finishes. Therefore, you should plan this kind of jobs very carefully and aim for all job steps to have very similar execution times. For example, if many of the job steps finish quickly, but just one remains on execution until reaching the walltime, most of the resources will remain idle for a long time. Even if your project is still being charged for the resources that remained idle, the creation of idle allocations is a very bad practice and should be avoided at all costs.

Related pages
Job Scheduling
Example Workflows
Known Issues on Setonix
External links
Slurm Workload Manager Documenta

Example Slurm Batch Scripts for Setonix on GPU Compute Nodes



By Alexis Espinosa

51 min

Add a reaction
On this page
On this page
Node architecture
Important: GCD vs GPU and effective meaning when allocating GPU resources at Pawsey
IMPORTANT: Shared jobs may not receive optimal binding
Slurm use of GPU nodes
Project name to access the GPU nodes is different
IMPORTANT: Add "-gpu" to your project account in order to access the GPU nodes
Pawsey's way for requesting resources on GPU nodes (different to standard Slurm)
Request for the amount of "allocation-packs" required for the job
--gpu-bind=closest may NOT work for all applications
Methods to achieve optimal binding of GCDs/GPUs
Method 1: Use of srun parameters for optimal binding
Method 2: "Manual" method for optimal binding
Thanks to CSC center and Lumi staff
Terminal N. Example error message for some GPU-aware MPI
Auxiliary technique 1: Using a wrapper to select 1 different GCD (logical/Slurm GPU) for each of the tasks spawned by srun
Listing N. selectGPU_X.sh wrapper script for "manually" selecting 1 GPU per task
Auxiliary technique 2: Using a list of CPU cores to control task placement
Use of generate_CPU_BIND.sh script for generating an ordered list of CPU cores for optimal binding
Use the script generate_CPU_BIND.sh only in GPU nodes
Terminal N. Explaining the use of the script "generate_CPU_BIND.sh" from an salloc session
MPI & OpenMP settings
Use OMP_NUM_THREADS to control the threads launched per task
GPU-Aware MPI
Use this for GPU-aware MPI codes
Test code: hello_jobstep
The test code: hello_jobstep. and thanks to ORNL staff for allowing for its use at Pawsey
Compilation and basic use of hello_jobstep test code
Terminal N. Explaining the use of the "hello_jobstep" code from an salloc session (allocation and check)
Terminal N. Explaining the use of the "hello_jobstep" code from an salloc session (compiling)
Terminal N. Explaining the use the "hello_jobstep" code from an salloc session (list allocated GPUs)
Using hello_jobstep code for testing the naive approach (a not recommended practice)
Terminal N. Explaining the use the "hello_jobstep" code from an salloc session ( "not recommended" use without full srun parameters)
Using hello_jobstep code for testing optimal binding for a pure MPI job (single threaded) 1 GPU per task
Terminal N. Testing srun settings (method 1) for optimal binding for pure MPI job 1 GPU per task.
Terminal N. Testing "manual" method (method 2) for optimal binding for pure MPI job 1 GPU per task.
Using hello_jobstep code for testing optimal binding for a hybrid (MPI + several OpenMP threads), 1 GCD (logical/Slurm GPU) per MPI task
Terminal N. Testing srun settings (method 1) for optimal binding for a case with 4 CPU threads per task and 1 GPU per task
Terminal N. Testing "manual" method (method 2) for optimal binding for a case with 4 CPU threads per task and 1 GPU per task
Using hello_jobstep code for testing visibility of all allocated GPUs to each of the tasks
Terminal N. Testing options to provide visibility of all GPUs to all CPUs allocated by srun
Example scripts for: Exclusive access to the GPU nodes with optimal binding
Single Exclusive Node Multi-GPU job: 8 GCDs (logical/Slurm GPUs), each of them controlled by one MPI task
N Exclusive Nodes Multi-GPU job: 8*N GCDs (logical/Slurm GPUs), each of them controlled by one MPI task
Example scripts for: Shared access to the GPU nodes with optimal binding
Shared node 1 GPU job
Shared node 3 MPI tasks each controlling 1 GCD (logical/Slurm GPU)
Example scripts for: Hybrid jobs (multiple threads) on the CPU side
Example scripts for: Jobs where each task needs access to multiple GPUs
Exclusive nodes: all 8 GPUs in each node accessible to all 8 tasks in the node
Shared nodes: Many GPUs requested but 2 GPUs binded to each task
Example scripts for: Packing GPU jobs
Packing the execution of 8 independent instances each using 1 GCD (logical/Slurm GPU)
Related pages
Node architecture
The GPU node architecture is different from that on the CPU-only nodes. The following diagram shows the connections between the CPU and GPUs on the node, which will assist with understanding recommendations for Slurm job scripts later on this page. Note that the numbering of the cores of the CPU has a slightly different order to that of the GPUs. Each GCD can access 64GB of GPU memory. This totals to 128GB per MI250X, and 256GB per standard GPU node. 


Figure 1. GPU node architecture. Note that the GPU's shown here are equivalent to a GCD (more info about this is in the Setonix General Information).

Each GPU node have 4 MI250X GPU cards, which in turn have 2 Graphical Compute Die (GCD), which are seen as 2 logical GPUs; so each GPU node has 8 GCDs that is equivalent to 8 slurm GPUs. On the other hand, the single AMD CPU chip has 64 cores organised in 8 groups that share the same L3 cache. Each of these L3 cache groups (or chiplets) have a direct Infinity Fabric connection with just one of the GCDs, providing optimal bandwidth. Each chiplet can communicate with other GCDs, albeit at a lower bandwidth due to the additional communication hops. (In the examples explained in the rest of this document, we use the numbering of the cores and bus IDs of the GCD to identify the allocated chiplets and GCDs, and their binding.)

Important: GCD vs GPU and effective meaning when allocating GPU resources at Pawsey

A MI250x GPU card has two GCDs. Previous generations of GPUs only had 1 GCD per GPU card, so these terms could be used interchangeably. The interchangeable usage continues even though now GPUs have more than one GCD. Slurm for instance only use the GPU terminology when referring to accelerator resources, so requests such as --gres=gpu:number
       is equivalent to a request for a certain number of GCDs per node. On Setonix, the max number is 8. (Note that the "equivalent" option --gpus-per-node=number
         is not recommended as we have found some bugs with its use.)

Furthermore, Pawsey DOES NOT use standard Slurm meaning for the --gres=gpu:number parameter. The meaning of this parameter has been superseeded to represent the request for a number of "allocation-packs". The new representation has been implemented to achieve best performance. Therefore, the current allocation method uses the "allocation-pack" as the basic allocation unit and, as explained in the rest of this document, users should only request for the number of "allocation-packs" that fullfill the needs of the job. Each allocation-pack provides:

1 whole CPU chiplet (8 CPU cores)

~32 GB memory (1/8 of the total available RAM)

1 GCD (slurm GPU) directly connected to that chiplet




IMPORTANT: Shared jobs may not receive optimal binding
For jobs that only use a partial set of resources of the node (non-exclusive jobs that share the rest of the node with other jobs), the current Setonix GPU configuration may not provide perfect allocation and binding, which may impact performance depending on the amount of CPU-GPU communication. This is under active investigation, and the recommendations included in this document will serve to achieve optimal allocations in most of the cases, but is not 100% guaranteed. Therefore, if you detect that imperfect binding or the use of shared nodes (even with optimal binding) is impacting the performance of your jobs, it is recommended to use exclusive nodes where possible, noticing that the project will still be charged for the whole node even if part of the resources remain idle. Please also report the observed issues to Pawsey's helpdesk.

Each GPU node also has an attached NVMe device with up to 3500 GiB of usable storage.

Further details of the node architecture are also available on the GPU node architecture page.

Slurm use of GPU nodes
Project name to access the GPU nodes is different
IMPORTANT: Add "-gpu" to your project account in order to access the GPU nodes
The default project name will not give you access to the GPU nodes. So, in order to access the GPU nodes, users need to add the postfix "-gpu" to their project name and explicitly indicate it in the resource request options:

#SBATCH -A <projectName>-gpu

So, for example, if your project name is "rottnest0001" the setting would be:

#SBATCH -A rottnest0001-gpu

This applies for all GPU partitions (gpu, gpu-dev & gpu-highmem).




Pawsey's way for requesting resources on GPU nodes (different to standard Slurm)
The request of resources for the GPU nodes has changed dramatically. The main reason for this change has to do with Pawsey's efforts to provide a method for optimal binding of the GPUs to the CPU cores in direct physical connection for each task. For this, we decided to completely separate the options used for resource request via salloc or (#SBATCH pragmas) and the options for the use of resources during execution of the code via srun.

Request for the amount of "allocation-packs" required for the job
With a new CLI filter that Pawsey staff had put in place for the GPU nodes, the request of resources in GPU nodes should be thought as requesting a number of "allocation-packs". Each "allocation-pack" provides:

1 whole CPU chiplet (8 CPU cores)

a bit less of 32 GB memory (29.44 GB of memory, to be exact, allowing some memory for the system to operate the node) = 1/8 of the total available RAM

1 GCD directly connected to that chiplet

For that, the request of resources only needs the number of nodes (–-nodes, -N) and the number of allocation-packs per node (--gres=gpu:number). The total of allocation-packs requested results from the multiplication of these two parameters. Note that the standard Slurm meaning of the second parameter IS NOT used at Pawsey. Instead, Pawsey's CLI filter interprets this parameter as:

the number of requested "allocation-packs" per node

Note that the "equivalent" option --gpus-per-node=number (which is also interpreted as the number of "allocation-packs" per node) is not recommended as we have found some bugs with its use.

Furthermore, in the request of resources, users should not indicate any other Slurm allocation option related to memory or CPU cores. Therefore, users should not use --ntasks, --cpus-per-task, --mem, etc. in the request headers of the script ( #SBATCH directives), or in the request options given to salloc for interactive sessions. If, for some reason, the requirements for a job are indeed determined by the number of CPU cores or the amount of memory, then users should estimate the number of "allocation-packs" that cover their needs. The "allocation-pack" is the minimal unit of resources that can be managed, so that all allocation requests should be indeed multiples of this basic unit.

Pawsey also has some site specific recommendations for the use/management of resources with srun command. Users should explicitly provide a list of several parameters for the use of resources by srun. (The list of these parameters is made clear in the examples below.) Users should not assume that srun will inherit any of these parameters from the allocation request. Therefore, the real management of resources at execution time is performed by the command line options provided to srun. Note that, for the case of srun, options do have the standard Slurm meaning. 



--gpu-bind=closest may NOT work for all applications
Within the full explicit srun options for "managing resources", there are some that help to achieve optimal binding of GPUs to their directly connected chiplet on the CPU. There are two methods to achieve this optimal binding of GPUs. So, together with the full explicit srun options, the following two methods can be used:

Include these two Slurm parameters: --gpus-per-task=<number> together with --gpu-bind=closest

"Manual" optimal binding with the use of "two auxiliary techniques" (explained later in the main document).

The first method is simpler, but may still launch execution errors for some codes. "Manual" binding may be the only useful method for codes relying OpenMP or OpenACC pragma's for moving data from/to host to/from GPU and attempting to use GPU-to-GPU enabled MPI communication. An example of such a code is Slate. 




The following table provides some examples that will serve as a guide for requesting resources in the GPU nodes. Most of the examples in the table provide are for typical jobs where multiple GPUs are allocated to the job as a whole but each of the tasks spawned by srun is binded and has direct access to only 1 GPU. For applications that require multiple GPUs per task, there 3 examples (*4, *5 & *7) where tasks are binded to multiple GPUs:

Required Resources per Job

New "simplified" way of requesting resources

Total Allocated resources

Charge per hour

The use of full explicit srun options is now required
(only the 1st method for optimal binding is listed here)

1 CPU task (single CPU thread) controlling 1 GCD (Slurm GPU)

#SBATCH --nodes=1 
#SBATCH --gres=gpu:1

1 allocation-pack =
1 GPU, 8 CPU cores (1 chiplet), 29.44 GB CPU RAM

64 SU


                *1
              

export OMP_NUM_THREADS=1 
srun -N 1 -n 1 -c 8 --gres=gpu:1 --gpus-per-task=1 --gpu-bind=closest <executable>
                

              

1 CPU task (with 14 CPU threads each) all threads controlling the same 1 GCD

#SBATCH --nodes=1 
#SBATCH --gres=gpu:2

2 allocation-packs=
2 GPUs, 16 CPU cores (2 chiplets), 58.88 GB CPU RAM

128 SU


                *2
              

export OMP_NUM_THREADS=14 
srun -N 1 -n 1 -c 16 --gres=gpu:1 --gpus-per-task=1 --gpu-bind=closest <executable>
              

3 CPU tasks (single thread each), each controlling 1 GCD with GPU-aware MPI communication

#SBATCH --nodes=1 
#SBATCH --gres=gpu:3

3 allocation-packs=
3 GPUs, 24 CPU cores (3 chiplets), 88.32 GB CPU RAM

192 SU


                *3
              

export MPICH_GPU_SUPPORT_ENABLED=1
                
export OMP_NUM_THREADS=1 
srun -N 1 -n 3 -c 8 --gres=gpu:3 --gpus-per-task=1 --gpu-bind=closest <executable>
              

2 CPU tasks (single thread each), each task controlling 2 GCDs with GPU-aware MPI communication

#SBATCH --nodes=1 
#SBATCH --gres=gpu:4

4 allocation-packs=
4 GPU, 32 CPU cores (4 chiplets), 117.76 GB CPU RAM

256 SU

 *4 

export MPICH_GPU_SUPPORT_ENABLED=1
                
export OMP_NUM_THREADS=1 
srun -N 1 -n 2 -c 16 --gres=gpu:4 --gpus-per-task=2 --gpu-bind=closest <executable>
                

              

5 CPU tasks (single thread each) all threads/tasks able to see all 5 GPUs

#SBATCH --nodes=1 
#SBATCH --gres=gpu:5

5 allocation-packs=
5 GPUs, 40 CPU cores (5 chiplets), 147.2 GB CPU RAM

320 SU


                *5

export MPICH_GPU_SUPPORT_ENABLED=1
export OMP_NUM_THREADS=1 
srun -N 1 -n 5 -c 8 --gres=gpu:5 <executable>
              

8 CPU tasks (single thread each), each controlling 1 GCD with GPU-aware MPI communication

#SBATCH --nodes=1 
#SBATCH --exclusive

8 allocation-packs=
8 GPU, 64 CPU cores (8 chiplets), 235 GB CPU RAM

512 SU

 *6 

export MPICH_GPU_SUPPORT_ENABLED=1
              
export OMP_NUM_THREADS=1 
srun -N 1 -n 8 -c 8 --gres=gpu:8 --gpus-per-task=1 --gpu-bind=closest <executable>
                

            

8 CPU tasks (single thread each), each controlling 4 GCD with GPU-aware MPI communication

#SBATCH --nodes=4 
#SBATCH --exclusive

32 allocation-packs=
4 nodes, each with: 8 GPU, 64 CPU cores (8 chiplets), 235 GB CPU RAM

2048 SU

 *7 

export MPICH_GPU_SUPPORT_ENABLED=1
              
export OMP_NUM_THREADS=1 
srun -N 4 -n 8 -c 32 --gres=gpu:8 --gpus-per-task=4 --gpu-bind=closest <executable>
                

            

1 CPU task (single thread), controlling 1 GCD but avoiding other jobs to run in the same node for ideal performance test.

#SBATCH --nodes=1 
#SBATCH --exclusive

8 allocation-packs=
8 GPU, 64 CPU cores (8 chiplets), 235 GB CPU RAM

512 SU

 *8 

export OMP_NUM_THREADS=1 
srun -N 1 -n 1 -c 8 --gres=gpu:1 --gpus-per-task=1 --gpu-bind=closest <executable>
                

            




By default, each node will also have 128 GiB of NVMe storage available under the /tmp and /var/tmp directories. A larger amount of storage can be requested, up to 3400 GiB, by adding tmp:<some-number>G to the --gres option.
For example, to request 5 GPUs and 1000 GiB of NVMe storage, use the following in an sbatch script:



#SBATCH --gres=gpu:5,tmp:1000G
Notes for the request of resources:

Note that this simplified way of resource request is based on requesting a number of "allocation-packs", so that standard use of Slurm parameters for allocation should not be used for GPU resources.

The --nodes (-N) option indicates the number of nodes requested to be allocated.

The --gres=gpu:number
         option indicates the number of allocation-packs requested to be allocated per node. (The "equivalent" option --gpus-per-node=number
         is not recommended as we have found some bugs with its use.)

The --exclusive option requests all the resources from the number of requested nodes. When this option is used, there is no need for the use of --gres=gpu:number
         during allocation and, indeed, its use is not recommended in this case.

There is currently an issue with NVMe allocation with --exclusive, where only 128 GiB is made available regardless of --gres=tmp settings. If more than 128 GiB is required, one should for now request all 8 GCDs explicitly with, for example, --gres=gpu:8,tmp=3500G.

There is no maximum NVMe allocation limit enforced for non-exclusive use, but we ask that no more than 2679 GiB be requested in this circumstance so that there other jobs can share the node.

Users should not include any other Slurm allocation option that may indicate some "calculation" of required memory or CPU cores. The management of resources should only be performed after allocation via srun options.

The same simplified resource request should be used for the request of interactive sessions with salloc.

IMPORTANT: In addition to the request parameters shown in the table, users should indeed use other Slurm request parameters related to partition, walltime, job naming, output, email, etc. (Check the examples of the full Slurm batch scripts.)

Notes for the use/management of resources with srun:

Note that, for the case of srun, options do have the standard Slurm meaning.

The following options need to be explicitly provided to srun and not assumed to be inherited with some default value from the allocation request:

The --nodes (-N) option indicates the number of nodes to be used by the srun step.

The --ntasks (-n) option indicates the total number of tasks to be spawned by the srun step. By default, tasks are spawned evenly across the number of allocated nodes.

The --cpus-per-task (-c) option should be set to multiples of 8 (whole chiplets) to guarantee that srun will distribute the resources in "allocation-packs" and then "reserving" whole chiplets per srun task, even if the real number is 1 thread per task. The real number of threads is controlled with the OMP_NUM_THREADS environment variable.

The --gres=gpu:number
         option indicates the number of GPUs per node to be used by the srun step. (The "equivalent" option --gpus-per-node=number
         is not recommended as we have found some bugs with its use.)

The --gpus-per-task option indicates the number of GPUs to be binded to each task spawned by the srun step via the -n option. Note that this option neglects sharing of the assigned GPUs to a task with other tasks. (See cases *4, *5 and *7 and their notes for non-intuitive cases.)

And for optimal binding, the following should be used:

The --gpu-bind=closest indicates that the chosen GPUs to be binded to each task should be the optimal (physically closest) to the chiplet assigned to each task.

IMPORTANT: The use of --gpu-bind=closest will assign optimal binding but may still NOT work and launch execution errors for codes relying OpenMP or OpenACC pragma's for moving data from/to host to/from GPU and attempting to use GPU-to-GPU enabled MPI communication. For those cases, the use of the "manual" optimal binding (method 2) is required. Method 2 is explained later in the main document.




(*1) This is the only case where srun may work fine with default inherited option values. Nevertheless, it is a good practice to always use full explicit options of srun to indicate the resources needed for the executable. In this case, the settings explicitly "reserve" a whole chiplet (-c 8) for the srun task and control the real number of threads with the OMP_NUM_THREADS environment variable. Although the use of gres=gpu, gpus-per-task & gpu-bind is reduntant in this case, we keep them for encouraging their use, which is strictly needed in the most of cases (except case *5).

(*2) The required CPU threads per task is 14 and that is controlled with the OMP_NUM_THREADS environment variable. But still the two full chiplets (-c 16) are indicated for each srun task.

(*3) The settings explicitly "reserve" a whole chiplet (-c 8) for each srun task. This provides "one-chiplet-long" separation among each of the CPU cores to be allocated for the tasks spawned by srun (-n 3).  The real number of threads is controlled with the OMP_NUM_THREADS variable. The requirement of optimal binding of GPU to corresponding chiplet is indicated with the option --gpu-bind=closest. And, in order to allow GPU-aware MPI communication, the environment variable 
          MPICH_GPU_SUPPORT_ENABLED
         is set to 1.

(*4) Each task needs to be in direct communication with 2 GCDs. For that, each of the CPU task reserve "two-full-chiplets". IMPORTANT: The use of -c 16 "reserves" a "two-chiplets-long" separation among the two CPU cores that are to be used (one for each of the srun tasks, -n 2 ). In this way, each task will be in direct communication to the two logical GPUs in the MI250X card that has optimal connection to the chiplets reserved for each task. The real number of threads is controlled with the OMP_NUM_THREADS variable. The requirement of optimal binding of GPU to corresponding chiplet is indicated with the option --gpu-bind=closest. And, in order to allow GPU-aware MPI communication, the environment variable 
          MPICH_GPU_SUPPORT_ENABLED
         is set to 1.

(*5) Sometimes, the executable (and not the scheduler) performs all the management of GPUs, like in the case of Tensorflow distributed training, and other Machine Learning Applications. If all the management logic for the GPUs is performed by the executable, then all the available resources should be exposed to it. IMPORTANT: In this case, the --gpu-bind option should not be provided. Neither the --gpus-per-task option should be provided, as all the available GPUs are to be available to all tasks. The real number of threads is controlled with the OMP_NUM_THREADS variable. And, in order to allow GPU-aware MPI communication, the environment variable 
          MPICH_GPU_SUPPORT_ENABLED
         is set to 1. These last two settings may not be necessary for aplications like Tensorflow.

(*6) All GPUs in the node are requested, which mean all the resources available in the node via the --exclusive allocation option (there is no need to indicate the number of GPUs per node when using exclusive allocation). The use of -c 8 provides "one-chiplet-long" separation among each of the CPU cores to be allocated for the tasks spawned by srun (-n 8).  The real number of threads is controlled with the OMP_NUM_THREADS variable. The requirement of optimal binding of GPU to corresponding chiplet is indicated with the option --gpu-bind=closest. And, in order to allow GPU-aware MPI communication, the environment variable 
          MPICH_GPU_SUPPORT_ENABLED
         is set to 1.

(*7) All resources in each node are requested via the --exclusive allocation option (there is no need to indicate the number of GPUs per node when using exclusive allocation). Each task needs to be in direct communication with 4 GCDs. For that, each of the CPU task reserve "four-full-chiplets". IMPORTANT: The use of -c 32 "reserves" a "four-chiplets-long" separation among the two CPU cores that are to be used per node (8 srun tasks in total, -n 8 ). The real number of threads is controlled with the OMP_NUM_THREADS variable. The requirement of optimal binding of GPU to corresponding chiplet is indicated with the option --gpu-bind=closest. In this way, each task will be in direct communication to the closest four logical GPUs in the node with respect to the chiplets reserved for each task. And, in order to allow GPU-aware MPI communication, the environment variable 
          MPICH_GPU_SUPPORT_ENABLED
         is set to 1. The --gres=gpu:8 option assigns 8 GPUs per node to the srun step (32 GPUs in total as 4 nodes are being assigned).

(*8) All GPUs in the node are requested using the --exclusive option, but only 1 CPU chiplet - 1 GPU "unit" (or allocation-pack) is used in the srun step.

General notes:

The allocation charge is for the total of allocated resources and not for the ones that are explicitly used in the execution, so all idle resources will also be charged

Note that examples above are just for quick reference and that they do not show the use of the 2nd method for optiomal binding (which may be the only way to achieve optimal binding for some applications). So, the rest of this page will describe in detail both methods of optimal binding and also show full job script examples for their use on Setonix GPU nodes.

Methods to achieve optimal binding of GCDs/GPUs
As mentioned above and, as the node diagram in the top of the page suggests, the optimal placement of GCDs and CPU cores for each task is to have direct communication among the CPU chiplet and the GCD in use. So, according to the node diagram, tasks being executed in cores in Chiplet 0 should be using GPU 4 (Bus D1), tasks in Chiplet 1 should be using GPU 5 (Bus D6), etc.

Method 1: Use of srun parameters for optimal binding
This is the most intuitive (and simple) method for achieving optimal placement of CPUs and GPUs in each task spawned by srun. This method consists in providing the  --gpus-per-task and the --gpu-bind=closest parameters. So, for example, in a job that requires the use of 8 CPU tasks (single threaded) with 1 GPU per task, the srun command to be used is:

export MPICH_GPU_SUPPORT_ENABLED=1
export OMP_NUM_THREADS=1
srun -N 1 -n 8 -c 8 --gres=gpu:8 --gpus-per-task=1 --gpu-bind=closest myMPIExecutable

The explanation of this method will be completed in the following sections where a very useful code (hello_jobstep) will be used to confirm optimal (or sub-optimal, or incorrect) binding of GCDs (Slurm GPUs) and chiplets for srun job steps. Other examples of its use are already listed in the table in the subsection above and its use in full scripts will be provided at the end of this page.

It is important to be aware that this method works fine for most codes, but not for all. Codes suffering MPI communication errors with this methodology, should try the "manual" binding method described next.

Method 2: "Manual" method for optimal binding
Thanks to CSC center and Lumi staff
We acknowledge that the use of this method to control CPU and GCD placement was initially taken from the LUMI supercomputing documentation at CSC. From there, we have further automated parts of it for its use in shared GPU nodes. We are very thankful to LUMI staff for their collaborative support in the use and configuration of Setonix.

For codes relying OpenMP or OpenACC pragma's for moving data from/to host to/from GCD and attempting to use GPU-to-GPU (GCD-to-GCD) enabled MPI communication, the first method may fail, giving errors similar to:

Terminal N. Example error message for some GPU-aware MPI

$ srun -N 1 -n 8 -c 8 --gres=gpu:8 --gpus-per-task=1 --gpu-bind=closest ./myCode_mpiGPU.exe
GTL_DEBUG: [0] hsa_amd_ipc_memory_attach (in gtlt_hsa_ops.c at line 1544):
HSA_STATUS_ERROR_INVALID_ARGUMENT: One of the actual arguments does not meet a precondition stated in the documentation of the corresponding formal argument.
MPICH ERROR [Rank 0] [job id 339192.2] [Mon Sep  4 13:00:27 2023] [nid001004] - Abort(407515138) (rank 0 in comm 0):
Fatal error in PMPI_Waitall: Invalid count, error stack:

For these codes, the alternative is to use a "manual" method. This second method is more elaborated than the first but, as said, may be the only option for some codes.

In this "manual" method, the  --gpus-per-task and the --gpu-bind parameters (key of the first method) should NOT be provided. And, instead of those two parameters, we use two auxiliary techniques:

A wrapper script that sets a single and different value of ROCR_VISIBLE_DEVICE variable for each srun task, then assigning a single and different GCD (logical/Slurm GPU) per task.

An ordered list of CPU cores in the --cpu-bind option of srun to explicitly indicate the CPU cores where each task will be placed.

These two auxiliary techniques are applied together and work in coordination to ensure the best possible match of CPU cores and GCDs.

Auxiliary technique 1: Using a wrapper to select 1 different GCD (logical/Slurm GPU) for each of the tasks spawned by srun
This first auxiliary technique uses the following wrapper script:

Listing N. selectGPU_X.sh wrapper script for "manually" selecting 1 GPU per task

#!/bin/bash
 
export ROCR_VISIBLE_DEVICES=$SLURM_LOCALID
exec $*

(Note that the user is responsible for creating this wrapper in their working directory and set the correct permissions for it to work. The wrapper need to have execution permissions. The command: chmod 755 selectGPU_X.sh, or similar will do the job for that.)

The wrapper script defines the value of the ROCm environment variable ROCR_VISIBLE_DEVICES with the value of the Slurm environment variable SLURM_LOCALID. It then executes the rest of the parameters given to the script which are the usual execution instructions for the program intended to be executed. The SLURM_LOCALID variable has the identification number of the task within each of the nodes (not a global identification, but an identification number local to the node). Further details about the variable are available in the Slurm documentation.

The wrapper should be called first and then the executable (and its parameters, if any). For example, in a job that requires the use of 8 CPU tasks (single threaded) with 1 GCD (logical/Slurm GPU) per task, the srun command to be used is:

export MPICH_GPU_SUPPORT_ENABLED=1
export OMP_NUM_THREADS=1
CPU_BIND=$(generate_CPU_BIND.sh map_cpu)
srun -N 1 -n 8 -c 8 --gres=gpu:8 --cpu-bind=${CPU_BIND} ./selectGPU_X.sh myMPIExecutable   

The wrapper will be ran by each of the 8 tasks spawned by srun (-n 8) and will assign a different and single value to ROCR_VISIBLE_DEVICES for each of the tasks. Furthermore, the task with SLURM_LOCALID=0 will be receive GCD 0 (Bus C1) as the only visible Slurm GPU for the task. The task with SLURM_LOCALID=1 will receive GPU 1 (Bus C6), and so forth.

The definition of  CPU_BIND and its use in the --cpu-bind option of the srun command is part of the second auxiliary technique. As mentioned above, the "manual" method consist of two auxiliary techniques that need to be applied together.  So the application of a second the second technique is compulsory and is explained in the following sub-section.

Auxiliary technique 2: Using a list of CPU cores to control task placement
This second auxiliary technique uses an ordered list of CPU cores to be binded to each of the tasks spawned by srun. An example of a "hardcoded" ordered list that would bind correctly the 8 GCDs across the 4 GPU cards in a node would be:

CPU_BIND="map_cpu:49,57,17,25,0,9,33,41"

("map_cpu" is a Slurm indicator of the type of binding to be used. Please read the Slurm documentation for further details.)

According to the node diagram at the top of this page, it is clear that this list consists of 1 CPU core per chiplet. What may not be very intuitive is the ordering. But after a second look, it can be seen that the order follows the identification numbers of the GCDs (logical/Slurm GPUs) in the node, so that each of the CPU cores correspond to the chiplet that is directly connected to each of the GCDs (in order). Then, the set of commands to use for a job that requires the use of 8 CPU tasks (single threaded) with 1 GCD (logical/Slurm GPU) per task would be:

export MPICH_GPU_SUPPORT_ENABLED=1
export OMP_NUM_THREADS=1
CPU_BIND="map_cpu:49,57,17,25,0,9,33,41"
srun -N 1 -n 8 -c 8 --gres=gpu:8 --cpu-bind=${CPU_BIND} ./selectGPU_X.sh myMPIExecutable   

This provides the optimal binding in a job that requires the use of 8 CPU tasks (single threaded) with 1 GCD (logical/Slurm GPU) per task.

For jobs that are hybrid, that is, that require multiple CPU threads per task, the list needs to be modified to be a list of masks instead of CPU core IDs. The explanation of the use of this list of masks will be given in the next subsection that also describes the use of an auxiliary script to generate the lists of CPU cores or mask for general cases.

For jobs that request exclusive use of the GPU nodes, the settings described in the example so far are enough for achieving optimal binding with the "manual" method. This works because the identification numbers of all the GPUs and the CPU cores that will be assigned to the job are known before hand (as all the resources of the node are what is requested). But when the job requires a reduced amount of resources, so that the request shares the rest of the node with other jobs, the GPUs and CPU cores that are to be allocated to the job are not known before submitting the script for execution. And, therefore, a "hardcoded" list of CPU cores that will always work to achieve optimal binding cannot be defined beforehand. To avoid this problem, for jobs that request resources in shared nodes, we provide a script that can generate the correct list once the job starts execution.

Use of generate_CPU_BIND.sh script for generating an ordered list of CPU cores for optimal binding
The generation of the ordered list to be used with the --cpu-bind option of srun can be automated within the script generate_CPU_BIND.sh, which is available by default to all users through the module pawseytools (loaded by default).

Use the script generate_CPU_BIND.sh only in GPU nodes
The use of the script generate_CPU_BIND.sh is only meaningful in GPU nodes and will report errors if executed on CPU nodes, like:

ERROR:root:Driver not initialized (amdgpu not found in modules)

or similar.

The generate_CPU_BIND.sh script receives one parameter (map_cpu OR mask_cpu) and gives back the best ordered list of CPU-cores or CPU-masks for optimal communication between tasks and GPUs.

For a better understanding of what this script generates and how it is useful, we can use an interactive session. Note that the request parameters given salloc  only include the number of nodes and the number of Slurm GPUs (GCDs) per node to request a number of "allocation-packs" (as described at the top of this page). In this case, 3 "allocation-packs" are requested:

Terminal N. Explaining the use of the script "generate_CPU_BIND.sh" from an salloc session

$ salloc -N 1 --gres=gpu:3 -A yourProject-gpu --partition=gpu-dev
salloc: Granted job allocation 1370877
 
 
$ scontrol show jobid $SLURM_JOBID
JobId=1370877 JobName=interactive
   UserId=quokka(20146) GroupId=quokka(20146) MCS_label=N/A
   Priority=16818 Nice=0 Account=rottnest0001-gpu QOS=normal
   JobState=RUNNING Reason=None Dependency=(null)
   Requeue=1 Restarts=0 BatchFlag=0 Reboot=0 ExitCode=0:0
   RunTime=00:00:48 TimeLimit=01:00:00 TimeMin=N/A
   SubmitTime=16:45:41 EligibleTime=16:45:41
   AccrueTime=Unknown
   StartTime=16:45:41 EndTime=17:45:41 Deadline=N/A
   SuspendTime=None SecsPreSuspend=0 LastSchedEval=16:45:41 Scheduler=Main
   Partition=gpu AllocNode:Sid=joey-02:253180
   ReqNodeList=(null) ExcNodeList=(null)
   NodeList=nid001004
   BatchHost=nid001004
   NumNodes=1 NumCPUs=48 NumTasks=1 CPUs/Task=1 ReqB:S:C:T=0:0:*:1
   TRES=cpu=48,mem=88320M,node=1,billing=192,gres/gpu=3
   Socks/Node=* NtasksPerN:B:S:C=0:0:*:1 CoreSpec=*
   MinCPUsNode=1 MinMemoryNode=0 MinTmpDiskNode=0
   Features=(null) DelayBoot=00:00:00
   OverSubscribe=OK Contiguous=0 Licenses=(null) Network=(null)
   Command=(null)
   WorkDir=/scratch/rottnest0001/quokka/hello_jobstep
   Power=
   CpusPerTres=gres:gpu:8
   MemPerTres=gpu:29440
   TresPerNode=gres:gpu:3  
 
 
$ rocm-smi --showhw
======================= ROCm System Management Interface =======================
============================ Concise Hardware Info =============================
GPU  DID   GFX RAS   SDMA RAS  UMC RAS   VBIOS           BUS
0    7408  DISABLED  ENABLED   DISABLED  113-D65201-042  0000:C9:00.0
1    7408  DISABLED  ENABLED   DISABLED  113-D65201-042  0000:D1:00.0
2    7408  DISABLED  ENABLED   DISABLED  113-D65201-042  0000:D6:00.0
================================================================================
============================= End of ROCm SMI Log ==============================
 
 
$ generate_CPU_BIND.sh map_cpu
map_cpu:21,2,14
 
 
$ generate_CPU_BIND.sh mask_cpu
mask_cpu:0000000000FF0000,00000000000000FF,000000000000FF00

As can be seen, 3 "allocation-packs" were requested, and the total amount of allocated resources are written in the output of the scontrol command, including the 3 GCDs (logical/Slurm GPUs) and 88.32GB of memory. The rocm-smi command gives a list of the three allocated devices, listed locally as GPU:0-BUS_ID:C9, GPU:1-BUS_ID:D1 & GPU:2-BUS_ID:D6.

When using generate_CPU_BIND.sh script with the parameter map_cpu, it creates a list of CPU-cores that can be used in the srun command for optimal binding. In this case, we get: map_cpu:21,2,14 which, in order, correspond to the slurm-sockets chiplet2,chiplet0,chiplet1; which are the ones in direct connection to the C9,D1,D6 GCDs respectively. (Check the GPU node architecture diagram at the top of this page.)

For jobs that require several threads per CPU task, srun would need a list of masks instead of CPU core IDs. The generate_CPU_BIND.sh script can generate this list when the parameter mask_cpu is used. Then, the script creates a list of hexadecimal CPU-masks that can be used for optimally binding an hybrid job. In this case, we get: mask_cpu:0000000000FF0000,00000000000000FF,000000000000FF00 . These masks, in order, correspond to masks that activate only the CPU-cores of chiplet2, chiplet0 & chiplet1; which are the ones in direct connection to the C9,D1,D6 GCDs respectively. (Check the GPU node architecture diagram at the top of this page and external SLURM documentation for detailed explanation about masks.)

An extensive documentation about the use of masks is in the online documentation of Slurm, but a brief explanation can be given here. First thing to notice is that masks have 16 hexadecimal characters and each of the characters can be understood as an hexadecimal "mini-mask" that correspond to 4 CPU-cores. Then, a pair of characters will cover 8 CPU-cores, that is: each pair of characters represents a chiplet. Then, for example, the second mask in the list (00000000000000FF) disables all the cores of the CPU for their use by the second MPI task, and only make available the first 8 cores, which correspond to chiplet0. (Remember to read numbers with the usual increase in hierarchy: right to left.) Then, the first character (right to left) is the hexadecimal mini-mask of CPU cores C00-C03, and the second character (right to left) is the hexadecimal mini-mask of CPU cores C04-C07.

To understand what the hexadecimal character really means we need to use their corresponding conversion to a binary number. To fully understand this, let's focus first on a hypothetical example. Let's assume, as an example, that one would like to make available only the third (C02) and the fourth (C03) CPU-cores, and that one would use binary numbers to represent a mini-mask of their availability or disability. Again, increasing hierarchy from right to left, the binary-mini-mask would be "1100" (third and fourth cores available). This binary-mini-mask represents the decimal number "12", and the hexadecimal-mini-mask is "C". Now, if the 4 cores of the mini-mask are to be available to the task, then the binary-mini-mask would be "1111", which represents the decimal number "15" and the hexadecimal-mini-mask is "F". With this in mind, it can be seen that the full masks in the original list represent availability of only the cores in chiplet2 (and nothing else) for the first task (and its threads) spawned by srun, only the cores of chiplet0 for the second task and only the cores of chiplet1 for the third task.

In practice, it is common to use the output provided by the generate_CPU_BIND.sh script and assign it to a variable which is then used within the srun command. So, a job that requires the use of 8 CPU tasks (single threaded) with 1 GCD (logical/Slurm GPU) per task, the set of commands to be used would be:

export MPICH_GPU_SUPPORT_ENABLED=1
export OMP_NUM_THREADS=1
CPU_BIND=$(generate_CPU_BIND.sh map_cpu)
srun -N 1 -n 8 -c 8 --gres=gpu:8 --cpu-bind=${CPU_BIND} ./selectGPU_X.sh myMPIExecutable

Note that the selectGPU_X.sh wrapper is part of the first auxiliary technique of the "manual" method of optimal binding and is described in the sub-sections above.

The explanation of the "manual" method will be completed in the following sections where a very useful code (hello_jobstep) will be used to confirm optimal (or sub-optimal) binding of GCDs (logical Slurm GPUs) and chiplets for srun job steps.

(If users want to list the generation script in order to check the logic within, they can use the following command:

cat $(which generate_CPU_BIND.sh)

)

MPI & OpenMP settings
Use OMP_NUM_THREADS to control the threads launched per task
As mentioned in the previous section, allocation of resources is granted in "allocation-packs" with 8 cores (1 chiplet) per GCD. Also briefly mentioned in previous section is the need of "reserving" chunks of whole chiplets (multiples of 8 CPU cores) in the srun command via the --cpus-per-task ( -c ) option. But the use of this option in srun is still more a "reservation" parameter for the srun tasks to be binded to the whole chiplets, rather than an indication of the "real number of threads" to be used by the executable. The real number of threads to be used by the executable needs to be controled by the  OpenMP environment variable OMP_NUM_THREADS. In other words, we use --cpus-per-task to make available whole chiplets to the srun task, but use OMP_NUM_THREADS to control the real number of threads per srun task.

For pure MPI-GPU jobs it is recommended to set OMP_NUM_THREADS=1 before executing the srun command and avoid unexpected use of OpenMP threads:

export OMP_NUM_THREADS=1

srun ... -c 8 ...

For GPU codes with hybrid management on the CPU side (MPI + OpenMP + GPU), the environment variable needs to be set to the required number of threads per MPI task. For example, if 4 threads per task are required, then settings should be:

export OMP_NUM_THREADS=4

srun ... -c 8 ...

Also mentioned above is the example of a case where the "real number of threads" is 14 (which is greater than 8) and, therefore, requiring more than one chiplet. In that case, srun should reserve the number of chiplets per task that satisfy the demand using multiples of 8 in the --cpus-per-task (-c) option, togehter with the set the real number of threads with the OMP_NUM_THREADS environment variable:

export OMP_NUM_THREADS=14

srun ... -c 16 ...

GPU-Aware MPI
Use this for GPU-aware MPI codes
To use GPU-aware Cray MPICH, users must set the following modules and environment variables:

module load craype-accel-amd-gfx90a 
module load rocm/<VERSION> 
export MPICH_GPU_SUPPORT_ENABLED=1

Test code: hello_jobstep
The test code: hello_jobstep. and thanks to ORNL staff for allowing for its use at Pawsey
In this page, an MPI+OpenMP+HIP "Hello, World" program (hello_jobstep) will be used to clarify the placement of tasks on CPU-cores and the associated GPU bindings.

We acknowledge Tom Papatheodore and Oak Ridge National Lab (ORNL) for allowing Pawsey to fork the repository of this useful code and to use it within our own documentation and training material. We also acknowledge the very useful information available in the ORNL documentation for systems similar to Setonix, particularly the Crusher system.

Later in this page, some full examples of batch scripts for the most common scenarios for executing jobs on GPU nodes are presented. In order to show how GCDs are bound to the CPU cores assigned to the job, we make use of the hello_jobstep code within these same examples. For this reason, before presenting the full example, we use this section to explain important details of the test code. (If researchers want to test the code by themselves, this is the forked repository for Pawsey: hello_jobstep repository.)

Compilation and basic use of hello_jobstep test code
The explanation of the test code will be provided with the output of an interactive session that use 3 "allocation-packs" to get access to the 3 GCDs (logical/Slurm GPUs) and 3 full CPU chiplets in different ways.

First part is creating the session and check that the resources were granted as 3 allocation-packs:

Terminal N. Explaining the use of the "hello_jobstep" code from an salloc session (allocation and check)

$ salloc -N 1 --gres=gpu:3 -A <yourProject>-gpu --partition=gpu-dev
salloc: Granted job allocation 339185
 
$ scontrol show jobid $SLURM_JOBID
JobId=339185 JobName=interactive
   UserId=quokka(20146) GroupId=quokka(20146) MCS_label=N/A
   Priority=16818 Nice=0 Account=rottnest0001-gpu QOS=normal
   JobState=RUNNING Reason=None Dependency=(null)
   Requeue=1 Restarts=0 BatchFlag=0 Reboot=0 ExitCode=0:0
   RunTime=00:00:48 TimeLimit=01:00:00 TimeMin=N/A
   SubmitTime=16:45:41 EligibleTime=16:45:41
   AccrueTime=Unknown
   StartTime=16:45:41 EndTime=17:45:41 Deadline=N/A
   SuspendTime=None SecsPreSuspend=0 LastSchedEval=16:45:41 Scheduler=Main
   Partition=gpu AllocNode:Sid=joey-02:253180
   ReqNodeList=(null) ExcNodeList=(null)
   NodeList=nid001004
   BatchHost=nid001004
   NumNodes=1 NumCPUs=48 NumTasks=1 CPUs/Task=1 ReqB:S:C:T=0:0:*:1
   TRES=cpu=48,mem=88320M,node=1,billing=192,gres/gpu=3
   Socks/Node=* NtasksPerN:B:S:C=0:0:*:1 CoreSpec=*
   MinCPUsNode=1 MinMemoryNode=0 MinTmpDiskNode=0
   Features=(null) DelayBoot=00:00:00
   OverSubscribe=OK Contiguous=0 Licenses=(null) Network=(null)
   Command=(null)
   WorkDir=/scratch/rottnest0001/quokka/hello_jobstep
   Power=
   CpusPerTres=gres:gpu:8
   MemPerTres=gpu:29440
   TresPerNode=gres:gpu:3  

Now compile the code:

Terminal N. Explaining the use of the "hello_jobstep" code from an salloc session (compiling)

$ cd $MYSCRATCH
$ git clone https://github.com/PawseySC/hello_jobstep.git
Cloning into 'hello_jobstep'...
...
Resolving deltas: 100% (41/41), done.
$ cd hello_jobstep
 
 
$ module load PrgEnv-cray craype-accel-amd-gfx90a rocm/<VERSION>
$ make hello_jobstep
CC -std=c++11 -fopenmp --rocm-path=/opt/rocm -x hip -D__HIP_ARCH_GFX90A__=1 --offload-arch=gfx90a -I/opt/rocm/include -c hello_jobstep.cpp
CC -fopenmp --rocm-path=/opt/rocm -L/opt/rocm/lib -lamdhip64 hello_jobstep.o -o hello_jobstep

Now check the current allocations available devices, specifically their BUS_ID:

Terminal N. Explaining the use the "hello_jobstep" code from an salloc session (list allocated GPUs)

$ rocm-smi --showhw
======================= ROCm System Management Interface =======================
============================ Concise Hardware Info =============================
GPU  DID   GFX RAS   SDMA RAS  UMC RAS   VBIOS           BUS          
0    7408  DISABLED  ENABLED   DISABLED  113-D65201-042  0000:C9:00.0 
1    7408  DISABLED  ENABLED   DISABLED  113-D65201-042  0000:D1:00.0 
2    7408  DISABLED  ENABLED   DISABLED  113-D65201-042  0000:D6:00.0 
================================================================================
============================= End of ROCm SMI Log ==============================

Using hello_jobstep code for testing the naive approach (a not recommended practice)
In a first test, we observe what happens when no "management" parameters are given to srun. So, in this "non-recommended" setting, the output is:

Terminal N. Explaining the use the "hello_jobstep" code from an salloc session ( "not recommended" use without full srun parameters)

$ export OMP_NUM_THREADS=1; srun -N 1 -n 3 ./hello_jobstep | sort -n
MPI 000 - OMP 000 - HWT 000 - Node nid001004 - RunTime_GPU_ID 0,1,2 - ROCR_VISIBLE_GPU_ID 0,1,2 - GPU_Bus_ID c9,d1,d6
MPI 001 - OMP 000 - HWT 001 - Node nid001004 - RunTime_GPU_ID 0,1,2 - ROCR_VISIBLE_GPU_ID 0,1,2 - GPU_Bus_ID c9,d1,d6
MPI 002 - OMP 000 - HWT 002 - Node nid001004 - RunTime_GPU_ID 0,1,2 - ROCR_VISIBLE_GPU_ID 0,1,2 - GPU_Bus_ID c9,d1,d6

As can be seen, each MPI task can be assigned to the same chiplet by the scheduler, which is not a recommended practice. Also, all three GCDs (logical/Slurm GPUs) that have been allocated are visible to each of the tasks. Although some codes are able to deal with this kind of available resources, this is not the recommended best practice. The recommended best practice is to assign CPU tasks to different chiplets and to provide only 1 GCD per task and, even more, to provide the optimal bandwidth between CPU and GCD.

Using hello_jobstep code for testing optimal binding for a pure MPI job (single threaded) 1 GPU per task
Starting from the same allocation as above (3 "allocation-packs"), now all the parameters needed to define the correct use of resources are provided to srun. In this case, 3 MPI tasks are to be ran (single threaded) each task making use of 1 GCD (logical/Slurm GPU). As described above, there are two methods to achieve optimal binding. The first method only uses Slurm parameters to indicate how resources are to be used by srun. In this case:

Terminal N. Testing srun settings (method 1) for optimal binding for pure MPI job 1 GPU per task.

$ export OMP_NUM_THREADS=1; srun -N 1 -n 3 -c 8 --gres=gpu:3 --gpus-per-task=1 --gpu-bind=closest ./hello_jobstep | sort -n
MPI 000 - OMP 000 - HWT 002 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d1
MPI 001 - OMP 000 - HWT 009 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d6
MPI 002 - OMP 000 - HWT 017 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID c9

As can be seen, GPU-BUS_ID:D1 is having direct communication with a CPU-core in chiplet0. Also GPU-BUS_ID:D6 is in direct communication with a CPU-core in chiplet1 , and GPU-BUS_ID:C9 with a CPU-cre in chiplet2, resulting in an optimal 1 chiplet to 1 GCD binding. (Check node map at the top of the page.)

A similar result can be obtained with the "manual" method for optimal binding. As detailed in sub-sections above, this method uses a wrapper (selectGPU_X.sh, listed above) to define which GCD (logical/Slurm GPU) is going to be visible to each task, and also the uses an ordered list of CPU cores (created with the script generate_CPU_BIND.sh, also described above) to bind the correct CPU core to each task. In this case:

Terminal N. Testing "manual" method (method 2) for optimal binding for pure MPI job 1 GPU per task.

$ CPU_BIND=$(generate_CPU_BIND.sh map_cpu)
$ echo $CPU_BIND
map_cpu:16,3,15

$ export OMP_NUM_THREADS=1; srun -N 1 -n 3 -c 8 --gres=gpu:3 --cpu-bind=${CPU_BIND} ./selectGPU_X.sh ./hello_jobstep | sort -n
MPI 000 - OMP 000 - HWT 016 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID c9
MPI 001 - OMP 000 - HWT 003 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 1 - GPU_Bus_ID d1
MPI 002 - OMP 000 - HWT 015 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 2 - GPU_Bus_ID d6

(Note that the users are reponsible to create their own wrapper with the content suggested in the section named “Method 2, manual method for optimal binding” above.)

As can be seen, GPU-BUS_ID:C9 is having direct communication with a CPU-core in chiplet2. Also GPU-BUS_ID:D1 is in direct communication with chiplet0, and GPU-BUS_ID:D6 with chiplet1, again resulting in an optimal 1-to-1 binding. (Note that in the "manual" method none of these two options are provided to srun: --gpus-per-task nor --gpu-bind.) (See the node map at the top of the page to confirm the optimal binding.)

There are some differences with the result just shown from the first and second methods (slurm and manual) of optimal binding. Although the ordering chiplets for a given rank is different here, this is not imporant since the CPU-to-GCD affinity is optimal. The key difference is in the values of the ROCR_VISIBLE_GPU_IDs. With the first method, these values are always 0 while, in the second method, these values are the ones given by the wrapper that "manually" selects the GPUs. This second difference has proven to be important and may be the reason why the "manual" binding is the only option for codes relying OpenMP or OpenACC pragma's for moving data from/to host to/from GPU and attempting to use GPU-to-GPU enabled MPI communication.

Using hello_jobstep code for testing optimal binding for a hybrid (MPI + several OpenMP threads), 1 GCD (logical/Slurm GPU) per MPI task
If the code is hybrid on the CPU side and needs the use of several OpenMP CPU threads, we then use the OMP_NUM_THREADS environment variable to control the number of threads. So, again, starting from the previous session with 3 "allocation-packs", consider a case for 3 MPI tasks, 4 OpenMP threads per task and 1 GCD (logical/Slurm GPU) per task:

Terminal N. Testing srun settings (method 1) for optimal binding for a case with 4 CPU threads per task and 1 GPU per task

$ export OMP_NUM_THREADS=4; srun -N 1 -n 3 -c 8 --gres=gpu:3 --gpus-per-task=1 --gpu-bind=closest ./hello_jobstep | sort -n
MPI 000 - OMP 000 - HWT 000 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d1
MPI 000 - OMP 001 - HWT 003 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d1
MPI 000 - OMP 002 - HWT 005 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d1
MPI 000 - OMP 003 - HWT 006 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d1
MPI 001 - OMP 000 - HWT 008 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d6
MPI 001 - OMP 001 - HWT 011 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d6
MPI 001 - OMP 002 - HWT 013 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d6
MPI 001 - OMP 003 - HWT 014 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d6
MPI 002 - OMP 000 - HWT 016 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID c9
MPI 002 - OMP 001 - HWT 019 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID c9
MPI 002 - OMP 002 - HWT 021 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID c9
MPI 002 - OMP 003 - HWT 022 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID c9

When the "manual" optimal binding is required, the mask_cpu parameter needs to be used in the generator script (and in the --cpu_bind option of srun):

Terminal N. Testing "manual" method (method 2) for optimal binding for a case with 4 CPU threads per task and 1 GPU per task

$ CPU_BIND=$(generate_CPU_BIND.sh mask_cpu)
$ echo $CPU_BIND
mask_cpu:0000000000FF0000,00000000000000FF,000000000000FF00

$ export OMP_NUM_THREADS=4; srun -N 1 -n 3 -c 8 --gres=gpu:3 --cpu-bind=${CPU_BIND} ./selectGPU_X.sh ./hello_jobstep | sort -n
MPI 000 - OMP 000 - HWT 016 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID c9
MPI 000 - OMP 001 - HWT 018 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID c9
MPI 000 - OMP 002 - HWT 021 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID c9
MPI 000 - OMP 003 - HWT 022 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID c9
MPI 001 - OMP 000 - HWT 000 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 1 - GPU_Bus_ID d1
MPI 001 - OMP 001 - HWT 003 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 1 - GPU_Bus_ID d1
MPI 001 - OMP 002 - HWT 005 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 1 - GPU_Bus_ID d1
MPI 001 - OMP 003 - HWT 006 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 1 - GPU_Bus_ID d1
MPI 002 - OMP 000 - HWT 008 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 2 - GPU_Bus_ID d6
MPI 002 - OMP 001 - HWT 011 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 2 - GPU_Bus_ID d6
MPI 002 - OMP 002 - HWT 013 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 2 - GPU_Bus_ID d6
MPI 002 - OMP 003 - HWT 014 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 2 - GPU_Bus_ID d6

(Note that the users are reponsible to create their own wrapper with the content suggested in the section named “Method 2, manual method for optimal binding” above.)

As explained in previous section, when more than 1 CPU is to be binded to each GPU like in this case, we should provide the cpu_bind using masks (and not a map of CPU numbers). These masks are provided by the generate_CPU_BIND.sh script when mask_cpu is used as the given parameter. The provided masks make available only the cores of chiplet2 to the first MPI task and its OpenMP threads, only the cores of chiplet0 to the second task and only the cores of chiplet1 to the third MPI task and its OpenMP threads.

From the output of the hello_jobstep code, it can be noted that the OpenMP threads use CPU-cores in the same CPU chiplet as the main thread (or MPI task). And all the CPU-cores of the corresponding chiplet are in direct communication with the GCD (logical/Slurm GPU) that has a direct physical connection to it. (Check the architecture diagram at the top of this page.)

Again, there is a difference is in the values of the ROCR_VISIBLE_GPU_IDs in the results of both methods. With the first method, these values are always 0 while, in the second method, these values are the ones given by the wrapper that "manually" selects the GCDs (logical/Slurm GPUs). This difference has proven to be important and may be the reason why the "manual" binding is the only option for codes relying OpenMP or OpenACC pragma's for moving data from/to host to/from GPU and attempting to use GPU-to-GPU enabled MPI communication.

Using hello_jobstep code for testing visibility of all allocated GPUs to each of the tasks
Some codes, like tensorflow and other machine learning engines, require visibility of all GPU resources for an internal-to-the-code management of resources. In that case, optimal binding cannot be provided to the code and then the responsability of optimal binding and communication among the resources is given completely to the code. In that case, the srun options to use are:

Terminal N. Testing options to provide visibility of all GPUs to all CPUs allocated by srun

$ export OMP_NUM_THREADS=1; srun -N 1 -n 3 -c 8 --gres=gpu:3 ./hello_jobstep | sort -n
MPI 000 - OMP 000 - HWT 000 - Node nid001004 - RunTime_GPU_ID 0,1,2 - ROCR_VISIBLE_GPU_ID 0,1,2 - GPU_Bus_ID c9,d1,d6
MPI 001 - OMP 000 - HWT 008 - Node nid001004 - RunTime_GPU_ID 0,1,2 - ROCR_VISIBLE_GPU_ID 0,1,2 - GPU_Bus_ID c9,d1,d6
MPI 002 - OMP 000 - HWT 016 - Node nid001004 - RunTime_GPU_ID 0,1,2 - ROCR_VISIBLE_GPU_ID 0,1,2 - GPU_Bus_ID c9,d1,d6

As can be seen, each MPI task is assigned to a different chiplet. Also, all three GCDs (logical/Slurm GPUs) that have been allocated are visible to each of the tasks. Yes, this practice coincides with the “not recommended” naive example above but, for the mentioned codes, is what they need to run and it’s the code “optimal” binding of the GPUs is now an internal responsability of the code.

Example scripts for: Exclusive access to the GPU nodes with optimal binding
In this section, a series of example slurm job scripts are presented in order for the users to be able to use them as a point of departure for preparing their own scripts. The examples presented here make use of most of the important concepts, tools and techniques explained in the previous section, so we encourage users to take a look into that top section of this page first.

Single Exclusive Node Multi-GPU job: 8 GCDs (logical/Slurm GPUs), each of them controlled by one MPI task
As for all scripts, we provide the parameters for requesting the necessary "allocation-packs" for the job. This example considers a job that will make use of the 8 GCDs (logical/Slurm GPUs) on 1 node (8 "allocation-packs"). The resources request use the following two parameters:

#SBATCH --nodes=1   #1 node in this example 
#SBATCH --exclusive #All resources of the node are exclusive to this job
#                   #8 GPUs per node (8 "allocation-packs" in total for the job)

Note that only these two allocation parameters are needed to provide the information for the requested number of allocation-packs, and no other parameter related to memory or CPU cores should be provided in the request header.

The use/management of the allocated resources is controlled by the srun options and some environmental variables. As mentioned above, there are two methods for achieving optimal binding. The method that uses only srun parameters is preferred (method 1), but may not always work and, in that case, the "manual" method (method 2) may be needed. The two scripts for the different methods for optimal binding are in the following tabs:


N Exclusive Nodes Multi-GPU job: 8*N GCDs (logical/Slurm GPUs), each of them controlled by one MPI task
As for all scripts, we provide the parameters for requesting the necessary "allocation-packs" for the job. The same procedure mentioned above for the single exclusive node job should be applied for multi-node exclusive jobs. The only difference when requesting resources is the number of exclusive nodes requested. So, for example, for a job requiring 2 exclusive nodes (16 GCDs (logical/Slurm GPUs) or 16 "allocation-packs") the resources request use the following two parameters:

#SBATCH --nodes=2   #2 nodes in this example 
#SBATCH --exclusive #All resources of the node are exclusive to this job

   #                   #8 GPUs per node (16 "allocation-packs" in total for the job)

Note that only these two allocation parameters are needed to provide the information for the requested number of allocation-packs, and no other parameter related to memory or CPU cores should be provided in the request header.

The use/management of the allocated resources is controlled by the srun options and some environmental variables. As mentioned above, there are two methods for achieving optimal binding. The method that uses only srun parameters is preferred (method 1), but may not always work and, in that case, the "manual" method (method 2) may be needed. The two scripts for the different methods for optimal binding are in the following tabs:





Example scripts for: Shared access to the GPU nodes with optimal binding
Shared node 1 GPU job
Jobs that need only 1 GCD (logical/Slurm GPU) for their execution are going to be sharing the GPU node with other jobs. That is, they will run in shared access, which is the default so no request for exclusive access is performed.

As for all scripts, we provide the parameters for requesting the necessary "allocation-packs" for the job. In this case we ask for 1 allocation-pack with:

#SBATCH --nodes=1              #1 nodes in this example  
#SBATCH --gres=gpu:1           #1 GPU per node (1 "allocation-pack" in total for the job)

Note that only these two allocation parameters are needed to provide the information for the requested number of allocation-packs, and no other parameter related to memory or CPU cores should be provided in the request header.

The use/management of the allocated resources is controlled by the srun options and some environmental variables. As only 1 allocation-pack is requested, there is no need to take any other action for optimal binding of CPU chiplet and GPU as it is guaranteed:

Listing N. exampleScript_1NodeShared_1GPU.sh

#!/bin/bash --login
#SBATCH --job-name=1GPUSharedNode
#SBATCH --partition=gpu
#SBATCH --nodes=1              #1 nodes in this example 
#SBATCH --gres=gpu:1           #1 GPU per node (1 "allocation-pack" in total for the job)
#SBATCH --time=00:05:00
#SBATCH --account=<yourProject>-gpu #IMPORTANT: use your own project and the -gpu suffix
#(Note that there is not request for exclusive access to the node)

#----
#Loading needed modules (adapt this for your own purposes):
module load PrgEnv-cray
module load rocm/<VERSION> craype-accel-amd-gfx90a
echo -e "\n\n#------------------------#"
module list

#----
#Printing the status of the given allocation
echo -e "\n\n#------------------------#"
echo "Printing from scontrol:"
scontrol show job ${SLURM_JOBID}

#----
#Definition of the executable (we assume the example code has been compiled and is available in $MYSCRATCH):
exeDir=$MYSCRATCH/hello_jobstep
exeName=hello_jobstep
theExe=$exeDir/$exeName

#----
#MPI & OpenMP settings
#Not needed for 1GPU:export MPICH_GPU_SUPPORT_ENABLED=1 #This allows for GPU-aware MPI communication among GPUs
export OMP_NUM_THREADS=1           #This controls the real CPU-cores per task for the executable

#----
#Execution
#Note: srun needs the explicit indication full parameters for use of resources in the job step.
#      These are independent from the allocation parameters (which are not inherited by srun)
#      For optimal GPU binding using slurm options,
#      "--gpus-per-task=1" and "--gpu-bind=closest" create the optimal binding of GPUs      
#      (Although in this case this can be avoided as only 1 "allocation-pack" has been requested)
#      "-c 8" is used to force allocation of 1 task per CPU chiplet. Then, the REAL number of threads
#         for the code SHOULD be defined by the environment variables above.
#      (The "-l" option is for displaying, at the beginning of each line, the taskID that generates the output.)
#      (The "-u" option is for unbuffered output, so that output is displayed as soon as it's generated.)
#      (If the output needs to be sorted for clarity, then add "| sort -n" at the end of the command.)
echo -e "\n\n#------------------------#"
echo "Test code execution:"
srun -l -u -N 1 -n 1 -c 8 --gres=gpu:1 --gpus-per-task=1 --gpu-bind=closest ${theExe}

#----
#Printing information of finished job steps:
echo -e "\n\n#------------------------#"
echo "Printing information of finished jobs steps using sacct:"
sacct -j ${SLURM_JOBID} -o jobid%20,Start%20,elapsed%20

#----
#Done
echo -e "\n\n#------------------------#"
echo "Done"

And the output after executing this example is:

Terminal N. Output for a 1 GPU job (using only 1 allocation-pack in a shared node)

$ sbatch exampleScript_1NodeShared_1GPU.sh
Submitted batch job 323098

$ cat slurm-323098.out
...
#------------------------#
Test code execution:
0: MPI 000 - OMP 000 - HWT 002 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d1
...
#------------------------#
Done

The output of the hello_jobstep code tells us that the CPU-core "002" and GPU with Bus_ID:D1 were utilised by the job. Optimal binding is guaranteed for a single "allocation-pack" as memory, CPU chiplet and GPU of each pack is optimal.

Shared node 3 MPI tasks each controlling 1 GCD (logical/Slurm GPU)
As for all scripts, we provide the parameters for requesting the necessary "allocation-packs" for the job. In this case we ask for 3 allocation-packs with:

#SBATCH --nodes=1           #1 nodes in this example  
#SBATCH --gres=gpu:3        #3 GPUs per node (3 "allocation-packs" in total for the job)

Note that only these two allocation parameters are needed to provide the information for the requested number of allocation-packs, and no other parameter related to memory or CPU cores should be provided in the request header.

The use/management of the allocated resources is controlled by the srun options and some environmental variables. As mentioned above, there are two methods for achieving optimal binding. The method that uses only srun parameters is preferred (method 1), but may not always work and, in that case, the "manual" method (method 2) may be needed. The two scripts for the different methods for optimal binding are in the following tabs:


Example scripts for: Hybrid jobs (multiple threads) on the CPU side
When the code is hybrid on the CPU side (MPI + OpenMP) the logic is similar to the above examples, except that more than 1 CPU-core chiplet needs to be accessible per srun task. This is controlled by the OMP_NUM_THREADS environment variable and will also imply a change in the settings for the optimal binding of resources when the "manual" binding (method 2) is applied.

In the following example, we use 3 GCDs (logical/slurm GPUs) (1 per MPI task) and the number of CPU threads per task is 5. As for all scripts, we provide the parameters for requesting the necessary "allocation-packs" for the job. In this case we ask for 3 allocation-packs with:

#SBATCH --nodes=1         #1 nodes in this example  
#SBATCH --gres=gpu:3      #3 GPUs per node (3 "allocation-packs" in total for the job)

Note that only these two allocation parameters are needed to provide the information for the requested number of allocation-packs, and no other parameter related to memory or CPU cores should be provided in the request header. And the real number of threads per task is controlled with:

export OMP_NUM_THREADS=5           #This controls the real CPU-cores per task for the executable

The use/management of the allocated resources is controlled by the srun options and some environmental variables. As mentioned above, there are two methods for achieving optimal binding. The method that uses only srun parameters is preferred (method 1), but may not always work and, in that case, the "manual" method (method 2) may be needed. The two scripts for the different methods for optimal binding are in the following tabs:


Example scripts for: Jobs where each task needs access to multiple GPUs
Exclusive nodes: all 8 GPUs in each node accessible to all 8 tasks in the node
Some applications, like Tensorflow and other Machine Learning applications, may requiere access to all the available GPUs in the node. In this case, the optimal binding and communication cannot be granted by the scheduler when assigning resources to the srun launcher. Then, the full responsability for the optimal use of the resources relies on the code itself.

As for all scripts, we provide the parameters for requesting the necessary "allocation-packs" for the job. This example considers a job that will make use of the 8 GCDs (logical/Slurm GPUs) on 2 nodes (16 "allocation-packs" in total). The resources request use the following two parameters:

#SBATCH --nodes=2   #2 nodes in this example 
#SBATCH --exclusive #All resources of each node are exclusive to this job

   #                   #8 GPUs per node (16 "allocation-packs" in total for the job)

Note that only these two allocation parameters are needed to provide the information for the requested number of allocation-packs, and no other parameter related to memory or CPU cores should be provided in the request header.

The use/management of the allocated resources is controlled by the srun options and some environmental variables. As mentioned above, optimal binding cannot be achieved by the scheduler, so no settings for optimal binding are given to the launcher. Also, all the GPUs in the node are available to each of the tasks:

Listing N. exampleScript_2NodesExclusive_16GPUs_8VisiblePerTask.sh

#!/bin/bash --login
#SBATCH --job-name=16GPUExclusiveNode-8GPUsVisiblePerTask
#SBATCH --partition=gpu
#SBATCH --nodes=2              #2 nodes in this example 
#SBATCH --exclusive            #All resources of the node are exclusive to this job
#                              #8 GPUs per node (16 "allocation packs" in total for the job)
#SBATCH --time=00:05:00
#SBATCH --account=<yourProject>-gpu #IMPORTANT: use your own project and the -gpu suffix

#----
#Loading needed modules (adapt this for your own purposes):
#For the hello_jobstep example:
module load PrgEnv-cray
module load rocm/<VERSION> craype-accel-amd-gfx90a
#OR for a tensorflow example:
#module load tensorflow/<version>
echo -e "\n\n#------------------------#"
module list

#----
#Printing the status of the given allocation
echo -e "\n\n#------------------------#"
echo "Printing from scontrol:"
scontrol show job ${SLURM_JOBID}

#----
#Definition of the executable (we assume the example code has been compiled and is available in $MYSCRATCH):
exeDir=$MYSCRATCH/hello_jobstep
exeName=hello_jobstep
theExe=$exeDir/$exeName

#----
#MPI & OpenMP settings if needed (these won't work for Tensorflow):
export MPICH_GPU_SUPPORT_ENABLED=1 #This allows for GPU-aware MPI communication among GPUs
export OMP_NUM_THREADS=1           #This controls the real CPU-cores per task for the executable

#----
#TensorFlow settings if needed:
#  The following two variables control the real number of threads in Tensorflow code:
#export TF_NUM_INTEROP_THREADS=1    #Number of threads for independent operations
#export TF_NUM_INTRAOP_THREADS=1    #Number of threads within individual operations 

#----
#Execution
#Note: srun needs the explicit indication full parameters for use of resources in the job step.
#      These are independent from the allocation parameters (which are not inherited by srun)
#      Each task needs access to all the 8 available GPUs in the node where it's running.
#      So, no optimal binding can be provided by the scheduler.
#      Therefore, "--gpus-per-task" and "--gpu-bind" are not used.
#      Optimal use of resources is now responsability of the code.
#      "-c 8" is used to force allocation of 1 task per CPU chiplet. Then, the REAL number of threads
#         for the code SHOULD be defined by the environment variables above.
#      (The "-l" option is for displaying, at the beginning of each line, the taskID that generates the output.)
#      (The "-u" option is for unbuffered output, so that output is displayed as soon as it's generated.)
#      (If the output needs to be sorted for clarity, then add "| sort -n" at the end of the command.)
echo -e "\n\n#------------------------#"
echo "Test code execution:"
srun -l -u -N 2 -n 16 -c 8 --gres=gpu:8 ${theExe}
#srun -l -u -N 2 -n 16 -c 8 --gres=gpu:8 python3 ${tensorFlowScript}

#----
#Printing information of finished job steps:
echo -e "\n\n#------------------------#"
echo "Printing information of finished jobs steps using sacct:"
sacct -j ${SLURM_JOBID} -o jobid%20,Start%20,elapsed%20

#----
#Done
echo -e "\n\n#------------------------#"
echo "Done"

And the output after executing this example is:

Terminal N. Output for a 16 GPU job with 16 tasks each of the task accessing the 8 GPUs in their running node

$ sbatch exampleScript_2NodesExclusive_16GPUs_8VisiblePerTask.sh
Submitted batch job 7798215

$ cat slurm-7798215.out
...
#------------------------#
Test code execution:
 0: MPI 000 - OMP 000 - HWT 001 - Node nid002944 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
 1: MPI 001 - OMP 000 - HWT 008 - Node nid002944 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
 2: MPI 002 - OMP 000 - HWT 016 - Node nid002944 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
 3: MPI 003 - OMP 000 - HWT 024 - Node nid002944 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
 4: MPI 004 - OMP 000 - HWT 032 - Node nid002944 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
 5: MPI 005 - OMP 000 - HWT 040 - Node nid002944 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
 6: MPI 006 - OMP 000 - HWT 049 - Node nid002944 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
 7: MPI 007 - OMP 000 - HWT 056 - Node nid002944 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
 8: MPI 008 - OMP 000 - HWT 000 - Node nid002946 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
 9: MPI 009 - OMP 000 - HWT 008 - Node nid002946 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
10: MPI 010 - OMP 000 - HWT 016 - Node nid002946 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
11: MPI 011 - OMP 000 - HWT 025 - Node nid002946 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
12: MPI 012 - OMP 000 - HWT 032 - Node nid002946 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
13: MPI 013 - OMP 000 - HWT 040 - Node nid002946 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
14: MPI 014 - OMP 000 - HWT 048 - Node nid002946 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
15: MPI 015 - OMP 000 - HWT 056 - Node nid002946 - RunTime_GPU_ID 0,1,2,3,4,5,6,7 - ROCR_VISIBLE_GPU_ID 0,1,2,3,4,5,6,7 - GPU_Bus_ID c1,c6,c9,ce,d1,d6,d9,de
...
#------------------------#
Done

The output of the hello_jobstep code tells us that job ran 8 MPI tasks on node nid002944 and other 8 MPI tasks on node nid002946. Each of the MPI tasks has only 1 CPU-core assigned to it (with the use of the OMP_NUM_THREADS environment variable in the script) and can be identified with the HWT number. Clearly, each of the CPU tasks run on a different chiplet.

More importantly for this example, each of the MPI tasks have access to the 8 GCDs (logical/Slurm GPU) in their node. Proper and optimal GPU management and communication is responsability of the code. The hardware identification is done via the Bus_ID (as the other GPU_IDs are not physical but relative to the job).

Shared nodes: Many GPUs requested but 2 GPUs binded to each task
Some applications may requiere each of the spawned task to have access to multiple GPUs. In this case, some optimal binding and communication can still be granted by the scheduler when assigning resources with the srun launcher. Although final responsability for the optimal use of the multiple GPUs assigned to each task relies on the code itself.

As for all scripts, we provide the parameters for requesting the necessary "allocation-packs" for the job. This example considers a job that will make use of the 6 GCDs (logical/Slurm GPUs) on 1 node (6 "allocation-packs" in total). The resources request use the following two parameters:

#SBATCH --nodes=1     #1 node in this example 
#SBATCH --gres=gpu:6  #6 GPUs per node (6 "allocation packs" in total for the job)

Note that only these two allocation parameters are needed to provide the information for the requested number of allocation-packs, and no other parameter related to memory or CPU cores should be provided in the request header.

The use/management of the allocated resources is controlled by the srun options and some environmental variables. As mentioned above, some best binding can still be achieved by the scheduler providing 2 GPUs to each of the tasks:

Listing N. exampleScript_1NodeShared_6GPUs_2VisiblePerTask.sh

#!/bin/bash --login
#SBATCH --job-name=6GPUSharedNode-2GPUsVisiblePerTask
#SBATCH --partition=gpu
#SBATCH --nodes=1              #1 nodes in this example 
#SBATCH --gres=gpu:6           #6 GPUs per node (6 "allocation packs" in total for the job)
#SBATCH --time=00:05:00
#SBATCH --account=<yourProject>-gpu #IMPORTANT: use your own project and the -gpu suffix

#----
#Loading needed modules (adapt this for your own purposes):
module load PrgEnv-cray
module load rocm/<VERSION> craype-accel-amd-gfx90a
echo -e "\n\n#------------------------#"
module list

#----
#Printing the status of the given allocation
echo -e "\n\n#------------------------#"
echo "Printing from scontrol:"
scontrol show job ${SLURM_JOBID}

#----
#Definition of the executable (we assume the example code has been compiled and is available in $MYSCRATCH):
exeDir=$MYSCRATCH/hello_jobstep
exeName=hello_jobstep
theExe=$exeDir/$exeName

#----
#MPI & OpenMP settings if needed (these won't work for Tensorflow):
export MPICH_GPU_SUPPORT_ENABLED=1 #This allows for GPU-aware MPI communication among GPUs
export OMP_NUM_THREADS=1           #This controls the real CPU-cores per task for the executable

#----
#Execution
#Note: srun needs the explicit indication full parameters for use of resources in the job step.
#      These are independent from the allocation parameters (which are not inherited by srun)
#      For best possible GPU binding using slurm options,
#      "--gpus-per-task=2" and "--gpu-bind=closest" will provide the best GPUs to the tasks.
#      But best is still not optimal.
#      Each task have access to 2 available GPUs in the node where it's running.
#      Optimal use of resources of each of the 2GPUs accesible per task is now responsability of the code.
#      IMPORTANT: Note the use of "-c 16" to "reserve" 2 chiplets per task and is consistent with
#                 the use of "--gpus-per-task=2" to "reserve" 2 GPUs per task. Then, the REAL number of
#                 threads for the code SHOULD be defined by the environment variables above.
#      (The "-l" option is for displaying, at the beginning of each line, the taskID that generates the output.)
#      (The "-u" option is for unbuffered output, so that output is displayed as soon as it's generated.)
#      (If the output needs to be sorted for clarity, then add "| sort -n" at the end of the command.)
echo -e "\n\n#------------------------#"
echo "Test code execution:"
srun -l -u -N 1 -n 3 -c 16 --gres=gpu:6 --gpus-per-task=2 --gpu-bind=closest ${theExe}

#----
#Printing information of finished job steps:
echo -e "\n\n#------------------------#"
echo "Printing information of finished jobs steps using sacct:"
sacct -j ${SLURM_JOBID} -o jobid%20,Start%20,elapsed%20

#----
#Done
echo -e "\n\n#------------------------#"
echo "Done"

And the output after executing this example is:

Terminal N. Output for a 6 GPU job with 3 tasks and 2 GPUs per task

$ sbatch exampleScript_1NodeShared_6GPUs_2VisiblePerTask.sh
Submitted batch job 7842635

$ cat slurm-7842635.out
...
#------------------------#
Test code execution:
0: MPI 000 - OMP 000 - HWT 000 - Node nid002948 - RunTime_GPU_ID 0,1 - ROCR_VISIBLE_GPU_ID 0,1 - GPU_Bus_ID d1,d6
1: MPI 001 - OMP 000 - HWT 016 - Node nid002948 - RunTime_GPU_ID 0,1 - ROCR_VISIBLE_GPU_ID 0,1 - GPU_Bus_ID c9,ce
2: MPI 002 - OMP 000 - HWT 032 - Node nid002948 - RunTime_GPU_ID 0,1 - ROCR_VISIBLE_GPU_ID 0,1 - GPU_Bus_ID d9,de
...
#------------------------#
Done

The output of the hello_jobstep code tells us that job ran 3 MPI tasks on node nid002948. Each of the MPI tasks has only 1 CPU-core assigned to it (with the use of the OMP_NUM_THREADS environment variable in the script) and can be identified with the HWT number. Clearly, each of the CPU tasks run on a different chiplet. But more important, the spacing of the chiplets is every 16 cores (two chiplets), thanks to the "-c 16" setting in the srun command, allowing for the best binding of the 2 GPUs assigned to each task.

More importantly for this example, each of the MPI tasks have access to 2 GCDs (logical/Slurm GPU) in their node. The hardware identification is done via the Bus_ID (as the other GPU_IDs are not physical but relative to the job). The assigned GPUs are indeed the 2 closest to the CPU core, as can be verified with the architecture diagram provided at the top of this page. Final proper and optimal GPU management and communication is responsability of the code. 

Example scripts for: Packing GPU jobs
Packing the execution of 8 independent instances each using 1 GCD (logical/Slurm GPU)
This kind of packing can be performed with the help of an additional job-packing-wrapper script (jobPackingWrapper.sh) that rules the independent execution of different codes (or different instances of the same code) to be ran by each of the srun-tasks spawned by srun. (It is important to understand that these instances do not interact with each other via MPI messaging.) The isolation of each code/instance should be performed via the logic included in this job-packing-wrapper script.

In the following example, the job-packing-wrapper creates 8 different output directories and then launches 8 different instances of the hello_nompi code. The output of each of the executions is saved in a different case directory and file. In this case, the executable do not receive any further parameters but, in practice, users should define the logic for their own purposes and, if needed, include the logic to receive different parameters for each instance.

Listing N. jobPackingWrapper.sh

#!/bin/bash
#Job Packing Wrapper: Each srun-task will use a different instance of the executable.
#                     For this specific example, each srun-task will run on a different case directory
#                     and create an isolated log file.
#                     (Adapt wrapper script for your own purposes.)

caseHere=case_${SLURM_PROCID}
echo "Executing job-packing-wrapper instance with caseHere=${caseHere}"

exeDir=${MYSCRATCH}/hello_jobstep
exeName=hello_nompi #Using the no-MPI version of the code
theExe=${exeDir}/${exeName}

logHere=log_${exeName}_${SLURM_JOBID}_${SLURM_PROCID}.out
mkdir -p $caseHere
cd $caseHere

${theExe} > ${logHere} 2>&1  

Note that besides the use of the additional job-packing-wrapper, the rest of the script is very similar to the single-node exclusive examples given above. As for all scripts, we provide the parameters for requesting the necessary "allocation-packs" for the job. This example considers a job that will make use of the 8 GCDs (logical/Slurm GPUs) on 1 node (8 "allocation-packs"). Each allocated-pack of GPU resources will be used by each of the instances controlled by the job-packing-wrapper. The resources request use the following two parameters:

#SBATCH --nodes=1   #1 node in this example 
#SBATCH --exclusive #All resources of the node are exclusive to this job

   #                   #8 GPUs per node (8 "allocation-packs" in total for the job)

Note that only these two allocation parameters are needed to provide the information for the requested number of allocation-packs, and no other parameter related to memory or CPU cores should be provided in the request header.

The use/management of the allocated resources is controlled by the srun options and some environmental variables. For srun, this is not different to an MPI job with 8 tasks. But in reality, this is not an MPI job. On the contrary, srun will spawn 8 tasks, each one of them executing the job-packing-wrapper, but the logic of the job-packing-wrapper allows for 8 independent executions of the desired code(s).

As mentioned above, there are two methods for achieving optimal binding. The method that uses only srun parameters is preferred (method 1), but may not always work and, in that case, the "manual" method (method 2) may be needed. The two scripts for the different methods for optimal binding are in the following tabs:

Listing N. exampleScript_1NodeExclusive_8GPUs_jobPacking.sh

#!/bin/bash --login
#SBATCH --job-name=JobPacking8GPUsExclusive-bindMethod1
#SBATCH --partition=gpu
#SBATCH --nodes=1              #1 nodes in this example 
#SBATCH --exclusive            #All resources of the node are exclusive to this job
#                              #8 GPUs per node (8 "allocation-packs" in total for the job)
#SBATCH --time=00:05:00
#SBATCH --account=<yourProject>-gpu #IMPORTANT: use your own project and the -gpu suffix

#----
#Loading needed modules (adapt this for your own purposes):
module load PrgEnv-cray
module load rocm/<VERSION> craype-accel-amd-gfx90a
echo -e "\n\n#------------------------#"
module list

#----
#Printing the status of the given allocation
echo -e "\n\n#------------------------#"
echo "Printing from scontrol:"
scontrol show job ${SLURM_JOBID}

#----
#Job Packing Wrapper: Each srun-task will use a different instance of the executable.
jobPackingWrapper="jobPackingWrapper.sh"

#----
#MPI & OpenMP settings
#No need for 1GPU steps:export MPICH_GPU_SUPPORT_ENABLED=1 #This allows for GPU-aware MPI communication among GPUs
export OMP_NUM_THREADS=1           #This controls the real CPU-cores per task for the executable

#----
#Execution
#Note: srun needs the explicit indication full parameters for use of resources in the job step.
#      These are independent from the allocation parameters (which are not inherited by srun)
#      "-c 8" is used to force allocation of 1 task per CPU chiplet. Then, the REAL number of threads
#         for the code SHOULD be defined by the environment variables above.
#      (The "-l" option is for displaying, at the beginning of each line, the taskID that generates the output.)
#      (The "-u" option is for unbuffered output, so that output is displayed as soon as it's generated.)
echo -e "\n\n#------------------------#"
echo "Test code execution:"
srun -l -u -N 1 -n 8 -c 8 --gres=gpu:8 --gpus-per-task=1 --gpu-bind=closest ./${jobPackingWrapper}

#----
#Printing information of finished job steps:
echo -e "\n\n#------------------------#"
echo "Printing information of finished jobs steps using sacct:"
sacct -j ${SLURM_JOBID} -o jobid%20,Start%20,elapsed%20

#----
#Done
echo -e "\n\n#------------------------#"
echo "Done" 

After execution of the main slurm bash script, 8 case directories are created (each one of them tagged with their corresponding SLURM_PROCID). And within each of them there is a log file corresponding the execution of each instance that ran according to the logic of the jobPackingWrapper.sh script:

Terminal N. Output for a single job ( on 1 node exclusive) that packs the execution of 8 independet instances

$ sbatch exampleScript_1NodeExclusive_8GPUs_jobPacking.sh
Submitted batch job 339328

$ startDir=$PWD; for iDir in $(ls -d case_*); do echo $iDir; cd $iDir; ls; cat *; cd $startDir; done
case_0
log_hello_nompi_339328_0.out
MAIN 000 - OMP 000 - HWT 002 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d1
case_1
log_hello_nompi_339328_1.out
MAIN 000 - OMP 000 - HWT 009 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d6
case_2
log_hello_nompi_339328_2.out
MAIN 000 - OMP 000 - HWT 017 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID c9
case_3
log_hello_nompi_339328_3.out
MAIN 000 - OMP 000 - HWT 025 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID ce
case_4
log_hello_nompi_339328_4.out
MAIN 000 - OMP 000 - HWT 032 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID d9
case_5
log_hello_nompi_339328_5.out
MAIN 000 - OMP 000 - HWT 044 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID de
case_6
log_hello_nompi_339328_6.out
MAIN 000 - OMP 000 - HWT 049 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID c1
case_7
log_hello_nompi_339328_7.out
MAIN 000 - OMP 000 - HWT 057 - Node nid001004 - RunTime_GPU_ID 0 - ROCR_VISIBLE_GPU_ID 0 - GPU_Bus_ID c6

Comparing the output of each of the instances of the hello_nompi code to the GPU node architecture diagram, it can be seen that the binding of the allocated GCDs (logical/Slurm GPUs) to the L3 cache group chiplets (slurm-sockets) is the optimal for each of them.

Related pages
Setonix User Guide

Example Slurm Batch Scripts for Setonix on CPU Compute Nodes

Setonix General Information: GPU node architecture








