#!/bin/bash --login

#SBATCH --account=pawsey0411
#SBATCH --nodes=1
#SBATCH --job-name=DALiuGE-wallaby_2026-04-20T07-02-10
#SBATCH --time=00:30:00
#SBATCH --error=logs/err-%j.log
#SBATCH --mem=16G

newgrp pawsey0411
umask 002

export DLG_ROOT=/scratch/pawsey0411/jblackwodo/dlg

source /software/projects/pawsey0411/venv/bin/activate

srun -l python3 -m dlg.deploy.start_dlg_cluster --log_dir /scratch/pawsey0411/jblackwodo/dlg/workspace/wallaby_2026-04-20T07-02-10 --physical-graph "/scratch/pawsey0411/jblackwodo/dlg/workspace/wallaby_2026-04-20T07-02-10/wallaby_test.graph"   --verbose-level 1  --max-threads 0 --app 0 --num_islands 1   --ssid wallaby_2026-04-20T07-02-10