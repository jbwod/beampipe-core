#!/bin/bash --login

#SBATCH --account=$ACCOUNT
#SBATCH --nodes=$NUM_NODES
#SBATCH --job-name=DALiuGE-$SESSION_ID
#SBATCH --time=$JOB_DURATION
#SBATCH --error=logs/err-%j.log
#SBATCH --mem=$SBATCH_MEM

newgrp $ACCOUNT
umask 002

export DLG_ROOT=$DLG_ROOT
$MODULES
$VENV
