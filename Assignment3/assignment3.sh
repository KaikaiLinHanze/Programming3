#!/bin/bash
#SBATCH --mail-user=kaikailin0707@gmail.com
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --partition=assemblix
#SBATCH --job-name=BlastpAssignmentKai
#SBATCH --time 4:00:00
export BLASTDB=/local-fs/datasets/
[ -d output ] || mkdir output

for i in {1..16};
do
/usr/bin/time -o output/timings.txt --append -f "${i}\t%e" blastp -query MCRA.faa -db refseq_protein/refseq_protein -num_threads $i -outfmt 6 >> output/blastoutput.txt;
done
python3 plot.py
