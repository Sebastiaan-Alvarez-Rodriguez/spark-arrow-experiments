# spark-arrow-experiments
Simple repository for testing and doing experiments.

## Experiments
We execute several experiments, to check whether our systems function.
Experiment 0: data scalability
Need:
 1. Reserve 8 nodes on GENI
 2. Boot Ceph & Spark on nodes
Repeat for all data sizes:
 3. Deploy data on nodes
 4. Submit job
 5. clean os caches between experiments


## Data Generation
We have a simple data generator in the [`data-generator`](/data-generator) directory.
Execute it using:
```bash
python3 data-generator/entrypoint.py
```