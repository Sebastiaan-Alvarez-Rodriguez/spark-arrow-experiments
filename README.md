# spark-arrow-experiments
Execution framework for testing and doing automated experiments with Spark and Ceph.


## Experiments
The general cycle all experiments follow:
 1. Divide nodes into pools.
 2. Install requirements.
 3. Start frameworks.
 4. Deploy data.
 5. Submit Spark application.
 6. Aggregate results.
 7. Stop frameworks

We formalized this concept inside this framework.
Experiments define how they give shape to this cycle by providing functions to the execution framework.
The execution framework triggers these functions at the correct time in the cycle.

We provided many default function implementations, to be able to perform automated experiments on our own.

To use our experimentation framework, provided in the [`expermenter`](/experimenter/) directory, use
```bash
python3 experimenter/entrypoint.py
```

## Data Generation
Additionally, we built a simple data generator in the [`data_generator`](/data_generator/) directory.
Execute it using:
```bash
python3 data_generator/entrypoint.py
```

## Requirements
For the experimentation framework, we require:
 - python>=3.2
 - metareserve
 - spark_deploy>=0.1.1
 - rados_deploy>=0.1.1
 - data_deploy>=0.5.0

For the data generator, we require:
 - pandas
 - pyarrow