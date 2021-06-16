# spark-arrow-experiments
Experimentation guidance framework, for testing and doing automated experiments with Spark and Ceph.

We have 3 independently usable modules:
 1. [`experimenter`](/experimenter/): Main experimentation guidance framework.
 2. [`data_generator`](/data_generator/): Plugin-based data generator. Use this to generate sample parquet files.
 3. [`graph_generator`](/graph_generator/): Simple graph generator, to plot results.


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

For the graph generator, we require:
 - numpy>=1.20.1
Many tools also require
 - scipy>=0.19.1
 - scikit-learn>=0.24.2


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
Experiments define how they give shape to this cycle by registering functions with the execution framework.
The execution framework triggers these functions at the correct time in the cycle.

We provided many default function implementations, to be able to perform automated experiments on our own.

To use our experimentation framework, provided in the [`expermenter`](/experimenter/) directory, use:
```bash
python3 experimenter/entrypoint.py -h
```



## Data Generation
We built a simple data generator in the [`data_generator`](/data_generator/) directory.
Execute it using:
```bash
python3 data_generator/entrypoint.py -h
```
By default, generated data is outputted to `/data_generator/generated/`.
We wrote one plugin, which generates a simple parquet file.

Instead of generating data for the experiments, there are also a few pre-generated files [here](https://github.com/JayjeetAtGithub/datasets).
To get git lfs objects, use:
```bash
apt update
apt install git-lfs
git clone https://github.com/JayjeetAtGithub/datasets
cd datasets/
git lfs pull
```



## Graph Generation
Our basic experiments return a timeseries as datapoints consisting of 2 64-bit integers.
The first number is the initialization time Spark reader implementations needed to start up.
The second number is the computation time Spark needed.

To use the graph generator, use:
```bash
python3 graph_generator/entrypoint.py -h
```
By default, generated graphs are outputted to `/data_generator/generated/`.