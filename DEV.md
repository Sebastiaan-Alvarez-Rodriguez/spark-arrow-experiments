# Ideas

Experiment reclassification:
 1. ExperimentConfig: Contains all the interesting properties, with sensible defaults. Users can initialize one and set all the properties they want. The user makes a bunch of configs. E.g: For data scalability, the user specifies 1 config for each next data size, with data size being the only variant.
 2. ExperimentScheduler/ExperimentIterator?:
Sorts experiment configs such that:
    1. the ceph cluster remains operational for maximal timespans.
    2. the spark cluster remains operational for maximal timespans.
Exposes the next allocation to perform, together with 2 bools, describing whether we need to halt and reallocate (1) Ceph and (2) Spark.
 3. ExperimentInterface: Contains the start, stop functions? Needed anyway?