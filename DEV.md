# Ideas

Experiment reclassification:
 1. ExperimentConfig: Contains all the interesting properties, with sensible defaults. Users can initialize one and set all the properties they want. The user makes a bunch of configs. E.g: For data scalability, the user specifies 1 config for each next data size, with data size being the only variant.
 2. ExperimentScheduler/ExperimentIterator?:
Sorts experiment configs such that:
    1. the ceph cluster remains operational for maximal timespans.
    2. the spark cluster remains operational for maximal timespans.
Exposes the next allocation to perform, together with 2 bools, describing whether we need to halt and reallocate (1) Ceph and (2) Spark.
 3. ExperimentInterface: Contains the start, stop functions? Needed anyway?


# Problems
 1. RADOS-Ceph seems to be unable to install with local-installed java, or at least, with our local-installed java. Also, setting JAVA_HOME does not help.
 Fixed once by install openjdk-11-jdk using apt.
 Implications: Cannot first install Spark, then Ceph. Can only colocate Ceph admin with spark nodes when the ceph admin installs first.
 Solutions: Change JAVA_HOME to point to the apt installation once installed. 
            Preferably, this pointing is temporary for the cmake & make processes.
            Additionally, could consider assigning the ceph-admin role to a node that actually hosts at least 1 ceph daemon instead of the one with lowest IP.