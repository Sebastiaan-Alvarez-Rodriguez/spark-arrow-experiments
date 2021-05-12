import metareserve
import spark_deploy
import rados_deploy

import experimenter.internal.data as data
from experimenter.internal.experiment.interface import ExperimentInterface
import utils.location as loc
from utils.printer import *


def execute(experiment, reservation):
    '''Execute an experiment.'''
    if not ExperimentInterface.is_experiment(experiment):
        raise ValueError('Passed value is not a valid experiment: {} (type: {}).'.format(experiment, type(experiment)))

    configs = list(experiment.get_configs())
    reservation_snapshot = list(reservation.nodes)

    if len(reservation) > max(len(x.node_config) for x in configs):
        printw('Reservation size ({} nodes) is more than we will need at most for this experiment ({} Spark-nodes + {} Ceph-nodes).'.format(len(reservation), max(configs.node_config, key=lambda x: len(x))))
    for idx, config in enumerate(configs):
        print('Starting experiment {}/{}'.format(idx+1, len(configs)))

        nodes_config = config.node_config
        num_spark_nodes = nodes_config.num_spark_nodes
        num_ceph_nodes = nodes_config.num_ceph_nodes

        ceph_nodes = reservation_snapshot[:num_ceph_nodes]
        spark_nodes = reservation_snapshot[num_ceph_nodes:num_ceph_nodes+num_spark_nodes+1]
        unused_nodes = reservation_snapshot[num_ceph_nodes+num_spark_nodes+1:] if len(reservation_snapshot) >= num_ceph_nodes+num_spark_nodes else []

        print('Ceph nodes:\n{}'.format(''.join('\t{}\n'.format(x) for x in ceph_nodes)))
        print('Total: {} nodes.\n'.format(len(ceph_nodes)))
        print('Spark nodes:\n{}'.format(''.join('\t{}\n'.format(x) for x in spark_nodes)))
        print('Total: {} nodes.\n'.format(len(spark_nodes)))

        if any(unused_nodes):
            printw('Currently not using {} nodes:\n{}'.format(len(unused_nodes), ''.join('\t{}\n'.format(x) for x in unused_nodes)))

        # Phase 0: Install Spark on nodes.
        # if idx > 0 and sorted_configs[idx-1].num_spark_nodes == num_spark_nodes: #TODO: can skip installation, only have to stop & restart
        print('Installing Spark on {} nodes...'.format(len(spark_nodes)))
        if not spark_deploy.install(metareserve.Reservation(spark_nodes), key_path=config.key_path, silent=config.spark_silent or config.silent):
            printe('Could not install Spark (iteration {}/{})'.format(idx+1, len(configs)))
            return False
        print('Starting Spark on {} nodes...'.format(len(spark_nodes)))
        if not spark_deploy.start(metareserve.Reservation(spark_nodes), key_path=config.key_path, worker_workdir=config.spark_workdir, silent=config.spark_silent or config.silent):
            printe('Could not start Spark (iteration {}/{})'.format(idx+1, len(configs)))
            return False

        # Phase 1: Install RADOS-Ceph on nodes.
        # if idx > 0 and sorted_configs[idx-1].ceph_config == nodes_config.ceph_config: # TODO: Can keep ceph running as-is, and just replace some data.
        # Note: Must make sure to use the previous ceph nodes. Due to changing spark reservation sizes, this is now not the case.
        if not rados_deploy.install(metareserve.Reservation(ceph_nodes), key_path=config.key_path, silent=config.ceph_silent or config.silent, cores=config.ceph_compile_cores):
            printe('Could not install RADOS-Ceph (iteration {}/{})'.format(idx+1, len(configs)))
            return False

        if not rados_deploy.start(metareserve.Reservation(ceph_nodes), key_path=config.key_path, mountpoint_path=config.ceph_mountpoint_path, silent=config.ceph_silent or config.silent):
            printe('Could not start RADOS-Ceph (iteration {}/{})'.format(idx+1, len(configs)))
            return False

        # Phase 2: Generate and deploy data on RADOS-Ceph cluster.
        state, data_path = data.generate(data_generator_name, dest=loc.data_generation_dir(), stripe=config.stripe)
        if not state:
            printe('Could not generate data using generator named "{}", destination: {} (iteration {}/{})'.format(data_generator_name, loc.data_generation_dir(), idx+1, len(configs)))
            return False 

        if not rados_deploy.deploy(metareserve.Reservation(ceph_nodes), paths=[data_path], key_path=config.key_path, stripe=config.stripe, multiplier=config.data_multiplier, mountpoint_path=config.ceph_mountpoint_path, silent=config.ceph_silent or config.silent):
            printe('Data deployment on RADOS-Ceph failed (iteration {}/{})'.format(idx+1, len(configs)))
            return False

        if not experiment.on_start(config, spark_nodes, ceph_nodes, num_experiment, amount_experiments):
            # TODO: Some computation should happen here.
            prints('Super hardcore computation completed! (iteration {}/{})'.format(idx+1, len(configs)))
        else:
            printw('Cancelled experiment {}/{}...'.format(idx+1, len(configs)))


        experiment.on_stop(config, spark_nodes, ceph_nodes, num_experiment, amount_experiments)

        if not rados_deploy.stop(metareserve.Reservation(ceph_nodes), key_path=config.key_path, mountpoint_path=config.ceph_mountpoint_path, silent=config.ceph_silent or config.silent):
            printe('Could not stop RADOS-Ceph deployment (iteration {}/{})'.format(idx+1, len(configs)))
            return False

        if not spark_deploy.stop(metareserve.Reservation(spark_nodes), key_path=config.key_path, worker_workdir=config.spark_workdir, silent=config.spark_silent or config.silent):
            printe('Could not stop Spark deployment (iteration {}/{})'.format(idx+1, len(configs)))
            return False

        break # Test completion. TODO: Remove
    return True