import metareserve
import spark_deploy
import rados_deploy

import experimenter.internal.data as data
from experimenter.internal.experiment.interface import ExperimentInterface
import utils.location as loc
from utils.printer import *

def _check_reservation_size(configs, reservation):
    max_node_conf = max((x.node_config for x in configs), key=lambda x: len(x))
    reserved_nodes_len = len(reservation)
    max_node_len = len(max_node_conf)
    if reserved_nodes_len > max_node_len:
        printw('Reservation size ({} nodes) is more than we will need at most for this experiment ({} Spark nodes + {} Ceph nodes).'.format(reserved_nodes_len, max_node_conf.num_spark_nodes, max_node_conf.num_ceph_nodes))
        return True
    elif reserved_nodes_len < max_node_len:
        printe('Not enough nodes reserved to satisfy largest experiment configuration. Have {} nodes, need {} ({} Spark nodes + {} Ceph nodes'.format(reserved_nodes_len, len(max_node_conf), max_node_conf.num_spark_nodes, max_node_conf.num_ceph_nodes))
        return False
    return True
    

def execute(experiment, reservation):
    '''Execute an experiment.'''
    if not ExperimentInterface.is_experiment(experiment):
        raise ValueError('Passed value is not a valid experiment: {} (type: {}).'.format(experiment, type(experiment)))

    configs = list(experiment.get_configs())
    reservation_snapshot = list(reservation.nodes)

    if not _check_reservation_size(configs, reservation_snapshot):
        return False

    num_experiments = len(configs)
    for idx, config in enumerate(configs):
        print('Starting experiment {}/{}'.format(idx+1, num_experiments))

        node_config = config.node_config
        ceph_config = node_config.ceph_config
        num_spark_nodes = node_config.num_spark_nodes
        num_ceph_nodes = node_config.num_ceph_nodes

        ceph_nodes = reservation_snapshot[:num_ceph_nodes]
        spark_nodes = reservation_snapshot[num_ceph_nodes:num_ceph_nodes+num_spark_nodes]
        unused_nodes = reservation_snapshot[num_ceph_nodes+num_spark_nodes:] if len(reservation_snapshot) >= num_ceph_nodes+num_spark_nodes else []

        print('Ceph nodes:\n{}'.format(''.join('\t{}\n'.format(x) for x in ceph_nodes)))
        print('Total: {} nodes.\n'.format(len(ceph_nodes)))
        print('Spark nodes:\n{}'.format(''.join('\t{}\n'.format(x) for x in spark_nodes)))
        print('Total: {} nodes.\n'.format(len(spark_nodes)))
        
        if any(unused_nodes):
            printw('Currently not using {} nodes:\n{}'.format(len(unused_nodes), ''.join('\t{}\n'.format(x) for x in unused_nodes)))

        # Phase 0: Install Spark on nodes.
        # if idx > 0 and sorted_configs[idx-1].num_spark_nodes == num_spark_nodes: #TODO: can skip installation, only have to restart
        print('Installing Spark on {} nodes...'.format(len(spark_nodes)))
        if not spark_deploy.install(metareserve.Reservation(spark_nodes), key_path=config.key_path, silent=config.spark_silent or config.silent):
            printe('Could not install Spark (iteration {}/{})'.format(idx+1, num_experiments))
            return False
        print('Starting Spark on {} nodes...'.format(len(spark_nodes)))
        if not spark_deploy.stop(metareserve.Reservation(spark_nodes), key_path=config.key_path, worker_workdir=config.spark_workdir, silent=config.spark_silent or config.silent):
            printe('Could not stop Spark (iteration {}/{})'.format(idx+1, num_experiments))
            return False
        retval, spark_master_id = spark_deploy.start(metareserve.Reservation(spark_nodes), key_path=config.key_path, worker_workdir=config.spark_workdir, silent=config.spark_silent or config.silent)
        if not retval:
            printe('Could not start Spark (iteration {}/{})'.format(idx+1, num_experiments))
            return False

        # Phase 1: Assign designations to RADOS-Ceph nodes and install RADOS-Ceph.
        for node, designations in zip(ceph_nodes, ceph_config.designations):
            node.extra_info['designations'] = ','.join(x.name.lower() for x in designations)
        # if idx > 0 and sorted_configs[idx-1].ceph_config == node_config.ceph_config: # TODO: Can keep ceph running as-is, and just replace some data.
        # Note: Must make sure to use the previous ceph nodes. Due to changing spark reservation sizes, this is now not the case.
        if not rados_deploy.install(metareserve.Reservation(ceph_nodes), key_path=config.key_path, silent=config.ceph_silent or config.silent, cores=config.ceph_compile_cores):
            printe('Could not install RADOS-Ceph (iteration {}/{})'.format(idx+1, num_experiments))
            return False

        retval, ceph_admin_id = rados_deploy.start(metareserve.Reservation(ceph_nodes), key_path=config.key_path, mountpoint_path=config.ceph_mountpoint_path, silent=config.ceph_silent or config.silent)
        if not retval:
            printe('Could not start RADOS-Ceph (iteration {}/{})'.format(idx+1, num_experiments))
            return False

        # Phase 2: Generate and deploy data on RADOS-Ceph cluster.
        retval, data_path = data.generate(config.data_generator_name, dest=loc.data_generation_dir(), stripe=config.stripe)
        if not retval:
            printe('Could not generate data using generator named "{}", destination: {} (iteration {}/{})'.format(config.data_generator_name, loc.data_generation_dir(), idx+1, num_experiments))
            return False 

        if not rados_deploy.deploy(metareserve.Reservation(ceph_nodes), paths=[data_path], key_path=config.key_path, stripe=config.stripe, multiplier=config.data_multiplier, mountpoint_path=config.ceph_mountpoint_path, silent=config.ceph_silent or config.silent):
            printe('Data deployment on RADOS-Ceph failed (iteration {}/{})'.format(idx+1, num_experiments))
            return False

        if not experiment.on_start(config, spark_nodes, ceph_nodes, idx, num_experiments):
            # TODO: Some computation should happen here.
            prints('Super hardcore computation completed! (iteration {}/{})'.format(idx+1, num_experiments))
        else:
            printw('Cancelled experiment {}/{}...'.format(idx+1, num_experiments))


        experiment.on_stop(config, spark_nodes, ceph_nodes, idx, num_experiments)

        if not rados_deploy.stop(metareserve.Reservation(ceph_nodes), key_path=config.key_path, mountpoint_path=config.ceph_mountpoint_path, silent=config.ceph_silent or config.silent):
            printe('Could not stop RADOS-Ceph deployment (iteration {}/{})'.format(idx+1, num_experiments))
            return False

        if not spark_deploy.stop(metareserve.Reservation(spark_nodes), key_path=config.key_path, worker_workdir=config.spark_workdir, silent=config.spark_silent or config.silent):
            printe('Could not stop Spark deployment (iteration {}/{})'.format(idx+1, num_experiments))
            return False

        break # Test completion. TODO: Remove
    return True