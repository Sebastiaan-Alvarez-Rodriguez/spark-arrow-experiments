import time

import metareserve
import spark_deploy
import rados_deploy

import experimenter.internal.data as data
from experimenter.internal.experiment.interface import ExperimentInterface
import utils.location as loc
from utils.printer import *

from experimenter.internal.experiment.blocker import block, BlockState


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
    

def _submit_blocking(config, command, spark_nodes):
    if not spark_deploy.submit(metareserve.Reservation(spark_nodes), command, paths=config.local_application_paths, key_path=config.key_path, master_id=spark_master_id, silent=config.spark_silent or config.silent):
        printw('Could not submit application on remote. Used command: {}'.format(command))
        time.sleep(10)
        continue

    for _try in range(config.tries):
        state = block(command, args=None, sleeptime=conf.sleeptime, dead_after_retries=conf.dead_after_tries)
        if state == BlockState.TIMEOUT:
            pass # TODO: recover
    return state == BlockState.COMPLETED


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

        experiment.on_distribute()

        ceph_nodes = reservation_snapshot[:num_ceph_nodes]
        spark_nodes = reservation_snapshot[num_ceph_nodes:num_ceph_nodes+num_spark_nodes]
        unused_nodes = reservation_snapshot[num_ceph_nodes+num_spark_nodes:] if len(reservation_snapshot) >= num_ceph_nodes+num_spark_nodes else []

        print('Ceph nodes:\n{}'.format(''.join('\t{}\n'.format(x) for x in ceph_nodes)))
        print('Total: {} nodes.\n'.format(len(ceph_nodes)))
        print('Spark nodes:\n{}'.format(''.join('\t{}\n'.format(x) for x in spark_nodes)))
        print('Total: {} nodes.\n'.format(len(spark_nodes)))
        
        if any(unused_nodes):
            printw('Currently not using {} nodes:\n{}'.format(len(unused_nodes), ''.join('\t{}\n'.format(x) for x in unused_nodes)))


        # Phase 1: Install RADOS-Ceph and Spark on nodes.
        print('Installing RADOS-Ceph and Spark on {} nodes...'.format(len(spark_nodes)))
        # Assign designations to RADOS-Ceph nodes.
        for node, designations in zip(ceph_nodes, ceph_config.designations):
            node.extra_info['designations'] = ','.join(x.name.lower() for x in designations)
        # if idx > 0 and sorted_configs[idx-1].ceph_config == node_config.ceph_config: # TODO: Can keep ceph running as-is, and just replace some data.
        # Note: Must make sure to use the previous ceph nodes. Due to changing spark reservation sizes, this is now not the case.
        if not rados_deploy.install_ssh(metareserve.Reservation(ceph_nodes+spark_nodes), key_path=config.key_path, cluster_keypair=None, silent=config.ceph_silent or config.silent):
            printe('Could not install SSH keys for internal cluster communication.')
            return False

        retval, ceph_admin_id = rados_deploy.install(metareserve.Reservation(ceph_nodes), key_path=config.key_path, silent=config.ceph_silent or config.silent, cores=config.ceph_compile_cores)
        if not retval:
            printe('Could not install RADOS-Ceph (iteration {}/{})'.format(idx+1, num_experiments))
            return False
        # if idx > 0 and sorted_configs[idx-1].num_spark_nodes == num_spark_nodes: #TODO: can skip installation, only have to restart
        experiment.on_install()
        if not spark_deploy.install(metareserve.Reservation(spark_nodes), key_path=config.key_path, silent=config.spark_silent or config.silent):
            printe('Could not install Spark (iteration {}/{})'.format(idx+1, num_experiments))
            return False


        # Phase 2: Start Spark and RADOS-Ceph on nodes.
        print('Starting RADOS-Ceph and Spark on {} nodes...'.format(len(spark_nodes)))
        retval, _ = rados_deploy.start(metareserve.Reservation(ceph_nodes+spark_nodes), key_path=config.key_path, admin_id=ceph_admin_id, mountpoint_path=config.ceph_mountpoint_path, silent=config.ceph_silent or config.silent)
        if not retval:
            printe('Could not start RADOS-Ceph (iteration {}/{})'.format(idx+1, num_experiments))
            return False

        if not spark_deploy.stop(metareserve.Reservation(spark_nodes), key_path=config.key_path, worker_workdir=config.spark_workdir, silent=config.spark_silent or config.silent):
            printe('Could not stop Spark (iteration {}/{})'.format(idx+1, num_experiments))
            return False
        retval, spark_master_id, spark_master_url = spark_deploy.start(metareserve.Reservation(spark_nodes), key_path=config.key_path, worker_workdir=config.spark_workdir, silent=config.spark_silent or config.silent)
        if not retval:
            printe('Could not start Spark (iteration {}/{})'.format(idx+1, num_experiments))
            return False


        # Phase 3: Generate and deploy data on RADOS-Ceph cluster.
        retval, num_rows = data.generate(config.data_generator_name, config.data_path, config.stripe, config.num_columns, config.data_format, extra_args=config.data_gen_extra_args, extra_kwargs=config.data_gen_extra_kwargs)
        if not retval:
            printe('Could not generate data using generator named "{}", destination: {} (iteration {}/{})'.format(config.data_generator_name, loc.data_generation_dir(), idx+1, num_experiments))
            return False 

        if not rados_deploy.deploy(metareserve.Reservation(ceph_nodes+spark_nodes), paths=[config.data_path], key_path=config.key_path, admin_id=ceph_admin_id, stripe=config.stripe, multiplier=config.data_multiplier, mountpoint_path=config.ceph_mountpoint_path, silent=config.ceph_silent or config.silent):
            printe('Data deployment on RADOS-Ceph failed (iteration {}/{})'.format(idx+1, num_experiments))
            return False

        # Phase 4: Start experiment.
        if experiment.on_start(config, spark_nodes, ceph_nodes, idx, num_experiments):
            cmd_builder = spark_deploy.SubmitCommandBuilder(cmd_type=config.spark_application_type)
            cmd_builder.set_master(spark_master_url)
            cmd_builder.set_deploymode(config.spark_deploymode)
            cmd_builder.add_java_options(*config.spark_java_options)
            cmd_builder.set_application(config.spark_application_path)
            cmd_builder.add_conf_options(*config.spark_conf_options)
            cmd_builder.set_args(config.spark_application_args)
            if config.spark_application_type == 'java':
                cmd_builder.set_class(config.spark_application_mainclass)
                cmd_builder.add_jars(*config.spark_extra_jars)
            command = cmd_builder.build()
            if _submit_blocking(config, command, spark_nodes):
                prints('Super hardcore computation completed! (iteration {}/{})'.format(idx+1, num_experiments))
            else:
                printe('Fatal error for experiment iteration {}/{}'.format(idx+1, num_experiments))

        else:
            printw('Cancelled experiment {}/{}...'.format(idx+1, num_experiments))


        # Phase 5: Stop instances.
        # experiment.on_stop(config, spark_nodes, ceph_nodes, idx, num_experiments)

        # if not spark_deploy.stop(metareserve.Reservation(spark_nodes), key_path=config.key_path, worker_workdir=config.spark_workdir, silent=config.spark_silent or config.silent):
        #     printe('Could not stop Spark deployment (iteration {}/{})'.format(idx+1, num_experiments))
        #     return False

        # if not rados_deploy.stop(metareserve.Reservation(ceph_nodes+spark_nodes), key_path=config.key_path, mountpoint_path=config.ceph_mountpoint_path, silent=config.ceph_silent or config.silent):
        #     printe('Could not stop RADOS-Ceph deployment (iteration {}/{})'.format(idx+1, num_experiments))
        #     return False

        break # Test completion. TODO: Remove

    experiment.on_end()
    return True