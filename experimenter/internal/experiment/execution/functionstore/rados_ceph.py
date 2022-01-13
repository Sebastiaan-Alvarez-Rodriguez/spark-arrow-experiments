import data_deploy
import metareserve
import rados_deploy

from utils.printer import *

def install_rados_ceph(interface, idx, num_experiments, ceph_nodes, spark_nodes):
    '''Instals Rados-Ceph on a series of nodes in parallel.
    Args:
        interface (ExecutionInterface): Interface this function is registered for. Used to get the config.
        idx (int): Experiment index.
        num_experiments (int): Amount of experiments.
        ceph_nodes (list(metareserve.Nodes)): Nodes to install ceph on.
        spark_nodes (list(metareserve.Nodes)): Spark nodes (used only to install common cluster SSH keys on).

    Required config args:
        key_path (str or None): Path to ssh key to use when connecting to cluster nodes.
        ceph_arrow_url (str): Arrow URL to download and install for Rados-Ceph.
        ceph_force_reinstall (bool): If set, reinstalls Arrow from scratch, even when already present.
        ceph_debug (bool): If set, builds with debug flags.
        ceph_compile_threads (int): Number of cores to use for compiling Arrow.
        ceph_silent (bool): Indication whether Ceph output must be suppressed.
        silent (bool): Indication whether general output must be suppressed.

    Produced config args:
        rados_ceph_admin_id (int): Node id of the node functioning as Ceph admin.

    Returns:
        `True` on success, `False` on failure.'''
    config = interface.config

    if not rados_deploy.install_ssh(metareserve.Reservation(ceph_nodes+spark_nodes), key_path=config.key_path, cluster_keypair=None, silent=config.ceph_silent or config.silent):
        printe('Could not install SSH keys for internal cluster communication (iteration {}/{})'.format(idx+1, num_experiments))
        return False
    retval, rados_ceph_admin_id = rados_deploy.install(metareserve.Reservation(ceph_nodes), key_path=config.key_path, arrow_url=config.ceph_arrow_url, force_reinstall=config.ceph_force_reinstall, debug=config.ceph_debug, silent=config.ceph_silent or config.silent, cores=config.ceph_compile_threads)
    if not retval:
        printe('Could not install RADOS-Ceph (iteration {}/{})'.format(idx+1, num_experiments))
        return False
    setattr(interface, 'rados_ceph_admin_id', rados_ceph_admin_id)
    return True


def start_rados_ceph(interface, idx, num_experiments, ceph_nodes, rados_ceph_admin_id, spark_nodes):
    '''Starts Rados-Ceph on a series of nodes in parallel.
    Args:
        interface (ExecutionInterface): Interface this function is registered for. Used to get the config.
        idx (int): Experiment index.
        num_experiments (int): Amount of experiments.
        ceph_nodes (list(metareserve.Nodes)): Nodes having installed Ceph.
        rados_ceph_admin_id (int): Node id of the node functioning as Ceph admin.
        spark_nodes (list(metareserve.Nodes)): Spark nodes (used only to deploy CephFS backend communication on).

    Required config args:
        key_path (str or None): Path to ssh key to use when connecting to cluster nodes.
        ceph_store_type (rados_deploy.StorageType): Storage backend type to use: MEMSTORE or BLUESTORE.
        ceph_mountpoint_dir (str): Location where CephFS will be mounted on all nodes.
        ceph_memstore_storage_size (str or None): If set, sets amount of bytes of RAM to allocate on each node. Value must use size indicator B, KiB, MiB, GiB, TiB.
        ceph_bluestore_path_override (str or None): If set, overrides the "device_path" extra info for all nodes with given value. Should point to device to use with bluestore on all nodes.
        osd_op_threads (int or None): Number of op threads to use for each OSD. Make sure this number is not greater than the amount of cores each OSD has.
        osd_pool_size (int or None): Fragmentation of object to given number of OSDs. Must be less than or equal to amount of OSDs.
        osd_max_obj_size (int): Maximal object size in bytes. Normal=128*1024*1024 (128MB).
        placement_groups (int or None): Amount of placement groups in Ceph. If not set, we use the recommended formula `(num osds * 100) / (pool size`, as found here: https://ceph.io/pgcalc/.
        use_client_cache (bool): Toggles using cephFS I/O cache.
        ceph_silent (bool): Indication whether Ceph output must be suppressed.
        silent (bool): Indication whether general output must be suppressed.

    Returns:
        `True` on success, `False` on failure.'''
    config = interface.config

    ceph_config = config.node_config.ceph_config
    reservation = metareserve.Reservation(ceph_nodes+spark_nodes)
    if config.ceph_store_type == rados_deploy.StorageType.MEMSTORE:
        from rados_deploy.start import memstore
        retval, _ = memstore(reservation, key_path=config.key_path, admin_id=rados_ceph_admin_id, mountpoint_path=config.ceph_mountpoint_dir, placement_groups=config.ceph_placement_groups, storage_size=config.ceph_memstore_storage_size, osd_op_threads=config.ceph_osd_op_threads, osd_pool_size=config.ceph_osd_pool_size, osd_max_obj_size=config.ceph_osd_max_obj_size, use_client_cache=config.ceph_use_client_cache, silent=config.ceph_silent or config.silent)
    else:
        from rados_deploy.start import bluestore
        retval, _ = bluestore(reservation, key_path=config.key_path, admin_id=rados_ceph_admin_id, mountpoint_path=config.ceph_mountpoint_dir, placement_groups=config.ceph_placement_groups, device_path=config.ceph_bluestore_path_override, osd_op_threads=config.ceph_osd_op_threads, osd_pool_size=config.ceph_osd_pool_size, osd_max_obj_size=config.ceph_osd_max_obj_size, use_client_cache=config.ceph_use_client_cache, silent=config.ceph_silent or config.silent)
    if not retval:
        printe('Could not start RADOS-Ceph (iteration {}/{})'.format(idx+1, num_experiments))
        return False
    return True


def stop_rados_ceph(interface, idx, num_experiments, ceph_nodes, rados_ceph_admin_id, spark_nodes):
    '''Stops Rados-Ceph on a series of nodes in parallel.
    Args:
        interface (ExecutionInterface): Interface this function is registered for. Used to get the config.
        idx (int): Experiment index.
        num_experiments (int): Amount of experiments.
        ceph_nodes (list(metareserve.Nodes)): Nodes to stop ceph for.
        rados_ceph_admin_id (int): Node id of the node functioning as Ceph admin.
        spark_nodes (list(metareserve.Nodes)): Spark nodes (used only to stop CephFS).

    Required config args:
        key_path (str or None): Path to ssh key to use when connecting to cluster nodes.
        ceph_store_type (rados_deploy.StorageType): Storage backend type to use: MEMSTORE or BLUESTORE.
        ceph_mountpoint_dir (str): Location where CephFS will be mounted on all nodes.
        ceph_silent (bool): Indication whether Ceph output must be suppressed.
        silent (bool): Indication whether general output must be suppressed.

    Returns:
        `True` on success, `False` on failure.'''
    config = interface.config

    if config.ceph_store_type == rados_deploy.StorageType.MEMSTORE:
        from rados_deploy.stop import memstore as stop_memstore
        retval = stop_memstore(metareserve.Reservation(ceph_nodes+spark_nodes), key_path=config.key_path, admin_id=rados_ceph_admin_id, mountpoint_path=config.ceph_mountpoint_dir, silent=config.ceph_silent or config.silent)
    else:
        from rados_deploy.stop import bluestore as stop_bluestore
        retval = stop_bluestore(metareserve.Reservation(ceph_nodes+spark_nodes), key_path=config.key_path, admin_id=rados_ceph_admin_id, mountpoint_path=config.ceph_mountpoint_dir, silent=config.ceph_silent or config.silent)
    if not retval:
        printe('Could not stop RADOS-Ceph deployment (iteration {}/{})'.format(idx+1, num_experiments))
        return False
    return True


def deploy_data_rados_ceph(interface, idx, num_experiments, ceph_nodes, rados_ceph_admin_id, spark_nodes):
    '''Deploy data on a Rados-Ceph cluster.
    Args:
        interface (ExecutionInterface): Interface this function is registered for. Used to get the config.
        idx (int): Experiment index.
        num_experiments (int): Amount of experiments.
        ceph_nodes (list(metareserve.Nodes)): Nodes to stop ceph for.
        rados_ceph_admin_id (int): Node id of the node functioning as Ceph admin.
        spark_nodes (list(metareserve.Nodes)): Spark nodes.

    Required config args:
        key_path (str or None): Path to ssh key to use when connecting to cluster nodes.
        data_path (str): Path to data to transmit.
        remote_data_dir (str): Data destination directory on remote.
        copy_multiplier (int): Amount of copies of each file to make on the remote.
        link_multiplier (int): Amount of hardlinks of each file to make on the remote.
        stripe (int): Object size to use.
        ceph_silent (bool): Indication whether Ceph output must be suppressed.
        silent (bool): Indication whether general output must be suppressed.

    Returns:
        `True` on success, `False` on failure.'''
    config = interface.config
    kwargs = {'admin_id': rados_ceph_admin_id, 'stripe': config.stripe}
    if not data_deploy.deploy(metareserve.Reservation(ceph_nodes+spark_nodes), key_path=config.key_path, paths=[config.data_path], dest=config.remote_data_dir, copy_multiplier=config.copy_multiplier, link_multiplier=config.link_multiplier, silent=config.ceph_silent or config.silent, plugin='rados_deploy', **kwargs):
        printe('Data deployment on RADOS-Ceph failed (iteration {}/{})'.format(idx+1, num_experiments))
        return False
    return True


def register_rados_ceph_functions(interface, idx, num_experiments, ceph_nodes=None, rados_ceph_admin_id=None, spark_nodes=None):
    '''Registers install, start and stop functions for Rados-Ceph.'''
    get_ceph_nodes = lambda iface: ceph_nodes if ceph_nodes else iface.distribution['rados_ceph']
    get_rados_ceph_admin_id = lambda iface: rados_ceph_admin_id if rados_ceph_admin_id != None else iface.rados_ceph_admin_id
    get_spark_nodes = lambda iface: spark_nodes if spark_nodes else iface.distribution['spark']

    interface.register('install_others_funcs', lambda iface: install_rados_ceph(iface, idx, num_experiments, get_ceph_nodes(iface), get_spark_nodes(iface)))
    interface.register('start_others_funcs', lambda iface: start_rados_ceph(iface, idx, num_experiments, get_ceph_nodes(iface), get_rados_ceph_admin_id(iface), get_spark_nodes(iface)))
    interface.register('stop_others_funcs', lambda iface: stop_rados_ceph(iface, idx, num_experiments, get_ceph_nodes(iface), get_rados_ceph_admin_id(iface), get_spark_nodes(iface)))


def register_rados_ceph_deploy_data(interface, idx, num_experiments, ceph_nodes=None, rados_ceph_admin_id=None, spark_nodes=None):
    '''Registers data deploy functions for Rados-Ceph.'''
    get_ceph_nodes = lambda iface: ceph_nodes if ceph_nodes else iface.distribution['rados_ceph']
    get_rados_ceph_admin_id = lambda iface: rados_ceph_admin_id if rados_ceph_admin_id != None else iface.rados_ceph_admin_id
    get_spark_nodes = lambda iface: spark_nodes if spark_nodes else iface.distribution['spark']

    interface.register('deploy_data_func', lambda iface: deploy_data_rados_ceph(iface, idx, num_experiments, get_ceph_nodes(iface), get_rados_ceph_admin_id(iface), get_spark_nodes(iface)))