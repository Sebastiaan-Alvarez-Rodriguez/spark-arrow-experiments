from utils.printer import *

def distribute_default(interface):
    '''Most simple node distributor possible: First x nodes are for Ceph, next y nodes are for Spark. No overlap.'''
    reservation_nodes = list(interface.reservation.nodes)
    config = interface.config
    node_config = config.node_config
    num_spark_nodes = node_config.num_spark_nodes
    num_ceph_nodes = node_config.num_ceph_nodes

    if len(reservation_nodes) < num_spark_nodes+num_ceph_nodes:
        printe('Not enough nodes reserved to satisfy largest experiment configuration. Have {} nodes, need {} ({} Spark nodes + {} Ceph nodes)'.format(reserved_nodes_len, len(max_node_conf), max_node_conf.num_spark_nodes, max_node_conf.num_ceph_nodes))
        return False, None
    rados_ceph_nodes = reservation_nodes[:num_ceph_nodes]
    spark_nodes = reservation_nodes[num_ceph_nodes:num_ceph_nodes+num_spark_nodes]
    return True, {'spark': spark_nodes, 'rados_ceph': rados_ceph_nodes}