from rados_deploy import Designation
from utils.printer import *

def distribute_automatic(interface):
    '''Distribution happens as follows: 
     - All nodes with a designation become ceph-nodes
        * If the manual designation is invalid, then this designation is removed 
        * Then, the node may not be a ceph-node
     - For the rest: 
        * If separated: the first x nodes become spark-nodes, the other y nodes become ceph-nodes
        * Else: the first x nodes become spark-nodes, the first y nodes become ceph-nodes
     - The configured designations are distributed over the un-designated ceph-nodes
    Args:
        interface (ExecutionInterface): Interface this function is registered for. Used to get the config.

    Required config args:
        node config (NodeConfiguration): Node configuration to distribute nodes for.

    Returns:
        `True`, `dict(name, list(metareserve.Node))` on success, `False`, `None` on failure.'''

    reservation_nodes = list(interface.reservation.nodes)
    config = interface.config
    node_config = config.node_config
    ceph_config = node_config.ceph_config
    num_spark_nodes = node_config.num_spark_nodes
    num_ceph_nodes = node_config.num_ceph_nodes
    separated = node_config.separated

    if not separated and len(reservation_nodes) < max(num_ceph_nodes, num_spark_nodes):
        printe('Not enough nodes reserved to satisfy largest experiment configuration. Have {} nodes, need {} ({} Spark nodes, {} Ceph nodes)'.format(reserved_nodes_len, len(max_node_conf), max_node_conf.num_spark_nodes, max_node_conf.num_ceph_nodes))
        return False, None

    if separated and len(reservation_nodes) < num_spark_nodes+num_ceph_nodes:
        printe('Not enough nodes reserved to satisfy largest experiment configuration. Have {} nodes, need {} ({} Spark nodes + {} Ceph nodes)'.format(reserved_nodes_len, len(max_node_conf), max_node_conf.num_spark_nodes, max_node_conf.num_ceph_nodes))
        return False, None

    rados_ceph_nodes = []

    # Determine manually set designations
    designations = ceph_config.designations
    for node in reservation_nodes:
        if 'designations' in node.extra_info:
            manual_designations = [Designation[y.strip().upper()].name for y in node.extra_info['designations'].split(',')]
            if manual_designations not in designations:
                printw('designation ({}) not in experiment-designations. Removing manual designation'.format(node.extra_info['designations']))
                del node.extra_info['designations']
            else:
                designations.remove(manual_designations)
                rados_ceph_nodes.append(node)
                num_ceph_nodes -= 1

    # Remove the ceph-nodes from the reservation
    if separated:
        reservation_nodes = [node for node in reservation_nodes if node not in rados_ceph_nodes]

    # Distribute Spark-nodes
    spark_nodes = reservation_nodes[:num_spark_nodes]

    # Distribute Ceph-nodes and provide designation 
    if separated:
        for node, designation in zip(reservation_nodes[num_spark_nodes:num_ceph_nodes+num_spark_nodes], designations):
            node.extra_info['designations'] = ','.join(x.name.lower() for x in designation)
            rados_ceph_nodes.append(node)
    else:
        reservation_nodes = [node for node in reservation_nodes if node not in rados_ceph_nodes]
        for node, designation in zip(reservation_nodes[:num_ceph_nodes], designations):
            node.extra_info['designations'] = ','.join(x.name.lower() for x in designation)
            rados_ceph_nodes.append(node)

    return True, {'spark': spark_nodes, 'rados_ceph': rados_ceph_nodes}



def distribute_default(interface):
    '''Most simple node distributor possible: 
            - if separted: First x nodes are for Ceph, next y nodes are for Spark. No overlap.
            - else: First x nodes are for Ceph, first y nodes are for Spark. No overlap
        Overwrites manual designation
    Args:
        interface (ExecutionInterface): Interface this function is registered for. Used to get the config.

    Required config args:
        node config (NodeConfiguration): Node configuration to distribute nodes for.

    Returns:
        `True`, `dict(name, list(metareserve.Node))` on success, `False`, `None` on failure.'''
    reservation_nodes = list(interface.reservation.nodes)
    config = interface.config
    node_config = config.node_config
    num_spark_nodes = node_config.num_spark_nodes
    num_ceph_nodes = node_config.num_ceph_nodes
    separated = node_config.separated

    if not separated and len(reservation_nodes) < max(num_ceph_nodes, num_spark_nodes):
        printe('Not enough nodes reserved to satisfy largest experiment configuration. Have {} nodes, need {} ({} Spark nodes, {} Ceph nodes)'.format(reserved_nodes_len, len(max_node_conf), max_node_conf.num_spark_nodes, max_node_conf.num_ceph_nodes))
        return False, None

    if separated and len(reservation_nodes) < num_spark_nodes+num_ceph_nodes:
        printe('Not enough nodes reserved to satisfy largest experiment configuration. Have {} nodes, need {} ({} Spark nodes + {} Ceph nodes)'.format(reserved_nodes_len, len(max_node_conf), max_node_conf.num_spark_nodes, max_node_conf.num_ceph_nodes))
        return False, None

    rados_ceph_nodes = reservation_nodes[:num_ceph_nodes]
    spark_nodes = reservation_nodes[:num_spark_nodes]

    # overwrite manual designation
    for node, designation in zip(rados_ceph_nodes, node_config.ceph_config.designations):
        node.extra_info['designations'] = ','.join(x.name.lower() for x in designation)

    return True, {'spark': spark_nodes, 'rados_ceph': rados_ceph_nodes}