import remoto
import concurrent.futures

from experimenter.internal.experiment.blocker import BlockState


def remote_file_find(spark_connectionwrappers, file):
    '''Simple method to find a file on a remote node.
    Args:
        spark_connectionwrappers (dict(metareserve.Node, experimenter.internal.remoto.RemotoSSHWrapper)): Dictionary mapping a node to an opened and ready connectionwrapper.
        files (str): Filepath to find.

    Returns:
        (BlockState, id). `BlockState.COMPLETE` is issued when we found the node containing `file`, with id the id of the node.
                          `BlockSTATE.BUSY` is returned when no node contained `file` at this time, and the id returned is -1.'''
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(spark_connectionwrappers)) as executor:
        futures_locate = {node: executor.submit(remoto.process.check, wrapper.connection, 'ls {}'.format(file), shell=True) for node, wrapper in spark_connectionwrappers.items()}
        results = {node: x.result()[2] for node, x in futures_locate.items()}
    if any(val == 0 for val in results.values()):
        driver_node_id = next(key.node_id for key, val in results.items() if val == 0)
        return BlockState.COMPLETE, driver_node_id
    return BlockState.BUSY, -1


def remote_count_lines(connection, file, needed_lines, silent):
    '''Method to count lines on a file on a remote node.
    Args:
        connection (remoto.Connection): Connection to remote.
        files (str): Filepath to read lines from.
        needed_lines (int): Number of lines we need to return a `BlockState.COMPLETE`.
        silent (bool): If set, we don't print. Otherwise, we print the amount of found lines.

    Returns:
        (BlockState, id). Returns `BlockState.COMPLETE` when the file contained enough lines, along with the number of lines.
                          Returns `BlockState.BUSY` when the file did not contain enough lines, along with the number of lines.
    '''
    out, err, exitcode = remoto.process.check(connection, 'cat {}'.format(file), shell=True)
    num_lines = len(out)
    if not silent:
        print('Found {}/{} lines'.format(num_lines, needed_lines))
    if num_lines >= needed_lines:
        return BlockState.COMPLETE, num_lines
    return BlockState.BUSY, num_lines