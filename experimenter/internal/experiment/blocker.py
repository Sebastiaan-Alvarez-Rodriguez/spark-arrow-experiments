import concurrent.futures
from enum import Enum
import time

class BlockState(Enum):
    '''Possible states.'''
    INIT = 0     # Not working yet.
    BUSY = 1     # Working.
    COMPLETE = 2 # Finished, with success.
    FAILED = 3   # Finished, with failure - internal failure.
    TIMEOUT = 4  # Finished, with failure - timeout.



def block(command, args=None, sleeptime=60, dead_after_tries=3):
    '''Blocks for given command. Command must return a `BlockState`.
    If the 'BUSY' state is returned, we sleep for sleeptime seconds. Any other state is seen as a final state, and makes the function return.
    Args:
        command (function): Function to periodically call.
        args (optional tuple): Container with args to use for calling.
        sleeptime (optional int): Number of seconds to sleep between calls.
        dead_after_tries (optional int): Number of times we call the command at most before giving up.

    Returns:
        `BlockState`, which callers can use to see what return condition was met.'''
    state = BlockState.INIT

    for x in range(dead_after_tries):
        tmp = command(*args) if args else command()
        state = tmp

        if state == BlockState.COMPLETE or state == BlockState.FAILED:
            return state
        time.sleep(sleeptime)

    return BlockState.TIMEOUT


def block_with_value(command, args=None, sleeptime=60, dead_after_tries=3, return_val=False):
    '''Blocks for given command. Command must return a `BlockState`, and another series of values.
    If the 'BUSY' state is returned, we sleep for sleeptime seconds. 'COMPLETED' and 'FAILED' are seen as final states, which make the function return.
    Note: If the command returns both a BlockState and an additional value, we check the difference with the previous value.
    We return when the value remains unchanged after `dead_after_tries` retries.

    Args:
        command (function): Function to periodically call.
        args (optional tuple): Container with args to use for calling.
        sleeptime (optional int): Number of seconds to sleep between calls.
        dead_after_tries (optional int): Number of times we call the command + getting the same returnvalue before giving up. Note: If the returnvalue changes once, the counter is reset.
        return_val (optional bool): If set, returns last encountered value along with final state. Otherwise, only returns final state.

    Returns:
        `(BlockState, command returnvalue)` if `return_val` is set, `BlockState` otherwise. Callers can use the BlockState to see what return condition was met.'''
    val_stored = None
    state = BlockState.INIT
    unchanged = 0

    while unchanged < dead_after_tries:
        tmp = command(*args) if args else command()
        
        state, *val = tmp
        if val == val_stored:
            unchanged += 1
        else:
            unchanged = 0
        val_stored = val

        if state == BlockState.COMPLETE or state == BlockState.FAILED:
            return state, val if return_val else state
        time.sleep(sleeptime)

    return BlockState.TIMEOUT, val if return_val else BlockState.TIMEOUT