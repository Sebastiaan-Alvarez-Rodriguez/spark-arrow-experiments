from enum import Enum
import time

class BlockState(Enum):
    '''Possible states.'''
    INIT = 0     # Not working yet.
    BUSY = 1     # Working.
    COMPLETE = 2 # Finished, with success.
    FAILED = 3   # Finished, with failure - internal failure.
    TIMEOUT = 4  # Finished, with failure - timeout.

class Blocker(object):
    '''Simple object to block control until Spark has finished executing.'''
    def __init__(self):
        pass

    def block(self, command, args=None, sleeptime=60, dead_after_retries=3):
        '''Blocks for given command. Command must return a `BlockState`, optionally with an additional value.
        If the 'COMPLETE' state is returned, blocking stops and we return True.
        If the 'FAILED' state is returned, blocking stops and we return False.
        If the 'BUSY' state is returned, we sleep for sleeptime seconds.
        Note: If the command returns both a BlockState and an additional value, we check the difference with the previous value.
        If the value remains unchanged after dead_after_retries retries, we assume that the application has died, and we return `False`.

        Args:
            command (function): Function to periodically call.
            args (optional tuple): Container with args to use for calling.
            sleeptime (optional int): Number of seconds to sleep between calls.
            dead_after_retries (optional int): Number of times to call the command at most before giving up.

        Returns:
            `BlockState`, which callers can use to see what return condition was met.'''
        val = None
        state = BlockState.INIT
        unchanged = 0

        while True:
            if args == None or len(args) == 0:
                tmp = command()
            else:
                tmp = command(*args)
            if len(tmp) == 2:
                state, val_cur = tmp
                if val_cur == val:
                    unchanged += 1
                else:
                    unchanged = 0
                val = val_cur
            else:
                state = tmp

            if state == BlockState.COMPLETE:
                return BlockState.COMPLETE # Completed!
            elif state == BlockState.FAILED:
                return BlockState.FAILED # User function tells we failed

            if unchanged == dead_after_retries:
                return BlockState.TIMEOUT
            time.sleep(sleeptime)


    @staticmethod
    def simple_spark_blocker(config):
        '''Steps:
         1. find the Spark node that has the log.
         2. read log, report length.
         '''
        pass
