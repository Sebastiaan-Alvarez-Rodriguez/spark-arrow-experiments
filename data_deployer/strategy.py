from enum import Enum


class DeployStrategy(Enum):
    STAR = 0,         # Directly distribute data from this machine to all remotes. Uses more network bandwidth, but faster iff the network does not bottleneck.
    REMOTE_BASED = 1  # First deploy to 1 node, then perform a STAR to all other nodes. Equivalent to STAR strategy if we only have 1 node.