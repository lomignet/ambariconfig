"""
Bunch of exceptions.
"""


class ClusterNotFound(Exception):
    """
    Could not find a cluster.
    """

    def __init__(self, message):
        self.message = message


class AmbariNotReachable(Exception):
    """
    Could not connect to Ambari.
    """

    def __init__(self, message):
        self.message = message


class InvalidValue(Exception):
    """
    Type not expected.
    """

    def __init__(self, message):
        self.message = message
