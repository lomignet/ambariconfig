import argparse
import logging


class Config():

    # Type of log.
    loglevel = 'WARN'

    # Display output of api calls (in debug mode)
    apiLogging = True

    # Display only tofix settings
    tofix = False

    # Ambari user
    user = 'admin'

    # Ambari pwd
    pwd = 'admin'

    # queue used for hive
    queue = 'default'

    # Setup llap
    llap = False

    # Cluster name
    _cluster = None

    # Number of containers
    containers = None

    # Ambari host
    ambariHost = 'localhost'

    # Ambari port
    ambariPort = 8080

    # api path
    apiPath = '/api/v1'

    # Update config settings
    update = False

    def __init__(self):
        """
        Initialise the parser and do its magic.
        """
        parser = argparse.ArgumentParser(
            description='Work out yarn configuration settings.',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )

        parser.add_argument(
            '--loglevel', '-l',
            dest='loglevel',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            type=str.upper,
            default=self.loglevel,
            help='Log level.'
        )

        parser.add_argument(
            '--no-apiLogging', '-n',
            dest='apiLogging',
            action='store_false',
            default=self.apiLogging,
            help='Disable logging api calls in debug mode.'
        )

        parser.add_argument(
            '--apipath',
            dest='apiPath',
            type=str,
            default=self.apiPath,
            help='Ambari api path.'
        )

        parser.add_argument(
            '--user', '-u',
            dest='user',
            type=str,
            default=self.user,
            help='Ambari user.'
        )

        parser.add_argument(
            '--pwd',
            dest='pwd',
            type=str,
            default=self.pwd,
            help='Ambari password.'
        )

        parser.add_argument(
            '--cluster',
            dest='cluster',
            type=str,
            default=self.cluster,
            help='Cluster to look at. If None and there is only one cluster managed by this '
            'ambari instance, it will be used. Otherwise will complain loudly.'
        )

        parser.add_argument(
            '--queue', '-q',
            dest='queue',
            type=str,
            default=self.queue,
            help='Queue used by hive/tez.'
        )

        parser.add_argument(
            '--containers', '-c',
            dest='containers',
            type=int,
            default=self.containers,
            help='Base of all memory settings. Will be worked out by default, but '
            'can be forced here. Useful for acc or dev.'
        )

        parser.add_argument(
            '--port', '-p',
            dest='ambariPort',
            type=int,
            default=self.ambariPort,
            help='Ambari port.'
        )

        parser.add_argument(
            '--tofix', '-t',
            dest='tofix',
            action='store_true',
            default=self.tofix,
            help='Only display values to fix.'
        )

        parser.add_argument(
            '--update',
            dest='update',
            action='store_true',
            default=self.update,
            help='Update the config values we can update.'
        )

        parser.add_argument(
            '--llap', '--no-llap',
            dest='llap',
            action=BooleanAction,
            default=self.llap,
            help='Configure llap.'
        )

        # Positional
        parser.add_argument(
            dest='ambariHost',
            type=str,
            nargs='?',
            default=self.ambariHost,
            help='Ambari host name.'
        )

        parser.parse_args(namespace=self)

        self.setLogging()

    @property
    def url(self):
        return 'http://' + self.ambariHost + ':' + str(self.ambariPort) + self.apiPath

    @property
    def cluster(self):
        return self._cluster

    # Cluster could be set outside config, after an API call
    @cluster.setter
    def cluster(self, value):
        self._cluster = value

    def setLogging(self):
        """
        Set up our own logger, and make urllib3 shut up a bit.
        """
        logging.basicConfig(level=self.loglevel)
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)


class BooleanAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        super(BooleanAction, self).__init__(option_strings, dest, nargs=0, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, False if option_string.startswith('--no') else True)
