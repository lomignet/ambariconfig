# Name

Basic Usage

./settings.py <ambari_host>

# Description

Will output on screen what the script thinks your hadoop configuration should be. You can then
add the flag `--update` to have the script update ambari. You will still need to restart
services manually.

# Caveat

Assumes that all data nodes are identical.

Does not work with LLAP (yet).

# Installation

Just git clone the repository. It should work out of the box on most (linux/unix) systems.

# Options

Just type `./ambarisettings.py --help`


    usage: settings.py [-h] [--loglevel {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                       [--no-apiLogging] [--apipath APIPATH] [--user USER]
                       [--pwd PWD] [--cluster CLUSTER] [--queue QUEUE]
                       [--containers CONTAINERS] [--port AMBARIPORT] [--tofix]
                       [--update] [--llap]
                       [ambariHost]

    Work out yarn configuration settings.

    positional arguments:
      ambariHost            Ambari host name. (default: localhost)

    optional arguments:
      -h, --help            show this help message and exit
      --loglevel {DEBUG,INFO,WARNING,ERROR,CRITICAL}, -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                            Log level. (default: WARN)
      --no-apiLogging, -n   Disable logging api calls in debug mode. (default:
                            True)
      --apipath APIPATH     Ambari api path. (default: /api/v1)
      --user USER, -u USER  Ambari user. (default: admin)
      --pwd PWD             Ambari password. (default: admin)
      --cluster CLUSTER     Cluster to look at. If None and there is only one
                            cluster managed by thisambari instance, it will be
                            used. Otherwise will complain loudly. (default: None)
      --queue QUEUE, -q QUEUE
                            Queue used by hive/tez. (default: default)
      --containers CONTAINERS, -c CONTAINERS
                            Base of all memory settings. Will be worked out by
                            default, but can be forced here. Useful for acc or
                            dev. (default: None)
      --port AMBARIPORT, -p AMBARIPORT
                            Ambari port. (default: 8080)
      --tofix, -t           Only display values to fix. (default: False)
      --update              Update the config values we can update. (default:
                            False)
      --llap, --no-llap     Configure llap. (default: False)



