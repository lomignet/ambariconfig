#!/usr/bin/env python3

"""
Talk to ambari to get information and suggest configuration.
"""
import pprint

from hadoopSettings.ambariApi import Api
from hadoopSettings.config import Config
from hadoopSettings.compute import Compute


config = Config()
api = Api(config)
c = Compute(config, api)
pp = pprint.PrettyPrinter(indent=2)

# Put all output in one array to display it in one go at the end to prevent
# interseding debug statement with useful ouput.

info = []


# shortucts
def s(pset, config, expected, description=None):
    """string"""
    info.append(c.expects(pset, config, expected, description))


def i(pset, config, expected, description=None):
    """int"""
    info.append(c.expects_int(pset, config, expected, description))


def xmx(pset, config, expected, description=None):
    """xmx"""
    info.append(c.expects_xmx(pset, config, expected, description))


def b(pset, config, expected, description=None):
    """boolean"""
    info.append(c.expects_bool(pset, config, expected, description))


def fyi(expected, description=None):
    info.append(c.fyi(expected, description))


def fyis(description):
    """FYI witha direct string"""
    info.append(c.fyis(description))


# Values used in multiple places
# ramPerContainer = int(c.ramPerContainer() / c.MB)
minContainerSize = int(c.minContainerSize() / c.MB)
dns = api.getDNInfo()
# nbDns = len(dns)
availableCores = min([dns[dn]['cpu'] for dn in dns.keys()]) - 1
tezContainerSize = 2 * minContainerSize * 2  # *2 for test

qcapacity = c.qcapacity()
yarnMemPerNode = c.yarnMemPerNode()


# llapContainerSize = tezContainerSize  # TODO: see https://community.hortonworks.com/questions/84636/llap-not-using-io-cache.html


info.append('')
info.append('Basic info')
fyi(
    pp.pformat(dns),
    "Data nodes."
)
fyi(
    pp.pformat(api.getTotalDNResources()),
    "Total cluster resources."
)
fyi(
    minContainerSize,
    'Min container size (MB), based on amount of ram/cpu in the cluster.'
)
fyi(
    c.numContainers(),
    'Number of containers based on recommendations.'
)
fyi(
    c.qcapacity(),
    'Default queue capacity.'
)


info.append('\nYarn config.')
i(
    'yarn-site',
    'yarn.nodemanager.resource.memory-mb',
    yarnMemPerNode / c.MB,
    "min(yarn memory for one DN) * 0.75."
)
i(
    'yarn-site',
    'yarn.scheduler.minimum-allocation-mb',
    minContainerSize,
    "Min container size."
)
i(
    'yarn-site',
    'yarn.scheduler.maximum-allocation-mb',
    yarnMemPerNode / c.MB,
    "Same as yarn.nodemanager.resource.memory-mb"
)
i(
    'yarn-site',
    'yarn.nodemanager.resource.cpu-vcores',
    availableCores,
    'Assuming the cluster in yarn only. Total cores per node -1'
)
i(
    'yarn-site',
    'yarn.scheduler.maximum-allocation-vcores',
    availableCores,
    'Assuming the cluster in yarn only. Total cores per node -1'
)
s(
    'capacity-scheduler',
    'yarn.scheduler.capacity.resource-calculator',
    'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator',
    'Take all resources in account, not only RAM'
)


info.append('Map/reduce config')
i(
    'mapred-site',
    'mapreduce.map.memory.mb',
    minContainerSize,
    "Min container size"
)
i(
    'mapred-site',
    'mapreduce.reduce.memory.mb',
    2 * minContainerSize,
    "2 * min container size"
)

xmx(
    'mapred-site',
    'mapreduce.map.java.opts',
    0.8 * minContainerSize,
    "0.8 * min container size"
)

xmx(
    'mapred-site',
    'mapreduce.reduce.java.opts',
    0.8 * 2 * minContainerSize,
    "0.8 * mapreduce.reduce.memory.mb"
)
i(
    'mapred-site',
    'yarn.app.mapreduce.am.resource.mb',
    2 * minContainerSize,
    "2 * min container size"
)
xmx(
    'mapred-site',
    'yarn.app.mapreduce.am.command-opts',
    0.8 * 2 * minContainerSize,
    "0.8 * yarn.app.mapreduce.am.resource.mb"
)
i(
    'mapred-site',
    'mapreduce.task.io.sort.mb',
    0.4 * minContainerSize,
    '0.4 * min container size'
)


info.append("\nHive and Tez configuration")
s(
    "hive-site",
    'hive.execution.engine',
    'tez',
    'Use Tez, not map/reduce.'
)
b(
    'hive-site',
    'hive.server2.enable.doAs',
    'false',
    'All queries will run as Hive user, allowing resource sharing/reuse.'
)
b(
    'hive-site',
    'hive.optimize.index.filter',
    'true',
    'This optimizes "select statement with where clause" on ORC tables',
)
s(
    'hive-site',
    'hive.fetch.task.conversion',
    'more',
    'This optimizes "select statement with limit clause;"',
)
b(
    'hive-site',
    'hive.compute.query.using.stats',
    'true',
    'This optimizes "select count (1) from table;" ',
)
for v in [
    'hive.vectorized.execution.enabled',
    'hive.vectorized.execution.reduce.enabled'
]:
    b(
        'hive-site',
        v,
        'true',
        'Perform operations in batch instead of single row',
    )

b(
    'hive-site',
    'hive.cbo.enable',
    'true',
    'Enable CBO. You still need to prepare it by using the analyse HQL command.',
)
for v in [
    'hive.compute.query.using.stats',
    'hive.stats.fetch.column.stats',
    'hive.stats.fetch.partition.stats',
    'hive.stats.autogather'
]:
    b(
        'hive-site',
        v,
        'true',
        'Use CBO.',
    )
s(
    'hive-site',
    'hive.server2.tez.default.queues',
    lambda x: config.queue in x,
    'Must contain the queue name'
)
b(
    'hive-site',
    'hive.tez.dynamic.partition.pruning',
    'true',
    'Make sure tez can prune whole partitions'
)
b(
    'hive-site',
    'hive.exec.parallel',
    'true',
    'Can Hive subqueries be executed in parallel',
)
b(
    'hive-site',
    'hive.auto.convert.join',
    'true',
    'use map joins as much as possible',
)
b(
    'hive-site',
    'hive.auto.convert.join.noconditionaltask',
    'true',
    'Use map joins for small datasets',
)
s(
    'hive-site',
    'hive.tez.container.size',
    tezContainerSize,
    'Multiple of min container size.'
)
i(
    'hive-site',
    'hive.auto.convert.join.noconditionaltask.size',
    0.33 * tezContainerSize * c.MB,
    'Threshold to perform map join. 1/3 * hive.tez.container.size.'
)
i(
    'hive-site',
    'hive.vectorized.groupby.maxentries',
    10240,
    'Reduces execution time on small datasets, but also OK for large ones.'
)
s(
    'hive-site',
    'hive.vectorized.groupby.flush.percent',
    '0.1',
    'Reduces execution time on small datasets, but also OK for large ones.'
)


b(
    'hive-site',
    'hive.server2.tez.initialize.default.sessions',
    'true',
    'Enable tez use without session pool if requested',
)
i(
    'hive-site',
    'hive.server2.tez.sessions.per.default.queue',
    3,
    'Number of parallel execution inside one queue.'
)

# TODO: queue
# get name
# get am %: 35
# get scheduler: fair


info.append("\nHive and Tez memory")
i(
    'tez-site',
    'tez.am.resource.memory.mb',
    minContainerSize,
    'Appmaster memory == min container size.'
)
b(
    'tez-site',
    'tez.am.container.reuse.enabled',
    'true',
    'Reuse tez containers to prevent reallocation.'
)

s(
    'tez-site',
    'tez.container.max.java.heap.fraction',
    '0.8',
    'default % of memory used for java opts',
)
i(
    'tez-site',
    'tez.runtime.io.sort.mb',
    0.25 * tezContainerSize,
    'memory when the output needs to be sorted. == 0.25 * tezContainerSize (up to 40%)'
)
i(
    'tez-site',
    'tez.runtime.unordered.output.buffer.size-mb',
    0.075 * tezContainerSize,
    'Memory when the output does not need to be sorted. 0.075 * hive.tez.container.size (up to 10%).'
)
i(
    'tez-site',
    'tez.task.resource.memory.mb',
    minContainerSize,
    'Mem to be used by launched taks. == min container size. Overriden by hive to hive.tez.container.size anyway.'
)
xmx(
    'tez-site',
    'tez.task.launch.cmd-opts',
    0.8 * minContainerSize,
    'xmx = 0.8 * minContainerSize'
)
xmx(
    'hive-site',
    'hive.tez.java.opts',
    0.8 * tezContainerSize,
    'xmx = 0.8 * tezContainerSize'
)

b(
    'hive-site',
    'hive.prewarm.enabled',
    'true',
    'Enable prewarm to reduce latency',
)
s(
    'hive-site',
    'hive.prewarm.numcontainers',
    lambda x: x >= 1,
    'Hold containers to reduce latency, >= 1',
)

i(
    'tez-site',
    'tez.session.am.dag.submit.timeout.secs',
    300,
    'Tez Application Master waits for a DAG to be submitted before shutting down. Only useful when reuse is enabled.',
)
i(
    'tez-site',
    'tez.am.container.idle.release-timeout-min.millis',
    10000,
    'Tez container min wait before shutting down. Should give enough time to an app to send the next query',
)
i(
    'tez-site',
    'tez.am.container.idle.release-timeout-max.millis',
    20000,
    'Tez container min wait before shutting down',
)
s(
    'tez-site',
    'tez.am.view-acls',
    '*',
    'Enable tz ui access'
)
s(
    'yarn-site',
    'yarn.timeline-service.entity-group-fs-store.group-id-plugin-classes',
    'org.apache.tez.dag.history.logging.ats.TimelineCachePluginImpl',
    'Set up tez UI'
)
s(
    'mapred-site',
    'mapreduce.job.acl-view-job',
    '*',
    'Enable tez ui for mapred jobs'
)

info.append("\nCompress all")
b('mapred-site', 'mapreduce.map.output.compress', 'true')
b('mapred-site', 'mapreduce.output.fileoutputformat.compress', 'true')
b('hive-site', 'hive.exec.compress.intermediate', 'true')
b('hive-site', 'hive.exec.compress.output', 'true')

info.append(
    "\nQueue configuration. Assuming queue {q} is subqueue from root. "
    "Note that undefined values are inherited from parent.".format(q=config.queue)
)
s(
    'capacity-scheduler',
    'yarn.scheduler.capacity.root.{q}.maximum-am-resource-percent'.format(q=config.queue),
    lambda x: x != 'NOT FOUND' and float(x) >= 0.2,
    'How much of the Q the AM can use. Must be at least 0.2.'
)
s(
    'capacity-scheduler',
    'yarn.scheduler.capacity.root.{q}.ordering-policy'.format(q=config.queue),
    'fair',
    'Helps small queries get a chunk of time between big ones',
)
s(
    'capacity-scheduler',
    'yarn.scheduler.capacity.root.{q}.user-limit-factor'.format(q=config.queue),
    lambda x: x != 'NOT FOUND' and int(x) >= 1,
    'How much of the Q capacity the user can exceed if enough resources. Should be at leat 1. 1=100%, 2=200%...',
)
s(
    'capacity-scheduler',
    'yarn.scheduler.capacity.root.{q}.minimum-user-limit-percent'.format(q=config.queue),
    lambda x: x != 'NOT FOUND' and int(x) >= 10,
    'How much of the Q in percent a user is guaranteed to get. Should be at least 10',
)

info.append("\nRandom stuff")
b('hdfs-site', 'dfs.client.use.datanode.hostname', 'true', "For AWS only")


info.append("\nLLAP")
if config.llap:
    info.append("\nThose configs are probably not good (yet). Need some revision.")
    b(
        'hive-interactive-env',
        'enable_hive_interactive',
        'true',
        'Enable LLAP'
    )
    b(
        'yarn-site',
        'yarn.resourcemanager.scheduler.monitor.enable',
        'true',
        'mandatory for LLAP'
    )
    s(
        'hive-site',
        'hive.server2.tez.sessions.per.default.queue',
        lambda x: x > 0,
        '> 0'
    )
    s(
        'hive-interactive-site',
        'hive.llap.daemon.queue.name',
        lambda x: x != 'default',
        "Not 'default'"
    )
    b(
        'hive-interactive-site',
        'hive.llap.io.enabled',
        'true',
        'Big performance improvement'
    )
    i(
        'tez-interactive-site',
        'tez.am.resource.memory.mb',
        ramPerContainer,
        'Appmaster memory. == ramPerContainer (for tez/llap)'
    )
    s(
        'hive-interactive-site',
        'hive.llap.io.threadpool.size',
        availableCores // nbDns,
        'number of IO threads, == to # of available cores per node.'
    )
    i(
        'hive-interactive-env',
        'num_llap_nodes',
        nbDns,
        'Number of nodes used for llap. # of actual DNs.'
    )
    i(
        'hive-interactive-env',
        'num_llap_nodes_for_llap_daemons',
        nbDns,
        'Number of nodes used for llap. # of actual DNs.'
    )
    i(
        'hive-interactive-site',
        'hive.llap.daemon.num.executors',
        availableCores // nbDns,
        'Number of fragment a single llap daemon can run. # of available core for this node.'
    )
    s(
        'hive-interactive-site',
        'hive.llap.io.memory.mode',
        lambda x: x in ('', 'cache'),
        "Must be empty or 'cache' (default) to use off heap cache which is assumed for other computations."
    )
    i(
        'hive-interactive-site',
        'hive.llap.daemon.yarn.container.mb',
        llapContainerSize,
        '== tezcontainersize '
    )
    i(
        'hive-interactive-site',
        'hive.llap.io.memory.size',
        0.2 * llapContainerSize,
        'Cache per daemon: 0.2 * memoryPerDaemon'
    )

    i(
        'hive-interactive-env',
        'llap_heap_size',
        0.8 * llapContainerSize - 500,
        'Heap per daemon. '
        '0.8 * memoryPerDaemon - 500MB (headroom)'
    )
else:
    b(
        'hive-interactive-env',
        'enable_hive_interactive',
        'false',
        'Disable LLAP'
    )


fyis("""

More doc can be found at:
Memory settings:
  https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.1/bk_command-line-installation/content/determine-hdp-memory-config.html
  https://community.hortonworks.com/articles/14309/demystify-tez-tuning-step-by-step.html
Hive performance tuning:
  https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.1/bk_hive-performance-tuning/content/ch_hive-perf-tuning-intro.html
  http://pivotalhd.docs.pivotal.io/docs/performance-tuning-guide.html
  https://www.justanalytics.com/blog/hive-tez-query-optimization
llap:
  https://community.hortonworks.com/questions/84636/llap-not-using-io-cache.html
""")

print("\n".join([j for j in info if j is not None]))

if config.update:
    updates = c.do_update()
    if updates:
        print("Update done, but you need to restart the services yourself via the web UI.")
    else:
        print("There was no update that could be done.")
else:
    print("Will not update the unexpected parameters without --update.")
