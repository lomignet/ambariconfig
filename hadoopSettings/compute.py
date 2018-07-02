# lru_cache will be used only to prevent printing out many times the
# same debugging message.
from functools import lru_cache
import inspect
import logging
import math
import pprint
import re

from hadoopSettings.exceptions import (InvalidValue)

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

pp = pprint.PrettyPrinter(indent=2)

class bcolor(object):
    """Backgroud colors"""
    OK_COL = '\033[92m'
    NOK_COL = '\033[91m'
    NOTBAD_COL = '\033[35m'
    END_COL = '\033[0m'

    OK_CHAR = "\u2714"
    NOK_CHAR = "\u2718"
    NOTBAD_CHAR = "~"


class Compute():
    """
    Compute memory settings on so on based on
    - disk
    - cpu
    - mem
    - documentation:
        https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.0.6.0/bk_installing_manually_book/content/rpm-chap1-11.html
    """

    # Easier access to some vars.
    KB = KB
    MB = MB
    GB = GB

    api = None
    hosts = None
    totals = None

    # Remember updates to apply them all together at the end.
    # Ambari has no way to change just one setting,
    # so bundling updates per group makes sense.
    toupdate = {}
    # Some params cannot be updated.
    noupdate = {}

    def __init__(self, config, api):
        self.config = config
        self.api = api
        self.hosts = api.getDNInfo()
        self.totals = api.getTotalDNResources()
        logging.info("Total DNs: {}".format(len(self.hosts)))
        logging.info("Total Mem: {b} ({gb:.4f} GB)".format(
            b=self.totals['mem'],
            gb=self.totals['mem'] / GB
        ))
        logging.info("Total CPU: {}".format(self.totals['cpu']))
        logging.info("Total disks/server: {}".format(self.totals['disk']))

    @lru_cache(maxsize=1)
    def memPerNode(self):
        """
        Get average actual memory per node in the cluster:
        (sum memory / count nodes)
        """
        mpn = int(self.totals['mem'] / self.numDNs())
        logging.info("memPerNode: {b} ({gb:.4f} GB)".format(
            b=mpn,
            gb=mpn / GB)
        )
        return mpn

    @lru_cache(maxsize=1)
    def cpuPerNode(self):
        """
        Get average actual cpu per node in the cluster:
        (sum cpu / count nodes)
        """
        cpn = int(self.totals['cpu'] / self.numDNs())
        logging.info("cpuPerNode: {b}".format(b=cpn))
        return cpn

    @lru_cache(maxsize=1)
    def numDNs(self):
        """
        Return number of datanodes in the cluster.
        """
        n = len(self.hosts)
        logging.info("numDNs: " + str(n))
        return n

    @lru_cache(maxsize=1)
    def reservedMem(self):
        """
        Reserved memory, ie. what should NOT be used by yarn.
        Not counting HBASE.
        """
        memPerNode = self.memPerNode()

        if memPerNode <= 8 * GB:
            n = 1
        elif memPerNode <= 24 * GB:
            n = 2
        elif memPerNode <= 48 * GB:
            n = 4
        elif memPerNode <= 64 * GB:
            n = 6
        elif memPerNode <= 96 * GB:
            n = 8
        elif memPerNode <= 128 * GB:
            n = 12
        elif memPerNode <= 256 * GB:
            n = 24
        elif memPerNode <= 512 * GB:
            n = 32
        else:
            n = 64
        logging.info("reservedMem: {b} ({gb} GB)".format(
            b=n * GB,
            gb=n
        ))
        return n * GB

    def qcapacity(self):
        try:
            return int(
                self.api.getConfigValue(
                    'capacity-scheduler',
                    'yarn.scheduler.capacity.root.default.capacity'
                )
            ) / 100
        except TypeError:
            # not an int
            return 1

    @lru_cache(maxsize=1)
    def totalAvailableRam(self):
        """
        Ram available for yarn (ie. all - reserverd)
        """
        n = self.totals['mem'] - (self.numDNs() * self.reservedMem())
        logging.info("TotalAvailableRam = {b} ({gb:.4f} GB)".format(
            b=n,
            gb=n / GB
        ))
        return n

    @lru_cache(maxsize=1)
    def yarnMemPerNode(self):
        dns = self.hosts
        return int(min([dns[dn]['mem'] for dn in dns.keys()]) * 0.75)

    @lru_cache(maxsize=1)
    def minContainerSize(self):
        """
        Min allocated ram per container.
        Initial thought was:
        0.99 * self.yarnMemPerNode() / (self.cpuPerNode() - 1)
        but this lead to yarn memory 99% used, with some idle CPUs.
        """

        # Coming from hortonworks documentation.
        mem = self.yarnMemPerNode()
        if mem < 4 * GB:
            mb = 256
        elif mem < 8 * GB:
            mb = 512
        elif mem < 24 * GB:
            mb = 1024
        else:
            mb = 2048

        logging.info("minContainerSize = {b} MB".format(
            b=mb,
        ))
        return mb * MB

    @lru_cache(maxsize=1)
    def numContainers(self):
        """
        Number of total containers. Note that this is the basic for most ram
        calculations, so can be overriden by a config option.
        On a dev node, the number of containers would probably be
        2 (because 1 disk only).
        """

        if self.config.containers is not None:
            logging.info(
                "Number of containers forced to " + str(self.config.containers)
            )
            return self.config.containers

        n = math.ceil(min(
            2 * self.totals['cpu'],
            1.8 * self.totals['disk'],
            self.totalAvailableRam() / self.minContainerSize()
        ))
        logging.info("numContainers = " + str(n))
        return n

    @lru_cache(maxsize=1)
    def ramPerContainer(self):
        n = math.floor(max(
            self.minContainerSize(),
            self.totalAvailableRam() / self.numContainers()
        ))
        logging.info("ramPerContainer = {b} ({gb:.3f} GB)".format(
            b=n,
            gb=n / GB
        ))
        return n

    def getMark(self, about):
        if about == 1:
            return bcolor.OK_COL + bcolor.OK_CHAR + bcolor.END_COL
        elif about > 0.95 and about < 1.05:
            return bcolor.NOTBAD_COL + bcolor.NOTBAD_CHAR + bcolor.END_COL
        else:
            return bcolor.NOK_COL + bcolor.NOK_CHAR + bcolor.END_COL

    def fyi(self, value, explanation):
        if self.config.tofix:
            return None
        else:
            return("FYI - {value}: {expl}".format(
                value=value,
                expl=explanation
            ))

    def fyis(self, explanation):
        if self.config.tofix:
            return None
        else:
            return("FYI - {expl}".format(
                expl=explanation
            ))

    def expects(self, pset, config, expect, explanation="", value=None, update=None):
        """
        Get live value from pset/config, compare it to expect and
        display everything nicely.

        If `value` is given in parameter, it is used instead of getting it
        live. It has probably been already gathered and somehow massaged.

        Update is the final string to use to update if expected is not usable (eg. callable).

        We can do an update if one of these is true:
        - update is not None (will use update value)
        - expect is not callable (will use expect value)

        """

        if pset is None and config is None:
            return self.fyi(expect, explanation)

        workValue = self.api.getConfigValue(pset, config) if value is None else value

        # About represents how close we are to the expected value.
        if callable(expect):
            about = 1 if expect(workValue) else 0
        elif expect is None:
            # no idea what to expect
            about = 0.99
        else:
            try:
                # Get the difference in percentage of the expected workValue.
                about = workValue / expect
            except TypeError:
                # Not a number and booleans are actually string.
                about = 1 if expect.strip() == workValue else 0

        # expect_str is a human readable display of expect
        if expect is None:
            expect_str = 'No idea'
        elif callable(expect):
            expect_str = "'{}'".format(inspect.getsource(expect).strip().rstrip(','))
        else:
            expect_str = str(expect)

        if about != 1:
            # We might need to update, see method heredoc for more info.
            if update is not None:
                self.stage_for_update(pset, config, update)
            elif not callable(expect):
                self.stage_for_update(pset, config, expect)
            else:
                self.mark_as_non_updatable(pset, config)

        # Always print unexpected data (about != 1).
        # The rest only if we do not want only data tofix.
        if about != 1 or not self.config.tofix:
            return("{check} {pset}/{config} = {value}, expects {expect} {expl} {about}".format(
                check=self.getMark(about),
                pset=pset,
                config=config,
                value=workValue,
                expect=expect_str,
                expl="({})".format(explanation) if explanation else "",
                about=" #{}%".format(int(100 * about))
            ))
        else:
            return None

    def expects_int(self, pset, config, expect, explanation=""):
        """
        Lazy shortcut when we know that the expected value is an int.
        """
        return self.expects(
            pset,
            config,
            expect if expect is None else int(expect),
            explanation
        )

    def expects_bool(self, pset, config, expect, explanation=""):
        """
        Lazy shortcut when we know that the expected value is a bool.
        """
        if expect not in (None, 'true', 'false'):
            raise InvalidValue("Expecting 'true' or 'false' for {p}/{c}, got '{e}' instead.".format(
                p=pset,
                c=config,
                e=expect
            ))

        return self.expects(
            pset,
            config,
            expect,
            explanation
        )

    def expects_xmx(self, pset, config, expect, explanation=""):
        """
        Lazy shortcut to match xmx values.
        Expect is then an int.
        """

        # cast to str to catch all cases (eg. int, None)
        value = str(self.api.getConfigValue(pset, config))
        pattern = '-Xmx(\d+)m'
        matches = re.search(pattern, value)
        if matches:
            # If we need to update, we only need to replace the xmx value, and keep the rest as is.
            if expect is None:
                updateval = None
            else:
                updateval = re.sub(
                    pattern,
                    lambda m: '-Xmx{}m'.format(int(expect)),
                    value
                )

            return self.expects(
                pset,
                config,
                expect if expect is None else int(expect),
                explanation,
                value=int(matches.group(1)),
                update=updateval
            )
        else:
            logging.error(
                "Could not extract an Xmx value from {v} from {p}/{c}"
                .format(v=value, p=pset, c=config)
            )
            return self.expects(
                pset,
                config,
                expect if expect is None else int(expect),
                explanation
            )

    def stage_for_update(self, pset, config, value):
        """
        Remember what needs to be updated.
        """
        if pset not in self.toupdate:
            self.toupdate[pset] = {}

        self.toupdate[pset][config] = value

    def mark_as_non_updatable(self, pset, config):
        if pset not in self.noupdate:
            self.noupdate[pset] = []

        self.noupdate[pset].append(config)

    def do_update(self):

        updates = False

        if self.noupdate:
            print("Cannot update:")
            pp.pprint(self.noupdate)

        for pset, configs in self.toupdate.items():
            print("Updating {pset} with:".format(pset=pset))
            pp.pprint(configs)
            self.api.update(pset, configs)
            updates = True

        return updates
