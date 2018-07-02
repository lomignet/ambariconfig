import functools
import json
import logging
import pprint
import requests
import time

from hadoopSettings.exceptions import (ClusterNotFound, AmbariNotReachable)

pp = pprint.PrettyPrinter(indent=2)


class Api():

    def __init__(self, config):
        self.config = config
        # Needs to be set once.
        self.setClusterName()

    def setClusterName(self):
        """
        If cluster name is not given in cli parameter, it can be
        found by querying ambari.
        If Ambari manages one and only one cluster its name is set.
        Otherwise bails out.
        """
        if self.config.cluster is None:
            clusters = self.call('/clusters', cluster=False)

            if len(clusters['items']) != 1:
                raise ClusterNotFound(
                    "None or more than one cluster managed by ambari. Api call returned:\n{}"
                    .format(clusters)
                )
            else:
                self.config.cluster = clusters['items'][0]['Clusters']['cluster_name']

    # This lru_cache is moslty to not pollute debug
    @functools.lru_cache(maxsize=128)
    def getConfigValue(self, pset, key):
        """
        Return the value of the config `key` from property set `pset`
        """
        tag = self.getTagFor(pset)
        confurl = '/configurations?type={type}&tag={tag}'.format(
            type=pset,
            tag=tag
        )
        configs = self.call(confurl)
        try:
            v = configs['items'][0]['properties'][key]
        except KeyError:
            logging.error(
                "Could not find {}/{}. Still returns 0 to carry on with script."
                .format(pset, key)
            )
            return 'NOT FOUND'

        try:
            # If we have a number, cast it
            v = int(v)
        except ValueError:
            pass

        logging.info("{p}/{k}={v}".format(p=pset, k=key, v=v))
        return v

    def getTagFor(self, pset):
        """
        Get latest config version for property set pset.
        """
        tags = self.call('?fields=Clusters/desired_configs')
        return tags['Clusters']['desired_configs'][pset]['tag']

    @functools.lru_cache(maxsize=128)
    def call(self, path, cluster=True):
        """
        Call path in param, returns json'ised object.
        If cluster=True, prefixes path with /clusters/:cluster. Most calls want that.
        """
        url = "{u}{c}{p}".format(
            u=self.config.url,
            c='/clusters/' + self.config.cluster if cluster else '',
            p=path
        )
        headers = {
            'X-Requested-By': 'ambari'
        }
        auth = requests.auth.HTTPBasicAuth(self.config.user, self.config.pwd)

        try:
            r = requests.get(url, headers=headers, auth=auth)
        except requests.exceptions.ConnectionError as e:
            raise AmbariNotReachable("Could not connect to {u}: {e}".format(
                u=self.config.url,
                e=e
            ))

        jsonresp = r.json()
        if self.config.apiLogging:
            logging.debug(pp.pformat(jsonresp))
        return jsonresp

    def put(self, path, data, cluster=True):
        """
        """
        url = "{u}{c}{p}".format(
            u=self.config.url,
            c='/clusters/' + self.config.cluster if cluster else '',
            p=path
        )
        headers = {
            'X-Requested-By': 'ambari',
        }
        auth = requests.auth.HTTPBasicAuth(self.config.user, self.config.pwd)
        try:
            r = requests.put(url, headers=headers, auth=auth, data=json.dumps(data))
        except requests.exceptions.ConnectionError as e:
            raise AmbariNotReachable("Could not connect to {u}: {e}".format(
                u=self.config.url,
                e=e
            ))
        # Will exception out on non success.
        # Note success is loosely defined: passing a string which can be parsed
        # as valid complete JSON up to a point will give a 200 code, even if the
        # data cannot be used to do an actual update, or if there is rubbish after
        # a complete json (ie. "[]boom" is seen as valid).
        r.raise_for_status()

    def getDNInfo(self):
        """
        Count memory and cpu of DATANODES only.

        returns a dict of hostnames => {cpu, mem}
        """
        hosts = [
            x['HostRoles']['host_name']
            for x in self.call('/components/DATANODE')['host_components']
            if x['HostRoles']['cluster_name'] == self.config.cluster
        ]
        # I am trying very hard to be nice on the reviewer and
        # not nest list comprehensions.
        info = {}
        for h in hosts:
            host = self.call('/hosts/{h}'.format(h=h))
            info[h] = {
                'cpu': host['Hosts']['cpu_count'],
                'mem': host['Hosts']['total_mem'] * 1024  # In bloody kb!!
            }
        return info

    def getTotalDNResources(self):
        """
        Returns dict {mem, cpu} for all DNs.
        """
        totals = functools.reduce(
            lambda x, y: {
                'mem': x['mem'] + y['mem'],
                'cpu': x['cpu'] + y['cpu'],
            },
            self.getDNInfo().values(),
            {'mem': 0, 'cpu': 0}
        )
        totals['disk'] = len(self.getConfigValue("hdfs-site", "dfs.datanode.data.dir").split(',')) * len(self.getDNInfo())
        self.totalMem = totals['mem']
        self.totalCPU = totals['cpu']
        return totals

    def displayHostInfo(self):
        """
        Nicely display some info about hosts.
        """
        print("DN resources:")
        pp.pprint(self.getDNInfo())
        print("Total resources:")
        pp.pprint(self.getTotalDNResources())

    def update(self, pset, configs):
        """
        Follows steps at https://cwiki.apache.org/confluence/display/AMBARI/Modify+configurations
        """
        desired_configs = self.call('?fields=Clusters/desired_configs')
        tag = desired_configs['Clusters']['desired_configs'][pset]['tag']

        current_config = self.call("/configurations?type={pset}&tag={tag}".format(
            pset=pset,
            tag=tag
        ))
        properties = current_config['items'][0]['properties']
        properties.update(configs)

        self.put('', [{
            'Clusters': {
                'desired_configs': {
                    'type': pset,
                    'tag': 'version' + str(int(time.time() * 1000000)),
                    'properties': properties,
                    'service_config_version_note': 'Updated via a wondrous script.'
                }
            }
        }])


