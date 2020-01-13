#!/usr/bin/env python3

import os
import subprocess
import re
import sys
sys.path.append('lib')

from ops.charm import CharmBase, CharmEvents
from ops.framework import EventBase, EventSource, StoredState
from ops.main import main

from jinja import Environment, FileSystemLoader
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    WaitingStatus,
)

from interfaces import CockroachDBPeers


class CockroachStartedEvent(EventBase):
    pass


class ClusterInitializedEvent(EventBase):
    def __init__(self, handle, cluster_id):
        super().__init__(handle)
        self.cluster_id = cluster_id

    def snapshot(self):
        return self.cluster_id

    def restore(self, cluster_id):
        self.cluster_id = cluster_id


class CockroachDBCharmEvents(CharmEvents):
    cockroachdb_started = EventSource(CockroachStartedEvent)
    cluster_initialized = EventSource(ClusterInitializedEvent)


class CockroachDBCharm(CharmBase):
    on = CockroachDBCharmEvents()
    state = StoredState()

    COCKROACHDB_SERVICE = 'cockroachdb.service'
    SYSTEMD_SERVICE_FILE = f'/etc/systemd/system/{COCKROACHDB_SERVICE}'
    WORKING_DIRECTORY = '/var/lib/cockroach'
    COCKROACH_INSTALL_DIR = '/usr/local/bin'
    COCKROACH_BINARY_PATH = f'{COCKROACH_INSTALL_DIR}/cockroach'

    def __init__(self, framework, key):
        super().__init__(framework, key)

        for event in (self.on.install,
                      self.on.start,
                      # self.on.upgrade_charm,
                      # self.on.config_changed,
                      self.on.cockroachpeer_relation_changed,
                      self.on.cockroachdb_started):
            self.framework.observe(event, self)

        self.peers = CockroachDBPeers(self, 'cockroachpeer')
        self.framework.observe(self.on.cluster_initialized, self.peers)

    def on_install(self, event):
        self.state.is_started = False

        ARCHITECTURE = 'amd64'  # hard-coded until it becomes important
        version = self.framework.model.config['version']
        cmd = (f'wget -qO- https://binaries.cockroachdb.com/cockroach-{version}.linux-{ARCHITECTURE}.tgz'
               f'| tar -C {self.COCKROACH_INSTALL_DIR} -xvz --wildcards --strip-components 1 --no-anchored "cockroach*/cockroach"')
        subprocess.check_call(cmd, shell=True)
        os.chown(self.COCKROACH_BINARY_PATH, 0, 0)

        self._setup_systemd_service()

    @property
    def is_single_node(self):
        """Both replication factors were set to 1 so it's a good guess that an operator wants a 1-node deployment."""
        default_zone_rf = self.framework.model.config['default_zone_replicas']
        system_data_rf = self.framework.model.config['system_data_replicas']
        return default_zone_rf == 1 and system_data_rf == 1

    def _setup_systemd_service(self):
        env = Environment(loader=FileSystemLoader('templates'))
        template = env.get_template('cockroachdb.service')

        if self.is_single_node:
            # start-single-node will set replication factors for all zones to 1.
            exec_start_line = f'ExecStart={self.COCKROACH_BINARY_PATH} start-single-node --insecure'
        else:
            join_addresses = f'{self.peers.advertise_addr},{",".join(self.peers.peer_addresses)}'
            # --insecure until the charm gets CA setup support figured out.
            exec_start_line = (f'ExecStart={self.COCKROACH_BINARY_PATH} --insecure'
                               f'--advertise-addr={self.peers.advertise_addr} '
                               f'--join={join_addresses}')
        ctxt = {
            'working_directory': self.WORKING_DIRECTORY,
            'exec_start_line': exec_start_line,
        }
        rendered_content = template.render(ctxt)
        with open(self.SYSTEMD_SERVICE_FILE, 'wb') as f:
            f.write(rendered_content.encode('utf-8'))
        subprocess.check_call(['systemctl', 'daemon-reload'])
        subprocess.check_call(['useradd', '-m', '--home-dir', self.WORKING_DIRECTORY, '--shell', '/usr/sbin/nologin', 'cockroach'])

    def on_start(self, event):
        unit = self.framework.model.unit
        # If both replication factors are set to 1 and the current unit != initial cluster unit,
        # don't start the process if the cluster has already been initialized.
        # This configuration is not practical in real deployments (i.e. multiple units, RF=1).
        initial_unit = self.peers.initial_unit
        if self.is_single_node and (initial_unit is not None and self.framework.model.unit.name != initial_unit):
            unit.status = BlockedStatus('Extra unit in a single-node deployment.')
            return
        subprocess.check_call(['systemctl', 'start', f'{self.COCKROACHDB_SERVICE}'])
        self.state.is_started = True
        self.on.cockroachdb_started.emit()

        if self.peers.is_joined and self.peers.is_cluster_initialized:
            unit.status = ActiveStatus()

    def on_cockroachpeer_relation_changed(self, event):
        if self.state.is_started and self.peers.is_cluster_initialized:
            self.framework.model.unit.status = ActiveStatus()

    def on_cockroachdb_started(self, event):
        if not self.peers.is_joined:
            event.defer()
            return

        unit = self.framework.model.unit
        if self.peers.is_cluster_initialized:
            # Skip this event when some other unit has already initialized a cluster.
            unit.status = ActiveStatus()
            return
        elif not unit.is_leader():
            unit.status = WaitingStatus('Waiting for the leader unit to initialize a cluster.')
            event.defer()
            return

        unit.status = MaintenanceStatus('Initializing the cluster')
        # Initialize the cluster if we're a leader in a multi-node deployment.
        if not self.is_single_node and self.framework.model.unit.is_leader():
            subprocess.check_call([self.COCKROACH_BINARY_PATH, 'init'])

        self.on.cluster_initialized.emit(self.__get_cluster_id())
        unit.status = ActiveStatus()

    def __get_cluster_id(self):
        out = subprocess.check_output([self.COCKROACH_BINARY_PATH, 'debug', 'gossip-values', '--insecure']).decode('utf-8')
        cluster_id_regex = re.compile(r'"cluster-id": ([0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12})$')
        for line in out.split('\n'):
            m = cluster_id_regex.match(line)
            if m:
                return m.group(1)


if __name__ == '__main__':
    main(CockroachDBCharm)
