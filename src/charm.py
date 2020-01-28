#!/usr/bin/env python3

import os
import subprocess
import re
import pwd
import sys
sys.path.append('lib')

from ops.charm import CharmBase, CharmEvents
from ops.framework import EventBase, EventSource, StoredState
from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    WaitingStatus,
    ModelError,
)
from interfaces import CockroachDBPeers

from jinja import Environment, FileSystemLoader
from datetime import timedelta
from time import sleep


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
    COCKROACH_USERNAME = 'cockroach'

    MAX_RETRIES = 10
    RETRY_TIMEOUT = timedelta(milliseconds=125)

    def __init__(self, framework, key):
        super().__init__(framework, key)

        for event in (self.on.install,
                      self.on.start,
                      # self.on.upgrade_charm,
                      self.on.config_changed,
                      self.on.cockroachpeer_relation_changed,
                      self.on.cockroachdb_started):
            self.framework.observe(event, self)

        self.peers = CockroachDBPeers(self, 'cockroachpeer')

    def on_install(self, event):
        self.state.is_started = False

        try:
            resource_path = self.model.resources.fetch('cockroach-linux-amd64')
        except ModelError:
            resource_path = None

        if resource_path is None:
            ARCHITECTURE = 'amd64'  # hard-coded until it becomes important
            version = self.model.config['version']
            cmd = (f'wget -qO- https://binaries.cockroachdb.com/cockroach-{version}.linux-{ARCHITECTURE}.tgz'
                   f'| tar -C {self.COCKROACH_INSTALL_DIR} -xvz --wildcards --strip-components 1 --no-anchored "cockroach*/cockroach"')
            subprocess.check_call(cmd, shell=True)
            os.chown(self.COCKROACH_BINARY_PATH, 0, 0)
        else:
            cmd = ['tar', '-C', self.COCKROACH_INSTALL_DIR, '-xv', '--wildcards',
                   '--strip-components', '1', '--no-anchored', 'cockroach*/cockroach', '-zf', str(resource_path)]
            subprocess.check_call(cmd)

        self._setup_systemd_service()

    @property
    def is_single_node(self):
        """Both replication factors were set to 1 so it's a good guess that an operator wants a 1-node deployment."""
        default_zone_rf = self.model.config['default_zone_replicas']
        system_data_rf = self.model.config['system_data_replicas']
        return default_zone_rf == 1 and system_data_rf == 1

    def _setup_systemd_service(self):
        if self.is_single_node:
            # start-single-node will set replication factors for all zones to 1.
            exec_start_line = f'ExecStart={self.COCKROACH_BINARY_PATH} start-single-node --advertise-addr {self.peers.advertise_addr} --insecure'
        else:
            peer_addresses = [self.peers.advertise_addr]
            if self.peers.is_joined:
                peer_addresses.extend(self.peers.peer_addresses)
            join_addresses = ','.join(peer_addresses)
            # --insecure until the charm gets CA setup support figured out.
            exec_start_line = (f'ExecStart={self.COCKROACH_BINARY_PATH} start --insecure '
                               f'--advertise-addr={self.peers.advertise_addr} '
                               f'--join={join_addresses}')
        ctxt = {
            'working_directory': self.WORKING_DIRECTORY,
            'exec_start_line': exec_start_line,
        }
        env = Environment(loader=FileSystemLoader('templates'))
        template = env.get_template('cockroachdb.service')
        rendered_content = template.render(ctxt)

        content_hash = hash(rendered_content)
        # TODO: read the rendered file instead to account for any manual edits.
        old_hash = getattr(self.state, 'rendered_content_hash', None)

        if old_hash is None or old_hash != content_hash:
            self.state.rendered_content_hash = content_hash
            with open(self.SYSTEMD_SERVICE_FILE, 'wb') as f:
                f.write(rendered_content.encode('utf-8'))
            subprocess.check_call(['systemctl', 'daemon-reload'])

            try:
                pwd.getpwnam(self.COCKROACH_USERNAME)
            except KeyError:
                subprocess.check_call(['useradd', '-m', '--home-dir', self.WORKING_DIRECTORY, '--shell', '/usr/sbin/nologin', self.COCKROACH_USERNAME])

            if self.state.is_started:
                subprocess.check_call(['systemctl', 'restart', f'{self.COCKROACHDB_SERVICE}'])

    def on_start(self, event):
        unit = self.model.unit
        # If both replication factors are set to 1 and the current unit != initial cluster unit,
        # don't start the process if the cluster has already been initialized.
        # This configuration is not practical in real deployments (i.e. multiple units, RF=1).
        initial_unit = self.peers.initial_unit
        if self.is_single_node and (initial_unit is not None and self.model.unit.name != initial_unit):
            unit.status = BlockedStatus('Extra unit in a single-node deployment.')
            return
        subprocess.check_call(['systemctl', 'start', f'{self.COCKROACHDB_SERVICE}'])
        self.state.is_started = True
        self.on.cockroachdb_started.emit()

        if self.peers.is_joined and self.peers.is_cluster_initialized:
            unit.status = ActiveStatus()

    def on_cockroachpeer_relation_changed(self, event):
        self._setup_systemd_service()

        if self.state.is_started and self.peers.is_cluster_initialized:
            self.model.unit.status = ActiveStatus()

    def on_cockroachdb_started(self, event):
        if not self.peers.is_joined:
            event.defer()
            return

        unit = self.model.unit
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
        if not self.is_single_node and self.model.unit.is_leader():
            subprocess.check_call([self.COCKROACH_BINARY_PATH, 'init', '--insecure'])

        self.on.cluster_initialized.emit(self.__get_cluster_id())
        unit.status = ActiveStatus()

    def __get_cluster_id(self):
        for _ in range(self.MAX_RETRIES):
            res = subprocess.run([self.COCKROACH_BINARY_PATH, 'debug', 'gossip-values', '--insecure'],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if not res.returncode:
                out = res.stdout.decode('utf-8')
                break
            elif not re.findall(r'code = Unavailable desc = node waiting for init', res.stderr.decode('utf-8')):
                raise RuntimeError('unexpected error returned while trying to obtain gossip-values')
            sleep(self.RETRY_TIMEOUT.total_seconds())

        cluster_id_regex = re.compile(r'"cluster-id": (?P<uuid>[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12})$')
        for line in out.split('\n'):
            m = cluster_id_regex.match(line)
            if m:
                return m.group('uuid')
        raise RuntimeError('could not find cluster-id in the gossip-values output')

    def on_config_changed(self, event):
        # TODO: handle configuration changes to replication factors and apply them via cockroach sql.
        pass


if __name__ == '__main__':
    main(CockroachDBCharm)
