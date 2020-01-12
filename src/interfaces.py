import json
import subprocess

from ops.framework import Object


class CockroachDBPeers(Object):
    def __init__(self, parent, relation_name):
        super().__init__(parent, relation_name)
        self.relation_name = relation_name

    @property
    def _relations(self):
        return self.framework.model.relations[self.relation_name]

    @property
    def is_single(self):
        return len(self._relations) == 1

    @property
    def is_joined(self):
        return self.framework.model.get_relation(self.relation_name)

    def on_cockroachpeer_relation_joined(self, event):
        self.peer_rel = self._relations[0]

    def on_cluster_initialized(self, event):
        if not self.framework.model.unit.is_leader():
            raise RuntimeError('The initial unit of a cluster must also be a leader.')
        self.peer_rel['initial_unit'] = self.framework.model.unit.name
        self.peer_rel.data[self.peer_rel.app]['cluster_id'] = event.cluster_id

    @property
    def is_cluster_initialized(self):
        """Determined by the presence of a cluster ID."""
        return True if self.peer_rel.data[self.peer_rel.app]['cluster_id'] else False

    @property
    def initial_unit(self):
        """Return the unit that has initialized the cluster."""
        if self.is_joined:
            return self.peer_rel.get('initial_unit')
        else:
            return None

    @property
    def peer_addresses(self):
        addresses = []
        for u in self.peer_rel.units:
            addresses.append(self.peer_rel.data[u]['ingress-address'])
        return addresses

    @property
    def advertise_addr(self):
        network_info = self.__network_get('cockroachpeer')
        return network_info['ingress-address']

    # TODO: move this to the operator framework model backend
    def __network_get(self, endpoint):
        return json.loads(subprocess.check_output(['network-get', endpoint, '--format=json']).decode('utf-8'))
