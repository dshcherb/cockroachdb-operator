import ipaddress

from ops.framework import Object


class CockroachDBPeers(Object):
    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.relation_name = relation_name
        self._relation = None
        self.framework.observe(charm.on.cluster_initialized, self)

    @property
    def _relations(self):
        return self.framework.model.relations[self.relation_name]

    @property
    def is_single(self):
        return len(self._relations) == 1

    @property
    def is_joined(self):
        return self.framework.model.get_relation(self.relation_name) is not None

    @property
    def relation(self):
        if self._relation is None:
            self._relation = self.framework.model.get_relation(self.relation_name)
        return self._relation

    def on_cluster_initialized(self, event):
        if not self.framework.model.unit.is_leader():
            raise RuntimeError('The initial unit of a cluster must also be a leader.')
        self.relation.data[self.relation.app]['initial_unit'] = self.framework.model.unit.name
        self.relation.data[self.relation.app]['cluster_id'] = event.cluster_id

    @property
    def is_cluster_initialized(self):
        """Determined by the presence of a cluster ID."""
        return self.relation.data[self.relation.app].get('cluster_id') is not None

    @property
    def initial_unit(self):
        """Return the unit that has initialized the cluster."""
        if self.is_joined:
            return self.relation.get('initial_unit')
        else:
            return None

    @property
    def peer_addresses(self):
        addresses = []
        for u in self.relation.units:
            addresses.append(self.relation.data[u]['ingress-address'])
        return addresses

    @property
    def advertise_addr(self):
        if self.is_joined:
            return self.model.get_binding(self.relation).network.ingress_address
        else:
            return ipaddress.ip_address('127.0.0.1')
