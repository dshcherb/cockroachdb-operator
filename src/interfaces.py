from ops.framework import Object


class CockroachDBPeers(Object):
    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.relation_name = relation_name

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
    def peer_rel(self):
        return self.framework.model.get_relation(self.relation_name)

    def on_cluster_initialized(self, event):
        if not self.framework.model.unit.is_leader():
            raise RuntimeError('The initial unit of a cluster must also be a leader.')
        self.peer_rel.data[self.peer_rel.app]['initial_unit'] = self.framework.model.unit.name
        self.peer_rel.data[self.peer_rel.app]['cluster_id'] = event.cluster_id

    @property
    def is_cluster_initialized(self):
        """Determined by the presence of a cluster ID."""
        return self.peer_rel.data[self.peer_rel.app].get('cluster_id') is not None

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
        return self.model.bindings['cockroachpeer'].ingress_address
