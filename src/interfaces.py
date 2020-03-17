from ops.framework import Object, StoredState


class CockroachDBPeers(Object):

    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(self._relation_name)
        self.framework.observe(charm.on.cluster_initialized, self)

        self.state.set_default(cluster_id=None)

    @property
    def _relations(self):
        return self.framework.model.relations[self._relation_name]

    @property
    def is_single(self):
        return len(self._relations) == 1

    @property
    def is_joined(self):
        return self._relation is not None

    def on_cluster_initialized(self, event):
        if not self.framework.model.unit.is_leader():
            raise RuntimeError('The initial unit of a cluster must also be a leader.')

        # A workaround for LP: #1859769.
        self.state.cluster_id = event.cluster_id
        if not self.is_joined:
            event.defer()
            return

        self._relation.data[self.model.app]['initial_unit'] = self.framework.model.unit.name
        self._relation.data[self.model.app]['cluster_id'] = self.state.cluster_id

    @property
    def is_cluster_initialized(self):
        """Determined by the presence of a cluster ID."""
        if self.is_joined:
            return self._relation.data[self.model.app].get('cluster_id') is not None
        elif self.state.cluster_id:
            return True
        else:
            return False

    @property
    def initial_unit(self):
        """Return the unit that has initialized the cluster."""
        if self.is_joined:
            return self._relation.get('initial_unit')
        else:
            return None

    @property
    def peer_addresses(self):
        addresses = []
        for u in self._relation.units:
            addresses.append(self._relation.data[u]['ingress-address'])
        return addresses

    @property
    def advertise_addr(self):
        return self.model.get_binding(self._relation_name).network.ingress_address
