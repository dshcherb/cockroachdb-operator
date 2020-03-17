CockroachDB Charm
==================================

# Deployment Requirements

The charm requires Juju 2.7.5 to be present (see [LP: #1865229](https://bugs.launchpad.net/juju/+bug/1865229)).

# Deployment

In order to deploy CockroachDB in a single-node mode, set replication factors to 1 explicitly.

```bash
juju deploy <charm-src-dir> --config default_zone_replicas=1 --config system_data_replicas=1
```

CockroachDB will use a replication factor of 3 unless explicitly specified.

```bash
juju deploy <charm-src-dir>
juju add-unit cockroachdb -n 2
```

HA with an explicit amount of replicas.

```bash
juju deploy <charm-src-dir> --config default_zone_replicas=3 --config system_data_replicas=3 -n 3
```

# Known Issues

The charm uses a workaround for [LP: #1859769](https://bugs.launchpad.net/juju/+bug/1859769) for single-node deployments by saving a cluster ID in a local state before the peer relation becomes available.
