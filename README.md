CockroachDB Charm
==================================

# Overview

This charm provides means to deploy and operate cockroachdb - a scalable, cloud-native SQL database with built-in clustering support.

# Deployment Requirements

The charm requires Juju 2.7.5 to be present (see [LP: #1865229](https://bugs.launchpad.net/juju/+bug/1865229)).

# Deployment

In order to deploy CockroachDB in a single-node mode, set replication factors to 1 explicitly.

```bash
juju deploy <charm-src-dir> --config default-zone-replicas=1 --config system-data-replicas=1
```

CockroachDB will use a replication factor of 3 unless explicitly specified.

```bash
juju deploy <charm-src-dir>
juju add-unit cockroachdb -n 2
```

HA with an explicit amount of replicas.

```bash
juju deploy <charm-src-dir> --config default-zone-replicas=3 --config system-data-replicas=3 -n 3
```

# Accessing CLI

```
juju ssh cockroachdb/0
cockroach sql
```

# Web UI

The web UI is accessible at `https://<unit-ip>:8080`

# Using Haproxy as a Load-balancer

An app deployed by this charm can be related to [charm-haproxy](https://github.com/dshcherb/charm-haproxy):

```bash
juju deploy <cockroachdb-charm-src-dir> --config default-zone-replicas=3 --config system-data-replicas=3 -n 3
juju deploy <haproxy-charm-src-dir>
juju relate haproxy cockroachdb
```

Currently the WEB UI is not exposed to an HTTP load-balancer (only postgres protocol connections over TCP are).

# Known Issues

The charm uses a workaround for [LP: #1859769](https://bugs.launchpad.net/juju/+bug/1859769) for single-node deployments by saving a cluster ID in a local state before the peer relation becomes available.
