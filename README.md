# kevaso-provider-linode

The [Kevaso](https://github.com/dlebee/kevaso) provider for Linode (Akamai Cloud). One program, two sides:

| Side | Runs on | Does | Holds |
|---|---|---|---|
| controller (`serve`) | the control plane | calls the Linode API for kevasod, over [the plugin protocol](https://github.com/dlebee/kevaso/blob/main/docs/adr/0003-provider-plugins.md) | the API token |
| node (`node`) | Linode workers | announces shared IPs over BGP | nothing |

What it serves:

| | Name | What you get |
|---|---|---|
| Hosts | | Workers that are Linodes are recognised by their external or internal IP: `kvo describe host` shows the Linode and region, and the Linode's private address becomes the host's internal IP when the operator set none |
| Load balancer | `linode-nodebalancer` | One NodeBalancer per `IP`, shared by every service on it (by default, all of a namespace's services of the class), a config per port, each to its own service's hosts over the private network. Its address is the balancer's: kept with the `IP` across moves and holders (parked, forwarding nowhere, while nobody holds it), on no host, so a silo can't send from it. Annotations on the `IP` or a service: `linode.kevaso.com/algorithm` (roundrobin, leastconn, source) and `linode.kevaso.com/proxy-protocol` (none, v1, v2); others in that domain are refused |
| Load balancer | `linode-shared-ip` | Exclusive (one host announces it, so one silo holds it): a reserved IPv4 (`kvo create ip NAME --class linode-shared-ip --region R`), or one you already own (the `IP`'s `spec.address`), shared between the region's Linodes and announced by whichever host has the silo, so it follows the silo and the silo can send from it (`egress.source: sameAsService`) |
| Storage | `linode-block` | Block Storage, created in the host's region, attached to it, formatted once and mounted for a container silo |
| DNS | `linode` | Linode DNS records for an IngressClass's host names, and for ACME DNS-01 |

An address reserved for an `IP` is remembered by the plugin until core records it; a plugin that
crashes in between leaves an unused reserved IPv4 in the account (Linode's reserved IPs carry no
label to find it by): the account's reserved IPs (Cloud Manager, or `GET /v4/networking/reserved/ips`) show them.

Install with `kvoadm plugin install`; the user guide is
[dlebee.github.io/kevaso-docs/providers/linode](https://dlebee.github.io/kevaso-docs/providers/linode/).

## Build

```bash
make build
```

## Test

```bash
make test
```

The e2e runs on real Linodes (it costs a few cents and deletes everything it made):

```bash
KEVASO_LINODE_TOKEN_FILE=~/linode-token.txt make e2e
```
