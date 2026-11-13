# kevaso-provider-linode

The [Kevaso](https://github.com/dlebee/kevaso) provider for Linode (Akamai Cloud). One program, two sides:

| Side | Runs on | Does | Holds |
|---|---|---|---|
| controller (`serve`) | the control plane | calls the Linode API for kevasod, over [the plugin protocol](https://github.com/dlebee/kevaso/blob/main/docs/adr/0003-provider-plugins.md) | the API token |
| node (`node`) | Linode workers | announces shared IPs over BGP | nothing |

What it serves:

| | Name | What you get |
|---|---|---|
| Hosts | | Workers that are Linodes are recognised by address: `kvo describe host` shows the Linode, region and private address |
| Load balancer | `linode-nodebalancer` | A NodeBalancer per `IP` (one is made for each service), sending each TCP port to the silo's host over the private network. Its address is the balancer's: kept with the `IP` across moves and holders (parked, forwarding nowhere, while nobody holds it), on no host, so a silo can't send from it |
| Load balancer | `linode-shared-ip` | A reserved IPv4 (`kvo create ip NAME --class linode-shared-ip --region R`), or one you already own (`spec.address`), shared between the region's Linodes and announced by whichever host has the silo, so it follows the silo and the silo can send from it (`egress.source: sameAsService`) |
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
