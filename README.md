# kevaso-provider-linode

The [Kevaso](https://github.com/dlebee/kevaso) provider for Linode (Akamai Cloud). One program, two sides:

| Side | Runs on | Does | Holds |
|---|---|---|---|
| controller (`serve`) | the control plane | calls the Linode API for kevasod, over [the plugin protocol](https://github.com/dlebee/kevaso/blob/main/docs/adr/0003-provider-plugins.md) | the API token |
| node (`node`) | Linode workers | announces shared IPs over BGP | nothing |

What it serves:

| | Name | What you get |
|---|---|---|
| Hosts | | Workers that are Linodes are recognised by their external or internal IP: `kvo describe host` shows the Linode and region, and the Linode's private address becomes the host's internal IP when the operator set none ([Addresses](#addresses)) |
| Load balancer | `linode-nodebalancer` | One NodeBalancer per `IP`, shared by every service on it (by default, all of a namespace's services of the class), a config per port, each to its own service's hosts over the private network. Its address is the balancer's: kept with the `IP` across moves and holders (parked, forwarding nowhere, while nobody holds it), on no host, so a silo can't send from it. Annotations on the `IP` or a service: `linode.kevaso.com/algorithm` (roundrobin, leastconn, source) and `linode.kevaso.com/proxy-protocol` (none, v1, v2); others in that domain are refused |
| Load balancer | `linode-shared-ip` | Exclusive (one host announces it, so one silo holds it): a reserved IPv4 (`kvo create ip NAME --class linode-shared-ip --region R`), or one you already own that is tagged for Kevaso (the `IP`'s `spec.address`, listed in the config's `adopt`), shared between the region's Linodes and announced by whichever host has the silo, so it follows the silo and the silo can send from it (`egress.source: sameAsService`) |
| Storage | `linode-block` | Block Storage, created in the host's region, attached to it, formatted once and mounted for a container silo |
| DNS | `linode` | Linode DNS records for an IngressClass's host names, and for ACME DNS-01 |

An address reserved for an `IP` is remembered by the plugin until core records it; a plugin that
crashes in between leaves an unused reserved IPv4 in the account (Linode's reserved IPs carry no
label to find it by): the account's reserved IPs (Cloud Manager, or `GET /v4/networking/reserved/ips`) show them.

An address the account already holds is used only if it is **tagged for Kevaso**: listed in the
config's `adopt` (`{"adopt": ["203.0.113.7"]}`; Linode's IP addresses carry no tags of their own).
An `IP` naming any other address is refused. The provider shares, and on release unshares, only an
address it reserved or one so listed: a machine's own address, and sharing set up outside Kevaso,
are never touched.

## Addresses

The provider never sets a host's `internalIP` or `externalIP`: the operator declares them when
adding the host (`kvoadm host add root@PUBLIC --external-ip PUBLIC`, or `--internal-ip`). From the
Linode's own record (`ipv4` in the Linode API) it reads only:

| Linode has | Provider reports | Host's internal IP | Host's external IP |
|---|---|---|---|
| a public IPv4 and a private IPv4 (192.168.128.0/17) | the private one, as `status.provider.privateAddress` | `spec.internalIP` if declared, else the private address | `spec.externalIP` as declared |
| a public IPv4 only | no private address | `spec.internalIP` if declared, else none | `spec.externalIP` as declared |

A host is recognised as a Linode when its declared internal or external IP is one of the Linode's
`ipv4` addresses. VLAN and VPC addresses are not in that list: the provider doesn't read them, and
a host declared only by such an address isn't recognised as a Linode.

On a Linode's public interface the public IPv4 is on the interface itself, so `hostPort: [external]`
opens on it. The provider and its tests only use Linodes with that interface (public plus the
optional private IPv4); a VPC interface whose public address is a 1:1 NAT hasn't been tried.

Install with `kvoadm plugin install --manifest kevaso-plugin.json` on each side: the manifest says
where its volumes' devices are (`/dev/disk/by-id/scsi-0Linode_Volume_…`), and kevasod refuses any
other device it answers, or every one if it was installed without the manifest. The user guide is
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
