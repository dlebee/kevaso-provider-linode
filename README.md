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
| Load balancers | NodeBalancers | A service of `type: LoadBalancer` with `loadBalancer: { provider: linode }` gets its own NodeBalancer, a config per port, each to the ready silos' hosts on a node port over the private network (the node port is open to the NodeBalancer only). Services of one namespace that name the same `linode.kevaso.com/nodebalancer` share one, each on its own ports; it is made with the first and removed with the last. Its address is its own (a kept IP can't front it), on no host, so a silo can't send from it. It answers on IPv6 too: the service's status shows both (`loadBalancerIP`, `loadBalancerIPv6`), and the backends stay on private IPv4 |
| Provider IPs | reserved IPv4s | An `IP` of `type: Provider, provider: linode, region: R` is a Linode reserved IPv4 in R, shared with the region's Linodes and announced over BGP by whichever worker runs the silo using it (`hostPort: { ip: NAME }`), so it follows the silo within the region and the silo can send from it. Only regions with Linode's BGP failover. Deleting the silo keeps it; deleting the `IP` releases it. IPv4 only: Linode gives extra IPv6 only as /64 ranges, and a range moves between Linodes as one unit, so `family: IPv6` is refused at apply. For IPv6, use the worker's own (each Linode's SLAAC address is its `externalIPv6`, found by the worker) or a host address of a /64 added to that Linode. Sharing a second IPv4 with a Linode needs Linode support to raise the account's per-Linode IPv4 limit |
| Storage | `linode-block` | Block Storage, created in the host's region, attached to it, formatted once and mounted for a container silo |
| DNS | `linode` | Linode DNS records for an IngressClass's host names, and for ACME DNS-01 |

An address reserved for an `IP` is remembered by the plugin until core records it; a plugin that
crashes in between leaves an unused reserved IPv4 in the account (Linode's reserved IPs carry no
label to find it by): the account's reserved IPs (Cloud Manager, or `GET /v4/networking/reserved/ips`) show them.

The provider shares, and on release unshares, only addresses it reserved: a machine's own address,
and sharing set up outside Kevaso, are never touched.

### Annotations

Declared to core, which refuses a misspelled one or a bad value at apply. Under the name the
plugin is installed as (`linode-work.kevaso.com/…` for a second account); on a LoadBalancer
service:

| Annotation | Values | Default |
|---|---|---|
| `linode.kevaso.com/nodebalancer` | a name (lower-case letters, digits, `-`; 19 at most): share that NodeBalancer with the namespace's services naming it | one per service |
| `linode.kevaso.com/nodebalancer-type` | `common`, `premium` (Linode has plan types, not sizes); fixed when it is made | `common` |
| `linode.kevaso.com/client-conn-throttle` | 0–20 new connections per second per client IP; 0 is no limit | 0 |
| `linode.kevaso.com/algorithm` | `roundrobin`, `leastconn`, `source` | `roundrobin` |
| `linode.kevaso.com/proxy-protocol` | `none`, `v1`, `v2`: the client's address to the silo (it must speak PROXY protocol on that port); the only way a silo sees it behind a NodeBalancer | `none` |
| `linode.kevaso.com/check` | `connection`, `none` | `connection` |
| `linode.kevaso.com/check-interval`, `check-timeout`, `check-attempts` | seconds (2–3600), seconds (1–30, below the interval), tries (1–30) | 5, 3, 2 |

Services sharing a NodeBalancer share its type and throttle: the type is the first one's (another
type is refused), the throttle the last one set.

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
