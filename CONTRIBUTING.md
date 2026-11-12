# Contributing

Apache 2.0, with the **Developer Certificate of Origin** (DCO) like [Kevaso core](https://github.com/dlebee/kevaso):
sign off every commit with `git commit -s`. CI rejects commits without it.

- `make vet test` before pushing. The Linode API is faked (`linode/fakeapi_test.go`): a change that
  calls a new endpoint adds it there.
- `make e2e` runs on real Linodes and costs money (cents per run). It needs a token in a file
  (`KEVASO_LINODE_TOKEN_FILE`) and core checked out next to this repo with `make workspace` run in
  it. Everything it makes is tagged with the run and deleted at the end, failed or not.
