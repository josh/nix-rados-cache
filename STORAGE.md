# Storage layout

> **Not final.** This layout is an early draft and will likely change as the project iterates. Do not build anything that depends on it staying stable.

This document specifies how a Nix binary cache is stored in RADOS. It is the contract for anyone inspecting objects directly or writing another implementation that shares a pool with this server. It does not describe the server's configuration or HTTP behaviour; see the README for those.

## Isolation model

A cache is a single RADOS pool, using the default namespace. Object names carry no cache name, prefix, magic value, or format version. Two caches that share a pool overlap completely and corrupt each other. The pool is the isolation mechanism.

## Object names

| Nix cache path       | RADOS objects                                                        |
|----------------------|----------------------------------------------------------------------|
| `<hash>.narinfo`     | `<hash>.narinfo`                                                     |
| `<hash>.ls`          | `<hash>.ls`                                                          |
| `log/<drv>`          | `log/<drv>`                                                          |
| `build-trace-v2/<drv>/<output>.doi` | `build-trace-v2/<drv>/<output>.doi`, only with `--ca-derivations` |
| `nar/<name>`         | `nar/<name>.0000000000000000`, `nar/<name>.0000000000000001`, …      |

`<hash>`, `<drv>` and `<name>` consist only of `A-Z a-z 0-9 . _ -`. RADOS names are flat; the `/` in `nar/` is an ordinary character. NAR names are whatever the client uploads, typically `<filehash>.nar.xz` or `<filehash>.nar`.

`nix-cache-info` is never stored: the server synthesizes it on GET and discards it on PUT. Its absence says nothing about whether the cache exists.

A name that does not match one of these shapes is foreign. The server never reads or deletes foreign objects.

## Plain objects

A narinfo, a NAR listing (`.ls`, written by Nix when `write-nar-listing` is on) and a build log (`log/<drv>`, written by `nix store copy-log`) are each one object holding the file's bytes verbatim, created exclusively in a single operation. It has no omap and at most four xattrs: `created`, `upstream`, `access_count` and `accessed`. Maximum size is 16 MiB.

A later PUT of a narinfo changes nothing except that its `Sig:` lines not already present are appended, so signatures accumulate and are never removed or reordered; every other field keeps the bytes from the first upload. A later PUT of a listing, log or realisation changes nothing; Nix re-registers a realisation with a full overwrite, so the first registration and its signatures are what the cache keeps.

## NAR objects

A NAR is split into fixed-size stripes in the layout of Ceph's libradosstriper, restricted to one object per stripe (`stripe_count` 1, `stripe_unit` equal to `object_size`). Stripe `i` is the object `nar/<name>.<i>` with `i` as sixteen lowercase hex digits, and holds bytes `i·S` up to `(i+1)·S` of the file verbatim, where `S` is the stripe size the server was started with (`--stripe-size`, 16 MiB by default). Only the last stripe is shorter. `rados --striper get` reads a whole NAR.

Stripe 0 carries the xattrs:

| xattr                         | value                                   |
|-------------------------------|-----------------------------------------|
| `striper.layout.stripe_unit`  | `S`                                     |
| `striper.layout.stripe_count` | `1`                                     |
| `striper.layout.object_size`  | `S`                                     |
| `striper.size`                | total length of the NAR in bytes        |
| `created`                     | see below                               |
| `upstream`                    | see below; absent for pushed objects    |
| `access_count`                | see below                               |
| `accessed`                    | see below                               |

Readers take `S` from `striper.layout.object_size`, never from the running server's flag, so NARs written under a different stripe size stay readable.

Stripes 1 and up are written first, each as one full-object write. Stripe 0 is written last, exclusively, with its xattrs and data in a single operation, and is the completion marker: a NAR exists once stripe 0 exists, and a reader never sees a partial NAR. A duplicate upload rewrites identical bytes to stripes 1 and up (NAR names are content hashes) and stops at stripe 0, which is never overwritten. Stripes without a stripe 0 are leftovers from a failed upload; nothing reads them and a future garbage collector may delete them.

`S` must not exceed the cluster's `osd_max_object_size`, and `S` plus the xattrs written with stripe 0 must fit within `osd_max_write_size`; a stripe the OSD refuses fails the upload.

## Access metadata

These xattrs live on a plain object or on stripe 0 of a NAR. `created` is when the object was written and never changes. `access_count` is a decimal ASCII count of requests that touched the object and `accessed` is the time of the latest; creation is the first, and every later successful GET, HEAD or PUT of the same name adds one. Times are RFC 3339 in UTC to the second. The count is best-effort: concurrent requests can lose increments. Both are activity signals for garbage collection, not download tallies; Nix fetches a narinfo several times per download and checks it again when pushing.

The object's own RADOS mtime is updated by these xattr writes, so it records when the object was last touched, not when its content was written.

`upstream` is the base URL the object was pulled from. With `--upstream` configured, a local miss is fetched from the first upstream that has it, stored unchanged with its signatures, and served in the same request.

## Integrity

The server stores and verifies no checksums. Nix clients verify NARs against the hashes and signatures in the corresponding narinfo.

## Inspecting with the rados CLI

Substitute the cache's pool.

```sh
rados -p nixcache ls
rados -p nixcache --striper get 'nar/<name>' nar.xz
rados -p nixcache getxattr 'nar/<name>.0000000000000000' striper.size
rados -p nixcache getxattr 'nar/<name>.0000000000000000' access_count
rados -p nixcache get '<hash>.narinfo' - | head
rados -p nixcache get '<hash>.ls' -
```
