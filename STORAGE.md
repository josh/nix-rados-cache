# Storage layout

> **Not final.** This layout is an early draft and will likely change as the project iterates. Do not build anything that depends on it staying stable.

This document specifies how a Nix binary cache is stored in RADOS. It is the contract for anyone inspecting objects directly or writing another implementation that shares a pool with this server. It does not describe the server's configuration or HTTP behaviour; see the README for those.

## Isolation model

A cache is a single RADOS pool, using the default namespace. Object names carry no cache name, prefix, magic value, or format version. Two caches that share a pool overlap completely and corrupt each other. The pool is the isolation mechanism.

## Object names

| Nix cache path       | RADOS objects                                                        |
|----------------------|----------------------------------------------------------------------|
| `<hash>.narinfo`     | `<hash>.narinfo`                                                     |
| `nar/<name>`         | `nar/<name>.0000000000000000`, `nar/<name>.0000000000000001`, …      |

`<hash>` and `<name>` consist only of `A-Z a-z 0-9 . _ -`. RADOS names are flat; the `/` in `nar/` is an ordinary character. NAR names are whatever the client uploads, typically `<filehash>.nar.xz` or `<filehash>.nar`.

`nix-cache-info` is never stored: the server synthesizes it on GET and discards it on PUT. Its absence says nothing about whether the cache exists.

A name that does not match one of these shapes is foreign. The server never reads or deletes foreign objects.

## Narinfo objects

A narinfo is one object holding the file's bytes verbatim, created exclusively in a single operation. A later PUT of the same name changes nothing except that its `Sig:` lines not already present are appended, so signatures accumulate and are never removed or reordered; every other field keeps the bytes from the first upload. It has no omap and at most one xattr, `access_count`. Maximum size is 16 MiB.

## NAR objects

A NAR is split into fixed-size stripes in the layout of Ceph's libradosstriper, restricted to one object per stripe (`stripe_count` 1, `stripe_unit` equal to `object_size`). Stripe `i` is the object `nar/<name>.<i>` with `i` as sixteen lowercase hex digits, and holds bytes `i·S` up to `(i+1)·S` of the file verbatim, where `S` is the stripe size the server was started with (`--stripe-size`, 16 MiB by default). Only the last stripe is shorter. `rados --striper get` reads a whole NAR.

Stripe 0 carries the xattrs:

| xattr                         | value                                   |
|-------------------------------|-----------------------------------------|
| `striper.layout.stripe_unit`  | `S`                                     |
| `striper.layout.stripe_count` | `1`                                     |
| `striper.layout.object_size`  | `S`                                     |
| `striper.size`                | total length of the NAR in bytes        |
| `access_count`                | see below; absent until the first GET   |

Readers take `S` from `striper.layout.object_size`, never from the running server's flag, so NARs written under a different stripe size stay readable.

Stripes 1 and up are written first, each as one full-object write. Stripe 0 is written last, exclusively, with its xattrs and data in a single operation, and is the completion marker: a NAR exists once stripe 0 exists, and a reader never sees a partial NAR. A duplicate upload rewrites identical bytes to stripes 1 and up (NAR names are content hashes) and stops at stripe 0, which is never overwritten. Stripes without a stripe 0 are leftovers from a failed upload; nothing reads them and a future garbage collector may delete them.

`S` must not exceed the cluster's `osd_max_object_size`, and `S` plus the xattrs written with stripe 0 must fit within `osd_max_write_size`; a stripe the OSD refuses fails the upload.

## Access counts

`access_count` is a decimal ASCII count of successful GET requests for the object, kept on a narinfo object or on stripe 0 of a NAR. It is absent until the first GET, and an absent xattr means zero. HEAD and PUT never touch it. It is best-effort: concurrent reads of one object can lose increments. A NAR's count is its downloads. A narinfo's count is queries: Nix GETs a narinfo several times per download and once more when pushing a path that already exists.

## Integrity

The server stores and verifies no checksums. Nix clients verify NARs against the hashes and signatures in the corresponding narinfo.

## Compatibility checklist

An alternative implementation must:

- name objects exactly as in *Object names* and add no prefix
- write NARs in the stripe layout above, stripe 0 last and exclusively
- never modify a narinfo except by appending `Sig:` lines; never overwrite a stripe 0
- store file bytes verbatim, with no omap and no xattrs other than those listed
- ignore names it does not recognise
- treat a missing `access_count` as zero; it may leave the xattr untouched

It must not:

- store metadata in omap, in a manifest object, or in xattrs other than those listed
- rely on a cache marker object; there is none
- store `nix-cache-info` as an object

## Inspecting with the rados CLI

Substitute the cache's pool.

```sh
rados -p nixcache ls
rados -p nixcache --striper get 'nar/<name>' nar.xz
rados -p nixcache getxattr 'nar/<name>.0000000000000000' striper.size
rados -p nixcache getxattr 'nar/<name>.0000000000000000' access_count
rados -p nixcache get '<hash>.narinfo' - | head
```
