# Storage layout

> **Not final.** This layout is an early draft and will likely change as the project iterates. Do not build anything that depends on it staying stable.

This document specifies how a Nix binary cache is stored in RADOS. It is the contract for anyone inspecting objects directly or writing another implementation that shares a pool with this server. It does not describe the server's configuration or HTTP behaviour; see the README for those.

## Isolation model

A cache is a single RADOS pool, using the default namespace. Object names carry no cache name, prefix, magic value, or format version. Two caches that share a pool overlap completely and corrupt each other. The pool is the isolation mechanism.

## Object names

| Nix cache path       | RADOS object name  |
|----------------------|--------------------|
| `<hash>.narinfo`     | `<hash>.narinfo`   |
| `nar/<name>`         | `nar/<name>`       |

`<hash>` and `<name>` consist only of `A-Z a-z 0-9 . _ -`. RADOS names are flat; the `/` in `nar/` is an ordinary character. NAR names are whatever the client uploads, typically `<filehash>.nar.xz` or `<filehash>.nar`.

`nix-cache-info` is never stored: the server synthesizes it on GET and discards it on PUT. Its absence says nothing about whether the cache exists.

A name that does not match one of these shapes is foreign. The server never reads or deletes foreign objects.

## Plain objects

Every object holds the file's bytes verbatim. It has no omap and at most one xattr, `access_count`.

`access_count` is a decimal ASCII count of successful GET requests for the object. It is absent until the first GET, and an absent xattr means zero. HEAD and PUT never touch it. It is best-effort: concurrent reads of one object can lose increments. A NAR's count is its downloads. A narinfo's count is queries: Nix GETs a narinfo several times per download and once more when pushing a path that already exists.

- Created exclusively in a single operation: an existing object is never overwritten. A duplicate upload leaves the stored bytes untouched.
- Maximum size is 16 MiB; larger uploads are refused. There is no striping.

## Integrity

The server stores and verifies no checksums. Nix clients verify NARs against the hashes and signatures in the corresponding narinfo.

## Compatibility checklist

An alternative implementation must:

- name objects exactly as in *Object names* and add no prefix
- never overwrite an existing object; create exclusively
- store file bytes verbatim, with no omap and no xattrs other than `access_count`
- ignore names it does not recognise
- treat a missing `access_count` as zero; it may leave the xattr untouched

It must not:

- store metadata in omap, in a manifest object, or in xattrs other than `access_count`
- rely on a cache marker object; there is none
- store `nix-cache-info` as an object

## Inspecting with the rados CLI

Substitute the cache's pool.

```sh
rados -p nixcache ls
rados -p nixcache stat 'nar/<name>'
rados -p nixcache getxattr 'nar/<name>' access_count
rados -p nixcache get '<hash>.narinfo' - | head
rados -p nixcache get 'nar/<name>' nar.xz
```
