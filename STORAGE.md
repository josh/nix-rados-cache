# Storage layout

> **Not final.** This layout is an early draft and will likely change as the project iterates. Do not build anything that depends on it staying stable.

This document specifies how a Nix binary cache is stored in RADOS. It is the contract for anyone inspecting objects directly or writing another implementation that shares a pool with this one. It does not describe how a server is configured or what it speaks over HTTP.

## Isolation model

A cache is a single RADOS pool, using the default namespace. Object names carry no cache name, prefix, magic value, or format version. Two caches that share a pool overlap completely and corrupt each other.

## Object names

| Nix cache path       | RADOS objects                                                        |
|----------------------|----------------------------------------------------------------------|
| `<hash>.narinfo`     | `<hash>.narinfo`                                                     |
| `<hash>.ls`          | `<hash>.ls`                                                          |
| `log/<drv>`          | `log/<drv>`                                                          |
| `build-trace-v2/<drv>/<output>.doi` | `build-trace-v2/<drv>/<output>.doi`                   |
| `nar/<name>`         | striped: `nar/<name>.0000000000000000`, `nar/<name>.0000000000000001`, … |
|                      | chunked: `nar/<name>` plus the `chunk/<sha256>` objects it lists      |

`<hash>`, `<drv>` and `<name>` consist only of `A-Z a-z 0-9 . _ -`. RADOS names are flat; the `/` in `nar/` is an ordinary character. NAR names are whatever the client uploads, typically `<filehash>.nar.xz` or `<filehash>.nar`. A `nar/` name ending in a dot and sixteen lowercase hex digits is a stripe; any other `nar/` name is a chunked NAR's manifest. `<sha256>` is sixty-four lowercase hex digits.

`nix-cache-info` is never stored, so its absence says nothing about whether the cache exists.

A name that does not match one of these shapes is foreign. An implementation never reads or deletes foreign objects.

## Plain objects

A narinfo, a NAR listing (`.ls`), a build log (`log/<drv>`) and a realisation (`.doi`) are each one object holding the file's bytes verbatim, created exclusively in a single operation. It has no omap and at most four xattrs: `created`, `upstream`, `access_count` and `accessed`. Maximum size is 16 MiB.

A later write of a narinfo changes nothing except that its `Sig:` lines not already present are added at the end, so signatures accumulate and are never removed or reordered; every other field keeps the bytes from the first upload. The merge reads the stored object and rewrites it whole, conditional on the version it read, because RADOS append is unavailable on erasure-coded pools. A later write of a listing, log or realisation changes nothing.

## NAR objects

A NAR is stored in one of two layouts, striped or chunked. The writer picks the layout; a reader finds out which one a NAR has by which objects exist, so both layouts coexist in a pool and a NAR is never stored in both. A NAR exists once its stripe 0 or its completed manifest exists.

## Striped NAR objects

A NAR is split into fixed-size stripes in the layout of Ceph's libradosstriper, restricted to one object per stripe (`stripe_count` 1, `stripe_unit` equal to `object_size`). Stripe `i` is the object `nar/<name>.<i>` with `i` as sixteen lowercase hex digits, and holds bytes `i·S` up to `(i+1)·S` of the file verbatim, where `S` is the stripe size the writer chose. Only the last stripe is shorter. `rados --striper get` reads a whole NAR.

Stripe 0 carries the xattrs:

| xattr                         | value                                   |
|-------------------------------|-----------------------------------------|
| `striper.layout.stripe_unit`  | `S`                                     |
| `striper.layout.stripe_count` | `1`                                     |
| `striper.layout.object_size`  | `S`                                     |
| `striper.size`                | total length of the NAR in bytes        |
| `created`                     | see below                               |
| `upstream`                    | see below; absent for uploaded objects  |
| `access_count`                | see below                               |
| `accessed`                    | see below                               |

Readers take `S` from `striper.layout.object_size`, never from their own configuration, so NARs written under a different stripe size stay readable.

Stripes 1 and up are written first, each as one full-object write. Stripe 0 is written last, exclusively, with its xattrs and data in a single operation, and is the completion marker: a NAR exists once stripe 0 exists, and a reader never sees a partial NAR. An upload of a NAR whose stripe 0 already exists writes nothing, so stripes laid out under one stripe size are never partly rewritten under another; two writers racing to upload the same new NAR must share a stripe size. Stripes without a stripe 0 are leftovers from a failed upload; nothing reads them.

`S` must not exceed the cluster's `osd_max_object_size`, and `S` plus the xattrs written with stripe 0 must fit within `osd_max_write_size`; a stripe the OSD refuses fails the upload.

## Chunked NAR objects

The uncompressed NAR is cut into content-defined chunks. Each chunk is stored as the object `chunk/<sha256>`, where `<sha256>` is the SHA-256 of the chunk's uncompressed bytes, holding one zstd frame that decompresses to the chunk. Chunk objects are shared by every NAR that contains the same bytes and carry no xattrs a reader needs. A chunk is written once and never rewritten; a writer that finds it present only refreshes its mtime and records the length it finds.

The manifest `nar/<name>` lists the chunks in order as fixed 40-byte records: the 32-byte SHA-256, the uncompressed length and the compressed length, both unsigned 32-bit little-endian. It carries the xattrs:

| xattr          | value                                                                 |
|----------------|-----------------------------------------------------------------------|
| `codec`        | `none` or `zstd`: how the NAR was uploaded and how it is served       |
| `striper.size` | length in bytes of the served NAR                                     |
| `created`, `upstream`, `access_count`, `accessed` | as for stripe 0                    |

A NAR with codec `none` is served as the concatenation of the decompressed chunks and `striper.size` is the sum of the uncompressed lengths. A NAR with codec `zstd` is served as the concatenation of the stored frames, which is a valid zstd stream decoding to the NAR, and `striper.size` is the sum of the compressed lengths; the served bytes differ from the bytes the client uploaded. A NAR uploaded under any other compression is stored striped.

The chunks are written first, then the manifest with its xattrs in a single operation; the manifest is complete once it carries `striper.size`, and a reader treats a manifest without it as absent. While an upload is in flight the writer rewrites the manifest without `striper.size` after every `S` bytes of input, where `S` is its stripe size, so the manifest's mtime shows the upload is alive. Two writers uploading the same NAR produce the same chunks and prefix-compatible manifests, so neither corrupts the other. Chunks listed by no complete manifest are leftovers from a failed upload.

The chunker is FastCDC with normalised chunking level 1 over a 64-bit gear hash. The gear table entry for byte value `i` is the first eight bytes, little-endian, of the SHA-256 of the single byte `i`. For an average chunk size `A`, a power of two with `b = log2(A)`, the minimum is `A/4` and the maximum `4·A`. The hash starts at zero for each chunk; for each byte after the first `A/4`, `h = h·2 + gear[byte]` modulo 2^64, and the chunk ends after that byte when the top `b+1` bits of `h` are zero before offset `A` or the top `b-1` bits are zero from offset `A` on. A chunk is cut at `4·A` bytes if no boundary came first, and the last chunk ends at the end of the NAR. Only writers using the same `A` share chunks; any `A` keeps every NAR readable. A chunk the OSD refuses fails the upload.

## Access metadata

These xattrs live on a plain object or on stripe 0 of a NAR. `created` is when the object was written and never changes. `access_count` is a decimal ASCII count of requests that touched the object and `accessed` is the time of the latest; creation is the first, and every later successful read or write of the same name adds one. Times are RFC 3339 in UTC to the second. The stamp is written only if the object still exists, so it never recreates one a collector has just deleted. The count is best-effort: concurrent requests can lose increments. Both are activity signals for garbage collection, not download tallies; a client fetches a narinfo several times per download and checks it again when pushing.

The object's own RADOS mtime is updated by these xattr writes, so it records when the object was last touched, not when its content was written.

`upstream` is the base URL of the cache the object was pulled from. The bytes are stored exactly as that cache served them, signatures included.

## Garbage collection

A collector is a separate process on the pool that runs with or without servers and takes no locks. It deletes nothing whose mtime is within a grace period, which keeps uploads and re-uploads in flight safe. Past the grace period, a plain object older than an optional expiry is deleted on age alone; a listing whose narinfo is gone is deleted; a NAR is deleted when no remaining narinfo's `URL:` names it, stripe 0 or manifest first; the stripes of a NAR that has no stripe 0 are deleted together once the newest of them is past the grace period, so an upload still writing its stripes is left alone; a manifest without `striper.size` is deleted once it is past the grace period; a chunk is deleted when no manifest that survives the run lists it, so the chunks of an upload in flight are kept by its rewritten manifest and the chunks of a NAR deleted in the same run go with it. Removing a store path for any other reason means deleting its narinfo; the listing and the NAR follow by those rules. Every delete is conditional on the object version the collector observed, so an object a request touches during the run survives it. Foreign names are never deleted.

## Integrity

A chunk's name is the SHA-256 of its uncompressed bytes, and a reader serving a NAR with codec `none` verifies each chunk against it as it decompresses and stops at a mismatch; a NAR with codec `zstd` is served unverified. No other checksums are stored or verified beyond those inside the narinfo, against which Nix clients verify every NAR.

## Inspecting with the rados CLI

Substitute the cache's pool.

```sh
rados -p nixcache ls
rados -p nixcache --striper get 'nar/<name>' nar.xz
rados -p nixcache getxattr 'nar/<name>.0000000000000000' striper.size
rados -p nixcache getxattr 'nar/<name>.0000000000000000' access_count
rados -p nixcache getxattr 'nar/<name>' codec
rados -p nixcache get 'nar/<name>' - | xxd | head
rados -p nixcache get 'chunk/<sha256>' - | zstd -d | head -c 64
rados -p nixcache get '<hash>.narinfo' - | head
rados -p nixcache get '<hash>.ls' -
```
