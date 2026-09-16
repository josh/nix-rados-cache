# Testing

Tests are end-to-end testscript scripts in `testdata/`. Do not add Go unit tests; new behavior gets covered by a txtar script exercising the real server over HTTP.

Requires `ceph-mon`, `ceph-osd`, `librados-dev`, and `nix`. The harness starts a throwaway Ceph cluster; the first startup attempt is occasionally flaky and is retried automatically.

```sh
go test -cover -timeout 20m .
```

## Coverage

Total coverage must not decrease. Before pushing, measure it and compare against the parent revision:

```sh
go test -cover -timeout 20m -coverprofile=cover.out .
go tool cover -func=cover.out
```

The server runs as a subprocess of the test binary; it exits cleanly on SIGINT so its coverage is collected. If coverage reports 0%, that flush is broken.

Cover the supported minimal surface (PUT/GET, name validation, the size limit, error mapping) well. Do not add tests for features out of scope for v0.1 or for unreachable error branches (`rados.NewConn` failure, `ListenAndServe` failure, the 400/500 store-error paths).
