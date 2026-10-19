package main

import (
	"cmp"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/klauspost/compress/zstd"
)

const (
	cacheInfo        = "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40\n"
	maxNarinfoSize   = 16 * 1024 * 1024
	encodeMinSize    = 1024
	readPiece        = 1024 * 1024
	accessCountXattr = "access_count"
	accessedXattr    = "accessed"
	createdXattr     = "created"
	upstreamXattr    = "upstream"
	sizeXattr        = "striper.size"
	stripeSizeXattr  = "striper.layout.object_size"
)

var (
	objectNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	sigLinePattern    = regexp.MustCompile(`^Sig: [^:\s]+:[A-Za-z0-9+/=]+$`)
	plainPattern      = regexp.MustCompile(`^(log/|build-trace-v2/[A-Za-z0-9._-]+/)?[A-Za-z0-9._-]+$`)
	stripePattern     = regexp.MustCompile(`^nar/[A-Za-z0-9._-]+\.[0-9a-f]{16}$`)
	zstdEncoder, _    = zstd.NewWriter(nil)
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "gc" {
		gc(os.Args[2:])
		return
	}
	listen := flag.String("listen", "127.0.0.1:8080", "TCP address to listen on")
	pool := flag.String("pool", "", "RADOS pool name")
	stripeSize := flag.Int("stripe-size", 16*1024*1024, "bytes per RADOS object for NARs")
	caDerivations := flag.Bool("ca-derivations", false, "store realisations of content-addressed derivations")
	logFile := flag.String("log-file", "", "append logs to this file instead of stderr")
	shutdownTimeout := flag.Duration("shutdown-timeout", 30*time.Second, "how long in-flight requests may finish after a shutdown signal")
	ioTimeout := flag.Duration("io-timeout", 30*time.Second, "longest pause allowed while reading a request body or writing a response")
	maxUploads := flag.Int("max-uploads", 8, "uploads handled at once; the rest wait")
	keys := trustedKeysFlag(flag.CommandLine)
	var upstreams []string
	flag.Func("upstream", "cache to pull misses from; repeatable, tried in order", func(s string) error {
		if u, err := url.Parse(s); err != nil || u.Host == "" {
			return fmt.Errorf("invalid upstream %q", s)
		}
		upstreams = append(upstreams, strings.TrimSuffix(s, "/"))
		return nil
	})
	flag.Parse()

	var logOut io.Writer = os.Stderr
	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintln(os.Stderr, "open log file:", err)
			os.Exit(1)
		}
		logOut = f
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(logOut, nil)))

	if *pool == "" {
		fmt.Fprintln(os.Stderr, "--pool is required")
		os.Exit(1)
	}
	if *stripeSize <= 0 {
		fmt.Fprintln(os.Stderr, "--stripe-size must be positive")
		os.Exit(1)
	}
	if *maxUploads <= 0 {
		fmt.Fprintln(os.Stderr, "--max-uploads must be positive")
		os.Exit(1)
	}

	_, ioctx, err := openPool(*pool)
	if err != nil {
		slog.Error("failed to open pool", "error", err)
		os.Exit(1)
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		slog.Error("listen", "error", err)
		os.Exit(1)
	}
	srv := &http.Server{Handler: newHandler(ioctx, *stripeSize, *caDerivations, *maxUploads, upstreams, keys, *ioTimeout)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		<-ctx.Done()
		slog.Info("shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			slog.Error("shutdown", "error", err)
		}
	}()

	slog.Info("listening", "address", *listen, "pool", *pool, "stripe_size", *stripeSize, "ca_derivations", *caDerivations, "upstreams", upstreams, "trusted_keys", len(keys))
	if err := srv.Serve(idleListener{ln, *ioTimeout}); !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
	<-done
}

func gc(args []string) {
	fs := flag.NewFlagSet("gc", flag.ExitOnError)
	pool := fs.String("pool", "", "RADOS pool name")
	minAge := fs.Duration("min-age", time.Hour, "never delete objects touched within this long")
	maxAge := fs.Duration("max-age", 0, "delete objects untouched for this long; 0 disables")
	var minFree, maxFree int64
	fs.Func("min-free", "evict store paths while the pool's free space is below this; K, M, G and T suffixes", func(s string) (err error) { minFree, err = parseSize(s); return })
	fs.Func("max-free", "stop evicting once free space reaches this; defaults to --min-free", func(s string) (err error) { maxFree, err = parseSize(s); return })
	priority := fs.String("priority", "oldest", "evict oldest or largest store paths first")
	dryRun := fs.Bool("dry-run", false, "report what would be deleted without deleting")
	keys := trustedKeysFlag(fs)
	_ = fs.Parse(args)
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if *pool == "" || (*priority != "oldest" && *priority != "largest") {
		fmt.Fprintln(os.Stderr, "--pool is required; --priority must be oldest or largest")
		os.Exit(1)
	}
	conn, ioctx, err := openPool(*pool)
	if err != nil {
		slog.Error("failed to open pool", "error", err)
		os.Exit(1)
	}
	grace, expiry := time.Now().Add(-*minAge), time.Now().Add(-*maxAge)
	deleted, stored, freed := 0, int64(0), int64(0)

	remove := func(name string) bool {
		stat, err := ioctx.Stat(name)
		if err != nil || !stat.ModTime.Before(grace) {
			return false
		}
		version, _ := ioctx.GetLastVersion()
		if *dryRun {
			slog.Info("would delete", "object", name)
		} else {
			op := rados.CreateWriteOp()
			defer op.Release()
			op.AssertVersion(version)
			op.Remove()
			if err := op.Operate(ioctx, name, rados.OperationNoFlag); err != nil {
				slog.Warn("skipped", "object", name, "error", err)
				return false
			}
			slog.Info("deleted", "object", name)
		}
		deleted++
		return true
	}
	walk := func(visit func(name string)) {
		if err := ioctx.ListObjects(visit); err != nil {
			slog.Error("list pool", "error", err)
			os.Exit(1)
		}
	}

	type path struct {
		name, url string
		size      int64
		mtime     time.Time
	}
	var paths []path
	refs := map[string]int{}
	walk(func(name string) {
		if !plainPattern.MatchString(name) {
			return
		}
		stat, err := ioctx.Stat(name)
		if err != nil || (*maxAge > 0 && stat.ModTime.Before(expiry) && remove(name)) {
			return
		}
		stored += int64(stat.Size)
		if !strings.HasSuffix(name, ".narinfo") {
			return
		}
		data, err := getObject(ioctx, name, new(int))
		if errors.Is(err, rados.ErrNotFound) {
			return
		}
		if err != nil {
			slog.Error("read narinfo", "object", name, "error", err)
			os.Exit(1)
		}
		if !trustedSig(keys, name, data) && remove(name) {
			return
		}
		p := path{name: name, mtime: stat.ModTime}
		for line := range strings.Lines(string(data)) {
			if v, ok := strings.CutPrefix(line, "URL: "); ok {
				p.url = strings.TrimSpace(v)
			}
			if v, ok := strings.CutPrefix(line, "FileSize: "); ok {
				p.size, _ = strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			}
		}
		if refs[p.url]++; refs[p.url] == 1 {
			stored += p.size
		}
		paths = append(paths, p)
	})
	if minFree > 0 {
		quota, err := poolQuota(conn, *pool)
		if err != nil {
			slog.Error("pool quota", "error", err)
			os.Exit(1)
		}
		free := quota - stored
		slog.Info("gc", "pool", *pool, "quota", quota, "stored", stored, "free", free)
		if free < minFree {
			slices.SortFunc(paths, func(a, b path) int {
				if *priority == "largest" {
					return cmp.Compare(b.size, a.size)
				}
				return a.mtime.Compare(b.mtime)
			})
			for _, p := range paths {
				if free+freed >= max(maxFree, minFree) {
					break
				}
				if remove(p.name) {
					if refs[p.url]--; refs[p.url] == 0 {
						freed += p.size
					}
				}
			}
		}
	}
	head := stripeName("", 0)
	walk(func(name string) {
		switch {
		case stripePattern.MatchString(name) && strings.HasSuffix(name, head) && refs[strings.TrimSuffix(name, head)] == 0:
			remove(name)
		case plainPattern.MatchString(name) && strings.HasSuffix(name, ".ls"):
			if _, err := ioctx.Stat(strings.TrimSuffix(name, ".ls") + ".narinfo"); errors.Is(err, rados.ErrNotFound) {
				remove(name)
			}
		}
	})
	// An upload writes its tail stripes over minutes or hours before stripe 0, so a headless NAR is judged by its newest stripe.
	orphans, active := map[string][]string{}, map[string]bool{}
	walk(func(name string) {
		if !stripePattern.MatchString(name) || strings.HasSuffix(name, head) {
			return
		}
		nar := name[:len(name)-len(head)]
		if _, err := ioctx.Stat(stripeName(nar, 0)); !errors.Is(err, rados.ErrNotFound) {
			return
		}
		if stat, err := ioctx.Stat(name); err == nil && !stat.ModTime.Before(grace) {
			active[nar] = true
		}
		orphans[nar] = append(orphans[nar], name)
	})
	for nar, names := range orphans {
		if !active[nar] {
			for _, name := range names {
				remove(name)
			}
		}
	}
	slog.Info("gc", "deleted", deleted, "freed", freed)
}

func trustedKeysFlag(fs *flag.FlagSet) map[string]ed25519.PublicKey {
	keys := map[string]ed25519.PublicKey{}
	fs.Func("trusted-public-key", "name:base64 of a key whose signature every narinfo must carry; repeatable, none accepts all", func(s string) error {
		name, b64, _ := strings.Cut(s, ":")
		key, err := base64.StdEncoding.DecodeString(b64)
		if err != nil || name == "" || len(key) != ed25519.PublicKeySize {
			return fmt.Errorf("invalid trusted public key %q", s)
		}
		keys[name] = key
		return nil
	})
	return keys
}

func trustedSig(keys map[string]ed25519.PublicKey, name string, narinfo []byte) bool {
	if len(keys) == 0 {
		return true
	}
	var path, hash, size string
	var refs, sigs []string
	for line := range strings.Lines(string(narinfo)) {
		switch k, v, _ := strings.Cut(strings.TrimSuffix(line, "\n"), ": "); k {
		case "StorePath":
			path = v
		case "NarHash":
			hash = v
		case "NarSize":
			size = v
		case "References":
			refs = strings.Fields(v)
		case "Sig":
			sigs = append(sigs, v)
		}
	}
	dir := path[:strings.LastIndex(path, "/")+1]
	if !strings.HasPrefix(path[len(dir):], strings.TrimSuffix(name, ".narinfo")+"-") {
		return false
	}
	for i := range refs {
		refs[i] = dir + refs[i]
	}
	fingerprint := []byte("1;" + path + ";" + hash + ";" + size + ";" + strings.Join(refs, ","))
	keys = maps.Clone(keys)
	for _, s := range sigs {
		n, b64, _ := strings.Cut(s, ":")
		sig, err := base64.StdEncoding.DecodeString(b64)
		if key, ok := keys[n]; ok && err == nil && ed25519.Verify(key, fingerprint, sig) {
			return true
		}
		delete(keys, n)
	}
	return false
}

func parseSize(s string) (int64, error) {
	units := map[string]int64{"": 1, "K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40}
	suffix := strings.TrimLeft(s, "0123456789")
	n, err := strconv.ParseInt(strings.TrimSuffix(s, suffix), 10, 64)
	if err != nil || units[suffix] == 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return n * units[suffix], nil
}

func poolQuota(conn *rados.Conn, pool string) (int64, error) {
	var dump struct {
		Pools []struct {
			Name     string `json:"pool_name"`
			MaxBytes int64  `json:"quota_max_bytes"`
		}
	}
	out, _, err := conn.MonCommand([]byte(`{"prefix":"osd dump","format":"json"}`))
	if err == nil {
		err = json.Unmarshal(out, &dump)
	}
	if err != nil {
		return 0, err
	}
	for _, p := range dump.Pools {
		if p.Name == pool && p.MaxBytes > 0 {
			return p.MaxBytes, nil
		}
	}
	return 0, fmt.Errorf("pool %q has no byte quota", pool)
}

func openPool(pool string) (*rados.Conn, *rados.IOContext, error) {
	conn, err := rados.NewConn()
	if err != nil {
		return nil, nil, fmt.Errorf("create connection: %w", err)
	}
	if err := conn.ReadDefaultConfigFile(); err != nil {
		return nil, nil, fmt.Errorf("read ceph config: %w", err)
	}
	if err := conn.ParseDefaultConfigEnv(); err != nil {
		return nil, nil, fmt.Errorf("parse CEPH_ARGS: %w", err)
	}
	if err := conn.Connect(); err != nil {
		return nil, nil, fmt.Errorf("connect to ceph: %w", err)
	}
	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		return nil, nil, fmt.Errorf("open pool %q: %w", pool, err)
	}
	return conn, ioctx, nil
}

func getObject(ioctx *rados.IOContext, name string, calls *int) ([]byte, error) {
	for {
		*calls++
		stat, err := ioctx.Stat(name)
		if err != nil {
			return nil, err
		}
		data := make([]byte, stat.Size+1)
		*calls++
		n, err := ioctx.Read(name, data, 0)
		if err != nil {
			return nil, err
		}
		if uint64(n) <= stat.Size {
			return data[:n], nil
		}
	}
}

func now() []byte {
	return []byte(time.Now().UTC().Format(time.RFC3339))
}

func stampNew(op *rados.WriteOp, upstream string) {
	op.Create(rados.CreateExclusive)
	op.SetXattr(createdXattr, now())
	stampAccess(op, 1)
	if upstream != "" {
		op.SetXattr(upstreamXattr, []byte(upstream))
	}
}

func putObject(ioctx *rados.IOContext, name string, data []byte, upstream string, calls *int) error {
	op := rados.CreateWriteOp()
	defer op.Release()
	stampNew(op, upstream)
	op.WriteFull(data)
	*calls++
	return op.Operate(ioctx, name, rados.OperationNoFlag)
}

func sigLines(narinfo []byte) []string {
	var sigs []string
	for line := range strings.Lines(string(narinfo)) {
		if line = strings.TrimSuffix(line, "\n"); strings.HasPrefix(line, "Sig: ") {
			sigs = append(sigs, line)
		}
	}
	return sigs
}

func appendSigs(ioctx *rados.IOContext, name string, sigs []string, calls *int) (err error) {
	for attempt := 1; ; attempt++ {
		var stored []byte
		if stored, err = getObject(ioctx, name, calls); err != nil {
			return err
		}
		version, _ := ioctx.GetLastVersion()
		existing := sigLines(stored)
		add := slices.DeleteFunc(slices.Clone(sigs), func(s string) bool { return slices.Contains(existing, s) })
		if len(add) == 0 {
			return nil
		}
		merged := append(stored, strings.Join(add, "\n")+"\n"...)
		if len(merged) > maxNarinfoSize {
			return &http.MaxBytesError{Limit: maxNarinfoSize}
		}
		op := rados.CreateWriteOp()
		op.AssertVersion(version)
		op.WriteFull(merged)
		*calls++
		err = op.Operate(ioctx, name, rados.OperationNoFlag)
		op.Release()
		if err == nil || attempt == 3 {
			return err
		}
	}
}

func stripeName(name string, i int) string {
	return fmt.Sprintf("%s.%016x", name, i)
}

func putNAR(ioctx *rados.IOContext, name string, body io.Reader, stripeSize int, upstream string, calls *int) error {
	var first []byte
	buf := make([]byte, stripeSize)
	total := 0
	for i := 0; ; i++ {
		n, err := 0, error(nil)
		for n < stripeSize && err == nil {
			var m int
			m, err = body.Read(buf[n:])
			n += m
		}
		if err != nil && err != io.EOF {
			return err
		}
		total += n
		if i == 0 {
			first, buf = buf[:n], make([]byte, stripeSize)
		} else if n > 0 {
			*calls++
			if err := ioctx.WriteFull(stripeName(name, i), buf[:n]); err != nil {
				return err
			}
		}
		if n < stripeSize {
			break
		}
	}
	size := []byte(strconv.Itoa(stripeSize))
	op := rados.CreateWriteOp()
	defer op.Release()
	stampNew(op, upstream)
	op.SetXattr("striper.layout.stripe_unit", size)
	op.SetXattr("striper.layout.stripe_count", []byte("1"))
	op.SetXattr(stripeSizeXattr, size)
	op.SetXattr(sizeXattr, []byte(strconv.Itoa(total)))
	op.WriteFull(first)
	*calls++
	return op.Operate(ioctx, stripeName(name, 0), rados.OperationNoFlag)
}

func stampAccess(op *rados.WriteOp, count uint64) {
	op.SetXattr(accessCountXattr, []byte(strconv.FormatUint(count, 10)))
	op.SetXattr(accessedXattr, now())
}

func (h *handler) setAccess(name string, s *reqStats) {
	s.radosCalls += 2
	xattrs, _ := h.ioctx.ListXattrs(name)
	count, _ := strconv.ParseUint(string(xattrs[accessCountXattr]), 10, 64)
	count++
	op := rados.CreateWriteOp()
	defer op.Release()
	op.AssertExists()
	stampAccess(op, count)
	if err := op.Operate(h.ioctx, name, rados.OperationNoFlag); err != nil {
		slog.Error("access count", "object", name, "error", err)
	}
	s.accessCount = count
}

type idleConn struct {
	net.Conn
	timeout time.Duration
}

func (c idleConn) Read(p []byte) (int, error) {
	_ = c.SetReadDeadline(time.Now().Add(c.timeout))
	return c.Conn.Read(p)
}

func (c idleConn) Write(p []byte) (int, error) {
	_ = c.SetWriteDeadline(time.Now().Add(c.timeout))
	return c.Conn.Write(p)
}

type idleListener struct {
	net.Listener
	timeout time.Duration
}

func (l idleListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return idleConn{c, l.timeout}, nil
}

type handler struct {
	ioctx      *rados.IOContext
	stripeSize int
	uploads    chan struct{}
	upstreams  []string
	trusted    map[string]ed25519.PublicKey
	client     *http.Client
	ioTimeout  time.Duration
}

type stallGuard struct {
	io.ReadCloser
	timer   *time.Timer
	timeout time.Duration
	cancel  context.CancelFunc
}

func (g stallGuard) Read(p []byte) (int, error) {
	g.timer.Reset(g.timeout)
	defer g.timer.Stop()
	return g.ReadCloser.Read(p)
}

func (g stallGuard) Close() error {
	g.timer.Stop()
	g.cancel()
	return g.ReadCloser.Close()
}

func (h *handler) fetch(method, path string) (*http.Response, string) {
	for _, u := range h.upstreams {
		ctx, cancel := context.WithCancel(context.Background())
		timer := time.AfterFunc(h.ioTimeout, cancel)
		req, _ := http.NewRequestWithContext(ctx, method, u+"/"+path, nil)
		resp, err := h.client.Do(req)
		if err != nil {
			slog.Warn("upstream", "url", u, "path", path, "error", err)
		} else if resp.StatusCode == http.StatusOK {
			resp.Body = stallGuard{resp.Body, timer, h.ioTimeout, cancel}
			return resp, u
		} else {
			_ = resp.Body.Close()
		}
		timer.Stop()
		cancel()
	}
	return nil, ""
}

type clientWriter struct {
	w   io.Writer
	err error
}

func (c *clientWriter) Write(p []byte) (int, error) {
	if c.err == nil {
		_, c.err = c.w.Write(p)
	}
	return len(p), nil
}

func (h *handler) pullNAR(w http.ResponseWriter, r *http.Request, name string, s *reqStats) bool {
	h.uploads <- struct{}{}
	defer func() { <-h.uploads }()
	resp, u := h.fetch(r.Method, name)
	if resp == nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	s.upstream = u
	w.Header().Set("Content-Type", "application/x-nix-nar")
	if resp.ContentLength >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
	}
	if r.Method == http.MethodHead {
		return true
	}
	client := &clientWriter{w: w}
	err := putNAR(h.ioctx, name, io.TeeReader(resp.Body, client), h.stripeSize, u, &s.radosCalls)
	if err != nil && !errors.Is(err, rados.ErrObjectExists) {
		slog.Error("store pulled object", "object", name, "error", err)
		_, _ = io.Copy(client, resp.Body)
		return true
	}
	slog.Info("pulled", "object", name, "upstream", u)
	return true
}

func (h *handler) pullPlain(name string, s *reqStats) ([]byte, error) {
	resp, u := h.fetch(http.MethodGet, name)
	if resp == nil {
		return nil, rados.ErrNotFound
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxNarinfoSize))
	if err != nil {
		slog.Warn("upstream", "url", u, "object", name, "error", err)
		return nil, rados.ErrNotFound
	}
	if !trustedSig(h.trusted, name, body) {
		slog.Warn("untrusted upstream narinfo", "url", u, "object", name)
		return nil, rados.ErrNotFound
	}
	if err := putObject(h.ioctx, name, body, u, &s.radosCalls); err != nil && !errors.Is(err, rados.ErrObjectExists) {
		return nil, err
	}
	s.upstream = u
	return body, nil
}

func newHandler(ioctx *rados.IOContext, stripeSize int, caDerivations bool, maxUploads int, upstreams []string, trusted map[string]ed25519.PublicKey, ioTimeout time.Duration) http.Handler {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 5 * time.Second}).DialContext
	transport.ResponseHeaderTimeout = 10 * time.Second
	h := &handler{ioctx: ioctx, stripeSize: stripeSize, uploads: make(chan struct{}, maxUploads), upstreams: upstreams, trusted: trusted, client: &http.Client{Transport: transport}, ioTimeout: ioTimeout}
	mux := http.NewServeMux()
	if caDerivations {
		mux.HandleFunc("GET /build-trace-v2/{drv}/{output}", h.getObject)
		mux.HandleFunc("PUT /build-trace-v2/{drv}/{output}", h.putObject)
	}
	mux.HandleFunc("GET /nix-cache-info", h.getCacheInfo)
	mux.HandleFunc("PUT /nix-cache-info", func(http.ResponseWriter, *http.Request) {})
	mux.HandleFunc("GET /nar/{name}", h.getObject)
	mux.HandleFunc("PUT /nar/{name}", h.putObject)
	mux.HandleFunc("GET /log/{name}", h.getObject)
	mux.HandleFunc("PUT /log/{name}", h.putObject)
	mux.HandleFunc("GET /{name}", h.getObject)
	mux.HandleFunc("PUT /{name}", h.putObject)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		mux.ServeHTTP(sw, r)
		slog.Info("request", "method", r.Method, "path", r.URL.Path, "status", sw.status, "duration", time.Since(start),
			"req_bytes", r.ContentLength, "resp_bytes", sw.bytes, "rados_calls", sw.stats.radosCalls, "access_count", sw.stats.accessCount, "upstream", sw.stats.upstream)
	})
}

type reqStats struct {
	radosCalls  int
	accessCount uint64
	upstream    string
}

func stats(w http.ResponseWriter) *reqStats {
	return &w.(*statusWriter).stats
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
	stats  reqStats
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}

func (h *handler) getCacheInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/x-nix-cache-info")
	_, _ = io.WriteString(w, cacheInfo)
}

func objectName(r *http.Request) (string, bool) {
	if strings.HasPrefix(r.URL.Path, "/build-trace-v2/") {
		drv, output := r.PathValue("drv"), r.PathValue("output")
		if !objectNamePattern.MatchString(drv) || !objectNamePattern.MatchString(output) {
			return "", false
		}
		return "build-trace-v2/" + drv + "/" + output, strings.HasSuffix(output, ".doi")
	}
	name := r.PathValue("name")
	if !objectNamePattern.MatchString(name) {
		return "", false
	}
	if strings.HasPrefix(r.URL.Path, "/nar/") {
		return "nar/" + name, true
	}
	if strings.HasPrefix(r.URL.Path, "/log/") {
		return "log/" + name, true
	}
	return name, strings.HasSuffix(name, ".narinfo") || strings.HasSuffix(name, ".ls")
}

func (h *handler) getObject(w http.ResponseWriter, r *http.Request) {
	name, ok := objectName(r)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if strings.HasPrefix(name, "nar/") {
		h.getNAR(w, r, name)
		return
	}
	s := stats(w)
	data, err := getObject(h.ioctx, name, &s.radosCalls)
	if errors.Is(err, rados.ErrNotFound) && strings.HasSuffix(name, ".narinfo") {
		data, err = h.pullPlain(name, s)
	}
	if err == nil && strings.HasSuffix(name, ".narinfo") && !trustedSig(h.trusted, name, data) {
		slog.Warn("untrusted narinfo", "object", name)
		err = rados.ErrNotFound
	}
	if err != nil {
		writeStoreError(w, name, err)
		return
	}
	h.setAccess(name, s)
	contentType := "text/x-nix-narinfo"
	switch {
	case strings.HasSuffix(name, ".ls") || strings.HasPrefix(name, "build-trace-v2/"):
		contentType = "application/json"
	case strings.HasPrefix(name, "log/"):
		contentType = "text/plain"
	}
	if len(data) >= encodeMinSize && strings.Contains(r.Header.Get("Accept-Encoding"), "zstd") {
		data = zstdEncoder.EncodeAll(data, nil)
		w.Header().Set("Content-Encoding", "zstd")
	}
	w.Header().Set("Vary", "Accept-Encoding")
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	_, _ = w.Write(data)
}

func (h *handler) getNAR(w http.ResponseWriter, r *http.Request, name string) {
	s := stats(w)
	head := stripeName(name, 0)
	s.radosCalls++
	xattrs, err := h.ioctx.ListXattrs(head)
	if errors.Is(err, rados.ErrNotFound) && h.pullNAR(w, r, name, s) {
		return
	}
	if err != nil {
		writeStoreError(w, name, err)
		return
	}
	size, _ := strconv.ParseUint(string(xattrs[sizeXattr]), 10, 64)
	stripeSize, _ := strconv.Atoi(string(xattrs[stripeSizeXattr]))
	if stripeSize <= 0 {
		writeStoreError(w, name, fmt.Errorf("bad %s xattr %q", stripeSizeXattr, xattrs[stripeSizeXattr]))
		return
	}
	h.setAccess(head, s)
	w.Header().Set("Content-Type", "application/x-nix-nar")
	w.Header().Set("Content-Length", strconv.FormatUint(size, 10))
	if r.Method == http.MethodHead {
		return
	}
	buf := make([]byte, min(readPiece, stripeSize))
	for off := uint64(0); off < size; {
		s.radosCalls++
		n, err := h.ioctx.Read(stripeName(name, int(off/uint64(stripeSize))), buf, off%uint64(stripeSize))
		if err != nil || n == 0 {
			slog.Error("read stripe", "object", name, "offset", off, "error", err)
			return
		}
		if _, err := w.Write(buf[:n]); err != nil {
			slog.Error("write response", "object", name, "error", err)
			return
		}
		off += uint64(n)
	}
}

func (h *handler) putObject(w http.ResponseWriter, r *http.Request) {
	name, ok := objectName(r)
	if !ok {
		http.NotFound(w, r)
		return
	}
	h.uploads <- struct{}{}
	defer func() { <-h.uploads }()
	s := stats(w)
	var err error
	if strings.HasPrefix(name, "nar/") {
		err = putNAR(h.ioctx, name, r.Body, h.stripeSize, "", &s.radosCalls)
	} else {
		data, rerr := io.ReadAll(http.MaxBytesReader(w, r.Body, maxNarinfoSize))
		var tooBig *http.MaxBytesError
		if errors.As(rerr, &tooBig) {
			http.Error(w, "object too large", http.StatusRequestEntityTooLarge)
			return
		}
		if rerr != nil {
			http.Error(w, rerr.Error(), http.StatusBadRequest)
			return
		}
		var sigs []string
		if strings.HasSuffix(name, ".narinfo") {
			sigs = sigLines(data)
		}
		if slices.ContainsFunc(sigs, func(s string) bool { return !sigLinePattern.MatchString(s) }) {
			http.Error(w, "malformed Sig line", http.StatusBadRequest)
			return
		}
		if strings.HasSuffix(name, ".narinfo") && (len(data) == 0 || data[len(data)-1] != '\n') {
			http.Error(w, "narinfo must end with a newline", http.StatusBadRequest)
			return
		}
		if strings.HasSuffix(name, ".narinfo") && !trustedSig(h.trusted, name, data) {
			http.Error(w, "narinfo lacks a signature by a trusted key", http.StatusForbidden)
			return
		}
		err = putObject(h.ioctx, name, data, "", &s.radosCalls)
		if errors.Is(err, rados.ErrObjectExists) && len(sigs) > 0 {
			err = cmp.Or(appendSigs(h.ioctx, name, sigs, &s.radosCalls), err)
		}
	}
	switch {
	case errors.Is(err, rados.ErrObjectExists):
		if strings.HasPrefix(name, "nar/") {
			name = stripeName(name, 0)
		}
		h.setAccess(name, s)
	case errors.As(err, new(*http.MaxBytesError)):
		http.Error(w, "object too large", http.StatusRequestEntityTooLarge)
	case err != nil:
		writeStoreError(w, name, err)
	default:
		w.WriteHeader(http.StatusCreated)
	}
}

func writeStoreError(w http.ResponseWriter, name string, err error) {
	if errors.Is(err, rados.ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	slog.Error("rados error", "object", name, "error", err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
