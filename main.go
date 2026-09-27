package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
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
)

const (
	cacheInfo        = "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40\n"
	maxNarinfoSize   = 16 * 1024 * 1024
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
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "TCP address to listen on")
	pool := flag.String("pool", "", "RADOS pool name")
	stripeSize := flag.Int("stripe-size", 16*1024*1024, "bytes per RADOS object for NARs")
	caDerivations := flag.Bool("ca-derivations", false, "store realisations of content-addressed derivations")
	logFile := flag.String("log-file", "", "append logs to this file instead of stderr")
	shutdownTimeout := flag.Duration("shutdown-timeout", 30*time.Second, "how long in-flight requests may finish after a shutdown signal")
	ioTimeout := flag.Duration("io-timeout", 30*time.Second, "longest pause allowed while reading a request body or writing a response")
	maxUploads := flag.Int("max-uploads", 8, "uploads handled at once; the rest wait")
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

	ioctx, err := openPool(*pool)
	if err != nil {
		slog.Error("failed to open pool", "error", err)
		os.Exit(1)
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		slog.Error("listen", "error", err)
		os.Exit(1)
	}
	srv := &http.Server{Handler: newHandler(ioctx, *stripeSize, *caDerivations, *maxUploads, upstreams, *ioTimeout)}
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

	slog.Info("listening", "address", *listen, "pool", *pool, "stripe_size", *stripeSize, "ca_derivations", *caDerivations, "upstreams", upstreams)
	if err := srv.Serve(idleListener{ln, *ioTimeout}); !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
	<-done
}

func openPool(pool string) (*rados.IOContext, error) {
	conn, err := rados.NewConn()
	if err != nil {
		return nil, fmt.Errorf("create connection: %w", err)
	}
	if err := conn.ReadDefaultConfigFile(); err != nil {
		return nil, fmt.Errorf("read ceph config: %w", err)
	}
	if err := conn.ParseDefaultConfigEnv(); err != nil {
		return nil, fmt.Errorf("parse CEPH_ARGS: %w", err)
	}
	if err := conn.Connect(); err != nil {
		return nil, fmt.Errorf("connect to ceph: %w", err)
	}
	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		return nil, fmt.Errorf("open pool %q: %w", pool, err)
	}
	return ioctx, nil
}

func getObject(ioctx *rados.IOContext, name string, calls *int) ([]byte, error) {
	*calls++
	stat, err := ioctx.Stat(name)
	if err != nil {
		return nil, err
	}
	data := make([]byte, stat.Size)
	*calls++
	n, err := ioctx.Read(name, data, 0)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func now() []byte {
	return []byte(time.Now().UTC().Format(time.RFC3339))
}

func putObject(ioctx *rados.IOContext, name string, data []byte, upstream string, calls *int) error {
	op := rados.CreateWriteOp()
	defer op.Release()
	op.Create(rados.CreateExclusive)
	op.SetXattr(createdXattr, now())
	stampAccess(op, 1)
	if upstream != "" {
		op.SetXattr(upstreamXattr, []byte(upstream))
	}
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

func appendSigs(ioctx *rados.IOContext, name string, sigs []string, calls *int) error {
	stored, err := getObject(ioctx, name, calls)
	if err != nil {
		return err
	}
	version, _ := ioctx.GetLastVersion()
	existing := sigLines(stored)
	add := slices.DeleteFunc(slices.Clone(sigs), func(s string) bool { return slices.Contains(existing, s) })
	if len(add) == 0 {
		return nil
	}
	op := rados.CreateWriteOp()
	defer op.Release()
	op.AssertVersion(version)
	op.WriteFull(append(stored, strings.Join(add, "\n")+"\n"...))
	*calls++
	return op.Operate(ioctx, name, rados.OperationNoFlag)
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
	op.Create(rados.CreateExclusive)
	op.SetXattr("striper.layout.stripe_unit", size)
	op.SetXattr("striper.layout.stripe_count", []byte("1"))
	op.SetXattr(stripeSizeXattr, size)
	op.SetXattr(sizeXattr, []byte(strconv.Itoa(total)))
	op.SetXattr(createdXattr, now())
	stampAccess(op, 1)
	if upstream != "" {
		op.SetXattr(upstreamXattr, []byte(upstream))
	}
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
	w.WriteHeader(http.StatusOK)
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

func (h *handler) pullPlain(name string, s *reqStats) error {
	resp, u := h.fetch(http.MethodGet, name)
	if resp == nil {
		return rados.ErrNotFound
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxNarinfoSize))
	if err == nil {
		err = putObject(h.ioctx, name, body, u, &s.radosCalls)
	}
	if err != nil && !errors.Is(err, rados.ErrObjectExists) {
		return err
	}
	s.upstream = u
	return nil
}

func newHandler(ioctx *rados.IOContext, stripeSize int, caDerivations bool, maxUploads int, upstreams []string, ioTimeout time.Duration) http.Handler {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 10 * time.Second
	h := &handler{ioctx: ioctx, stripeSize: stripeSize, uploads: make(chan struct{}, maxUploads), upstreams: upstreams, client: &http.Client{Transport: transport}, ioTimeout: ioTimeout}
	mux := http.NewServeMux()
	if caDerivations {
		mux.HandleFunc("GET /build-trace-v2/{drv}/{output}", h.getObject)
		mux.HandleFunc("PUT /build-trace-v2/{drv}/{output}", h.putObject)
	}
	mux.HandleFunc("GET /nix-cache-info", h.getCacheInfo)
	mux.HandleFunc("PUT /nix-cache-info", h.putCacheInfo)
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
	w.Header().Set("Content-Length", strconv.Itoa(len(cacheInfo)))
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, cacheInfo)
}

func (h *handler) putCacheInfo(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
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
		if err = h.pullPlain(name, s); err == nil {
			data, err = getObject(h.ioctx, name, &s.radosCalls)
		}
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
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
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
	w.WriteHeader(http.StatusOK)
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
		w.WriteHeader(http.StatusOK)
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
