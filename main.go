package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ceph/go-ceph/rados"
)

const (
	cacheInfo        = "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40\n"
	maxNarinfoSize   = 16 * 1024 * 1024
	accessCountXattr = "access_count"
	accessedXattr    = "accessed"
	createdXattr     = "created"
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
	logFile := flag.String("log-file", "", "append logs to this file instead of stderr")
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

	ioctx, err := openPool(*pool)
	if err != nil {
		slog.Error("failed to open pool", "error", err)
		os.Exit(1)
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		slog.Info("shutting down")
		os.Exit(0)
	}()

	slog.Info("listening", "address", *listen, "pool", *pool, "stripe_size", *stripeSize)
	if err := http.ListenAndServe(*listen, newHandler(ioctx, *stripeSize)); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
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

func putObject(ioctx *rados.IOContext, name string, data []byte, calls *int) error {
	op := rados.CreateWriteOp()
	defer op.Release()
	op.Create(rados.CreateExclusive)
	op.SetXattr(createdXattr, now())
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

func putNAR(ioctx *rados.IOContext, name string, body io.Reader, stripeSize int, calls *int) error {
	var first []byte
	buf := make([]byte, stripeSize)
	total := 0
	for i := 0; ; i++ {
		n, err := io.ReadFull(body, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
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
	op.WriteFull(first)
	*calls++
	return op.Operate(ioctx, stripeName(name, 0), rados.OperationNoFlag)
}

func setAccess(ioctx *rados.IOContext, name string, prev []byte, calls *int) (uint64, error) {
	count, _ := strconv.ParseUint(string(prev), 10, 64)
	count++
	op := rados.CreateWriteOp()
	defer op.Release()
	op.SetXattr(accessCountXattr, []byte(strconv.FormatUint(count, 10)))
	op.SetXattr(accessedXattr, now())
	*calls++
	return count, op.Operate(ioctx, name, rados.OperationNoFlag)
}

type handler struct {
	ioctx      *rados.IOContext
	stripeSize int
}

func newHandler(ioctx *rados.IOContext, stripeSize int) http.Handler {
	h := &handler{ioctx: ioctx, stripeSize: stripeSize}
	mux := http.NewServeMux()
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
		var s reqStats
		mux.ServeHTTP(sw, r.WithContext(context.WithValue(r.Context(), ctxKey{}, &s)))
		attrs := []any{"method", r.Method, "path", r.URL.Path, "status", sw.status, "duration", time.Since(start),
			"req_bytes", r.ContentLength, "resp_bytes", sw.bytes, "rados_calls", s.radosCalls}
		if s.accessCount > 0 {
			attrs = append(attrs, "access_count", s.accessCount)
		}
		slog.Info("request", attrs...)
	})
}

type ctxKey struct{}

type reqStats struct {
	radosCalls  int
	accessCount uint64
}

func stats(r *http.Request) *reqStats {
	return r.Context().Value(ctxKey{}).(*reqStats)
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
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
	data, err := getObject(h.ioctx, name, &stats(r).radosCalls)
	if err != nil {
		writeStoreError(w, name, err)
		return
	}
	if r.Method == http.MethodGet {
		s := stats(r)
		s.radosCalls++
		xattrs, _ := h.ioctx.ListXattrs(name)
		count, err := setAccess(h.ioctx, name, xattrs[accessCountXattr], &s.radosCalls)
		if err != nil {
			slog.Error("access count", "object", name, "error", err)
		}
		s.accessCount = count
	}
	contentType := "text/x-nix-narinfo"
	switch {
	case strings.HasSuffix(name, ".ls"):
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
	s := stats(r)
	head := stripeName(name, 0)
	s.radosCalls++
	xattrs, err := h.ioctx.ListXattrs(head)
	if err != nil {
		writeStoreError(w, name, err)
		return
	}
	size, _ := strconv.ParseUint(string(xattrs[sizeXattr]), 10, 64)
	stripeSize, _ := strconv.Atoi(string(xattrs[stripeSizeXattr]))
	if r.Method == http.MethodGet {
		count, err := setAccess(h.ioctx, head, xattrs[accessCountXattr], &s.radosCalls)
		if err != nil {
			slog.Error("access count", "object", name, "error", err)
		}
		s.accessCount = count
	}
	w.Header().Set("Content-Type", "application/x-nix-nar")
	w.Header().Set("Content-Length", strconv.FormatUint(size, 10))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	buf := make([]byte, stripeSize)
	for i, sent := 0, uint64(0); sent < size; i++ {
		s.radosCalls++
		n, err := h.ioctx.Read(stripeName(name, i), buf, 0)
		if err != nil || n == 0 {
			slog.Error("read stripe", "object", stripeName(name, i), "error", err)
			return
		}
		sent += uint64(n)
		if _, err := w.Write(buf[:n]); err != nil {
			return
		}
	}
}

func (h *handler) putObject(w http.ResponseWriter, r *http.Request) {
	name, ok := objectName(r)
	if !ok {
		http.NotFound(w, r)
		return
	}
	s := stats(r)
	var err error
	if strings.HasPrefix(name, "nar/") {
		err = putNAR(h.ioctx, name, r.Body, h.stripeSize, &s.radosCalls)
	} else {
		data, rerr := io.ReadAll(io.LimitReader(r.Body, maxNarinfoSize+1))
		if rerr != nil {
			http.Error(w, rerr.Error(), http.StatusBadRequest)
			return
		}
		if len(data) > maxNarinfoSize {
			http.Error(w, "object too large", http.StatusRequestEntityTooLarge)
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
		err = putObject(h.ioctx, name, data, &s.radosCalls)
		if errors.Is(err, rados.ErrObjectExists) && len(sigs) > 0 {
			err = cmp.Or(appendSigs(h.ioctx, name, sigs, &s.radosCalls), err)
		}
	}
	switch {
	case errors.Is(err, rados.ErrObjectExists):
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
