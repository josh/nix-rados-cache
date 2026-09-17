package main

import (
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
	"strconv"
	"strings"
	"time"

	"github.com/ceph/go-ceph/rados"
)

const (
	cacheInfo        = "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40\n"
	maxObjectSize    = 16 * 1024 * 1024
	accessCountXattr = "access_count"
)

var (
	objectNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	errObjectExists   = errors.New("object exists")
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "TCP address to listen on")
	pool := flag.String("pool", "", "RADOS pool name")
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

	slog.Info("listening", "address", *listen, "pool", *pool)
	if err := http.ListenAndServe(*listen, newHandler(ioctx)); err != nil {
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

func putObject(ioctx *rados.IOContext, name string, data []byte, calls *int) error {
	op := rados.CreateWriteOp()
	defer op.Release()
	op.Create(rados.CreateExclusive)
	op.WriteFull(data)
	*calls++
	err := op.Operate(ioctx, name, rados.OperationNoFlag)
	if errors.Is(err, rados.ErrObjectExists) {
		return errObjectExists
	}
	return err
}

func countAccess(ioctx *rados.IOContext, name string, calls *int) (uint64, error) {
	*calls++
	xattrs, _ := ioctx.ListXattrs(name)
	count, _ := strconv.ParseUint(string(xattrs[accessCountXattr]), 10, 64)
	count++
	*calls++
	return count, ioctx.SetXattr(name, accessCountXattr, []byte(strconv.FormatUint(count, 10)))
}

type handler struct {
	ioctx *rados.IOContext
}

func newHandler(ioctx *rados.IOContext) http.Handler {
	h := &handler{ioctx: ioctx}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /nix-cache-info", h.getCacheInfo)
	mux.HandleFunc("PUT /nix-cache-info", h.putCacheInfo)
	mux.HandleFunc("GET /nar/{name}", h.getObject)
	mux.HandleFunc("PUT /nar/{name}", h.putObject)
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
	if r.Method != http.MethodHead {
		_, _ = io.WriteString(w, cacheInfo)
	}
}

func (h *handler) putCacheInfo(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
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
	return name, strings.HasSuffix(name, ".narinfo")
}

func (h *handler) getObject(w http.ResponseWriter, r *http.Request) {
	name, ok := objectName(r)
	if !ok {
		http.NotFound(w, r)
		return
	}
	data, err := getObject(h.ioctx, name, &stats(r).radosCalls)
	if err != nil {
		writeStoreError(w, name, err)
		return
	}
	if r.Method == http.MethodGet {
		s := stats(r)
		count, err := countAccess(h.ioctx, name, &s.radosCalls)
		if err != nil {
			slog.Error("access count", "object", name, "error", err)
		}
		s.accessCount = count
	}
	contentType := "text/x-nix-narinfo"
	if strings.HasPrefix(name, "nar/") {
		contentType = "application/x-nix-nar"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(data)
	}
}

func (h *handler) putObject(w http.ResponseWriter, r *http.Request) {
	name, ok := objectName(r)
	if !ok {
		http.NotFound(w, r)
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, maxObjectSize+1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(data) > maxObjectSize {
		http.Error(w, "object too large", http.StatusRequestEntityTooLarge)
		return
	}
	err = putObject(h.ioctx, name, data, &stats(r).radosCalls)
	switch {
	case errors.Is(err, errObjectExists):
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
