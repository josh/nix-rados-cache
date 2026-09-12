package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/ceph/go-ceph/rados"
)

const (
	cacheInfo     = "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40\n"
	maxObjectSize = 16 * 1024 * 1024
)

var (
	objectNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	errObjectExists   = errors.New("object exists")
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "TCP address to listen on")
	pool := flag.String("pool", "", "RADOS pool name")
	flag.Parse()

	if *pool == "" {
		fmt.Fprintln(os.Stderr, "--pool is required")
		os.Exit(1)
	}

	ioctx, err := openPool(*pool)
	if err != nil {
		slog.Error("failed to open pool", "error", err)
		os.Exit(1)
	}

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

func getObject(ioctx *rados.IOContext, name string) ([]byte, error) {
	stat, err := ioctx.Stat(name)
	if err != nil {
		return nil, err
	}
	data := make([]byte, stat.Size)
	n, err := ioctx.Read(name, data, 0)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func putObject(ioctx *rados.IOContext, name string, data []byte) error {
	op := rados.CreateWriteOp()
	defer op.Release()
	op.Create(rados.CreateExclusive)
	op.WriteFull(data)
	err := op.Operate(ioctx, name, rados.OperationNoFlag)
	if errors.Is(err, rados.ErrObjectExists) {
		return errObjectExists
	}
	return err
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
	return mux
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
	data, err := getObject(h.ioctx, name)
	if err != nil {
		writeStoreError(w, name, err)
		return
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
	err = putObject(h.ioctx, name, data)
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
