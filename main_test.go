package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"
)

var cephDaemonLogs *LogDemux

func TestMain(m *testing.M) {
	testscript.Main(m, map[string]func(){
		"nix-rados-cache": main,
	})
}

const timeoutGracePeriod = 2 * time.Second

func TestScript(t *testing.T) {
	ctx := t.Context()
	var deadline time.Time
	if dl, ok := t.Deadline(); ok {
		if time.Until(dl) <= timeoutGracePeriod {
			t.Fatalf("not enough time")
		}
		deadline = dl.Add(-timeoutGracePeriod)
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		t.Cleanup(cancel)
	}

	cephDaemonLogs = &LogDemux{}
	var setupBuffer bytes.Buffer
	detachSetup := cephDaemonLogs.Attach(&setupBuffer)
	var confPath string
	var err error
	for attempt := 1; attempt <= 2; attempt++ {
		attemptCtx, attemptCancel := context.WithCancel(ctx)
		confPath, err = startCephCluster(t, attemptCtx, cephDaemonLogs)
		if err == nil {
			t.Cleanup(attemptCancel)
			break
		}
		attemptCancel()
		t.Logf("ceph cluster startup attempt %d failed: %v", attempt, err)
	}
	detachSetup()
	if err != nil {
		t.Log("=== Ceph cluster setup logs ===")
		_, _ = io.Copy(t.Output(), &setupBuffer)
		t.Fatal(err)
	}

	for _, poolType := range []string{"replicated", "erasure"} {
		t.Run(poolType, func(t *testing.T) {
			testscript.Run(t, testscript.Params{
				Dir:                 "testdata",
				ContinueOnError:     true,
				RequireExplicitExec: true,
				Deadline:            deadline,
				Cmds: map[string]func(*testscript.TestScript, bool, []string){
					"bin-cmp":            cmdBinCmp,
					"bin-file":           cmdBinFile,
					"create-pool":        cmdCreatePool,
					"rados-object-count": cmdRadosObjectCount,
					"tail-logs":          cmdTailLogs,
					"wait4http":          cmdWait4HTTP,
					"wait4log":           cmdWait4Log,
				},
				Setup: func(env *testscript.Env) error {
					env.Setenv("CEPH_CONF", confPath)
					env.Setenv("DEFAULT_POOL_TYPE", poolType)

					home := filepath.Join(env.WorkDir, "home")
					if err := os.MkdirAll(home, 0o755); err != nil {
						return err
					}
					env.Setenv("HOME", home)
					env.Setenv("XDG_CACHE_HOME", filepath.Join(home, ".cache"))
					env.Setenv("XDG_STATE_HOME", filepath.Join(home, ".local", "state"))
					env.Setenv("NIX_CONFIG", "experimental-features = nix-command\nnarinfo-cache-negative-ttl = 0\nnarinfo-cache-positive-ttl = 0\n")

					port, err := getFreePort()
					if err != nil {
						return fmt.Errorf("failed to allocate PORT: %w", err)
					}
					env.Setenv("PORT", strconv.Itoa(port))
					return nil
				},
			})
		})
	}
}

func getFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func cmdTailLogs(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! tail-logs")
	}
	pipeReader, detach := cephDaemonLogs.AttachPipe()
	tailCtx, cancel := context.WithCancel(context.Background())

	var output bytes.Buffer
	tailOutput := &LogDemux{}
	detachOutput := tailOutput.Attach(&output)

	var tailers sync.WaitGroup
	var serverFile *os.File
	ts.Defer(func() {
		cancel()
		_ = pipeReader.Close()
		detach()
		tailers.Wait()
		if serverFile != nil {
			_ = serverFile.Close()
		}
		detachOutput()
		if output.Len() > 0 {
			ts.Logf("%s", strings.TrimSuffix(output.String(), "\n"))
		}
	})

	tail := func(prefix string, open func() (io.Reader, error)) {
		defer tailers.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		var reader *bufio.Reader
		for {
			select {
			case <-tailCtx.Done():
				return
			case <-ticker.C:
				if reader == nil {
					r, err := open()
					if err != nil {
						continue
					}
					reader = bufio.NewReader(r)
				}
				line, err := reader.ReadString('\n')
				if err != nil {
					if err == io.EOF {
						continue
					}
					if !errors.Is(err, io.ErrClosedPipe) {
						_, _ = fmt.Fprintf(tailOutput, "%s tail error: %v\n", prefix, err)
					}
					return
				}
				_, _ = fmt.Fprintf(tailOutput, "%s %s\n", prefix, strings.TrimRight(line, "\n"))
			}
		}
	}

	serverLog := ts.MkAbs("server.log")
	tailers.Add(2)
	go tail("[ceph]", func() (io.Reader, error) { return pipeReader, nil })
	go tail("[nix-rados-cache]", func() (io.Reader, error) {
		f, err := os.Open(serverLog)
		if err != nil {
			return nil, err
		}
		serverFile = f
		return f, nil
	})
}

func cmdCreatePool(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! create-pool")
	}
	ctx := context.Background()
	poolType := ts.Getenv("DEFAULT_POOL_TYPE")
	confPath := ts.Getenv("CEPH_CONF")

	poolName := fmt.Sprintf("test-%x", mathrand.Uint64())

	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		createArgs := []string{"--conf", confPath, "osd", "pool", "create", poolName, "8"}
		if poolType == "erasure" {
			createArgs = append(createArgs, "8", "erasure", testECProfileName, testECCrushRuleName)
		}
		output, err := exec.CommandContext(ctx, "ceph", createArgs...).CombinedOutput()
		if exec.CommandContext(ctx, "ceph", "--conf", confPath, "osd", "pool", "get", poolName, "size").Run() == nil {
			break
		}
		if attempt == maxAttempts {
			ts.Fatalf("failed to create %s pool after %d attempts: %v\noutput: %s", poolType, maxAttempts, err, output)
		}
		time.Sleep(1 * time.Second)
	}

	ts.Setenv("NIX_RADOS_CACHE_POOL", poolName)
	ts.Defer(func() {
		deleteCmd := exec.Command("ceph", "--conf", confPath, "osd", "pool", "delete", poolName, poolName, "--yes-i-really-really-mean-it")
		if err := deleteCmd.Run(); err != nil {
			ts.Logf("warning: failed to delete pool %s: %v", poolName, err)
		}
	})
}

func cmdWait4HTTP(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 1 {
		ts.Fatalf("usage: wait4http <url>")
	}
	for range 150 {
		if resp, err := http.Get(args[0]); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	ts.Fatalf("%s never returned 200", args[0])
}

func cmdWait4Log(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 2 {
		ts.Fatalf("usage: wait4log <regexp> <file>")
	}
	pattern, err := regexp.Compile(args[0])
	if err != nil {
		ts.Fatalf("invalid pattern: %v", err)
	}
	path := ts.MkAbs(args[1])
	for range 150 {
		if data, err := os.ReadFile(path); err == nil && pattern.Match(data) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	ts.Fatalf("pattern %q did not appear in %s", args[0], args[1])
}

func cmdRadosObjectCount(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! rados-object-count")
	}
	if len(args) != 1 {
		ts.Fatalf("usage: rados-object-count <prefix>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "rados", "--conf", ts.Getenv("CEPH_CONF"), "--pool", ts.Getenv("NIX_RADOS_CACHE_POOL"), "ls")
	output, err := cmd.CombinedOutput()
	if err != nil {
		ts.Fatalf("failed to list rados objects: %v\noutput: %s", err, output)
	}
	count := 0
	for _, line := range strings.Split(string(output), "\n") {
		if line != "" && strings.HasPrefix(line, args[0]) {
			count++
		}
	}
	_, _ = fmt.Fprintf(ts.Stdout(), "%d\n", count)
}

func cmdBinFile(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("unsupported: ! bin-file")
	}
	if len(args) != 2 {
		ts.Fatalf("usage: bin-file <path> <size-bytes>")
	}
	size, err := strconv.Atoi(args[1])
	if err != nil {
		ts.Fatalf("invalid size: %s", args[1])
	}
	file, err := os.Create(ts.MkAbs(args[0]))
	if err != nil {
		ts.Fatalf("failed to create file: %v", err)
	}
	defer func() { _ = file.Close() }()
	if _, err := io.Copy(file, io.LimitReader(rand.Reader, int64(size))); err != nil {
		ts.Fatalf("failed to write file: %v", err)
	}
}

func cmdBinCmp(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 2 {
		ts.Fatalf("usage: bin-cmp file1 file2")
	}
	data1, err := os.ReadFile(ts.MkAbs(args[0]))
	if err != nil {
		ts.Fatalf("failed to read %s: %v", args[0], err)
	}
	data2, err := os.ReadFile(ts.MkAbs(args[1]))
	if err != nil {
		ts.Fatalf("failed to read %s: %v", args[1], err)
	}
	if !bytes.Equal(data1, data2) {
		ts.Fatalf("%s and %s differ", args[0], args[1])
	}
}

const (
	testECProfileName                = "k2m1"
	testECCrushRuleName              = "test-k2m1"
	testECCrushRuleKeepalivePoolName = "test-k2m1-rule-keepalive"
)

func startCephCluster(t *testing.T, ctx context.Context, out io.Writer) (string, error) {
	t.Helper()

	confPath, err := setupCephDir(ctx, t.TempDir(), out)
	if err != nil {
		return "", err
	}
	if err := startCephMon(t, ctx, confPath, out); err != nil {
		return "", err
	}
	if err := startCephOsd(t, ctx, confPath, out); err != nil {
		return "", err
	}
	if err := runCeph(ctx, confPath, "osd", "erasure-code-profile", "set", testECProfileName, "k=2", "m=1", "crush-failure-domain=osd"); err != nil {
		return "", err
	}
	if err := runCeph(ctx, confPath, "osd", "crush", "rule", "create-erasure", testECCrushRuleName, testECProfileName); err != nil {
		return "", err
	}
	if err := runCeph(ctx, confPath, "osd", "pool", "create", testECCrushRuleKeepalivePoolName, "1", "1", "erasure", testECProfileName, testECCrushRuleName); err != nil {
		return "", err
	}
	return confPath, nil
}

func randomUUID() (string, error) {
	uuid := make([]byte, 16)
	if _, err := rand.Read(uuid); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil
}

func setupCephDir(ctx context.Context, tmpDir string, out io.Writer) (string, error) {
	fsid, err := randomUUID()
	if err != nil {
		return "", fmt.Errorf("failed to generate cluster fsid: %w", err)
	}
	monPort, err := getFreePort()
	if err != nil {
		return "", fmt.Errorf("failed to allocate monitor port: %w", err)
	}
	monAddr := fmt.Sprintf("127.0.0.1:%d", monPort)
	confPath := filepath.Join(tmpDir, "ceph.conf")

	conf := fmt.Sprintf(cephConfTemplate, fsid, monAddr, tmpDir)

	for _, dir := range []string{"mon", "osd/ceph-0", "osd/ceph-1", "osd/ceph-2", "run", "crash"} {
		if err := os.MkdirAll(filepath.Join(tmpDir, dir), 0o755); err != nil {
			return confPath, err
		}
	}
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		return confPath, err
	}

	monmapPath := filepath.Join(tmpDir, "monmap")
	steps := [][]string{
		{"monmaptool", "--conf", confPath, monmapPath, "--create", "--fsid", fsid},
		{"monmaptool", "--conf", confPath, monmapPath, "--add", "mon1", "v1:" + monAddr},
		{"ceph-mon", "--conf", confPath, "--mkfs", "--id", "mon1", "--monmap", monmapPath},
	}
	for _, step := range steps {
		cmd := exec.CommandContext(ctx, step[0], step[1:]...)
		cmd.Stdout = out
		cmd.Stderr = out
		if err := cmd.Run(); err != nil {
			return confPath, fmt.Errorf("%s failed: %w", strings.Join(step, " "), err)
		}
	}
	if err := os.Remove(monmapPath); err != nil {
		return confPath, err
	}
	return confPath, nil
}

const cephConfTemplate = `[global]
fsid = %[1]s
mon_host = v1:%[2]s/0
public_network = 127.0.0.1/32
auth_cluster_required = none
auth_service_required = none
auth_client_required = none
auth_allow_insecure_global_id_reclaim = true
pid_file = %[3]s/$type.$id.pid
admin_socket = %[3]s/$name.$pid.asok
crash_dir = %[3]s/crash
exporter_sock_dir = %[3]s/run
immutable_object_cache_sock = %[3]s/run/immutable_object_cache.sock
keyring = /dev/null
run_dir = %[3]s/run
log_to_file = false
log_to_stderr = true
osd_max_object_size = 33554432
osd_max_write_size = 16
osd_pool_default_size = 1
osd_pool_default_min_size = 1
osd_crush_chooseleaf_type = 0
mon_allow_pool_size_one = true

[mon]
mon_initial_members = mon1
mon_data = %[3]s/mon/ceph-$id
mon_cluster_log_to_file = false
mon_cluster_log_to_stderr = true
mon_allow_pool_delete = true

[osd]
osd_data = %[3]s/osd/ceph-$id
osd_objectstore = memstore
`

func startDaemon(t *testing.T, ctx context.Context, out io.Writer, args ...string) error {
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", args[0], err)
	}
	t.Cleanup(func() {
		if err := cmd.Wait(); err != nil {
			t.Logf("%s exited: %v", strings.Join(args, " "), err)
		}
	})
	return nil
}

func startCephMon(t *testing.T, ctx context.Context, confPath string, out io.Writer) error {
	if err := startDaemon(t, ctx, out, "ceph-mon", "--conf", confPath, "--id", "mon1", "--foreground"); err != nil {
		return err
	}
	return waitForCeph(ctx, confPath, func(status cephStatus) bool { return status.Monmap.NumMons > 0 })
}

func startCephOsd(t *testing.T, ctx context.Context, confPath string, out io.Writer) error {
	for i := 0; i < 3; i++ {
		osdID := strconv.Itoa(i)
		osdUUID, err := randomUUID()
		if err != nil {
			return fmt.Errorf("failed to generate OSD %d uuid: %w", i, err)
		}
		mkfs := exec.CommandContext(ctx, "ceph-osd", "--conf", confPath, "--id", osdID, "--mkfs", "--osd-uuid", osdUUID)
		mkfs.Stdout = out
		mkfs.Stderr = out
		if err := mkfs.Run(); err != nil {
			return fmt.Errorf("failed to initialize OSD %d filesystem: %w", i, err)
		}
		if err := startDaemon(t, ctx, out, "ceph-osd", "--conf", confPath, "--id", osdID, "--foreground"); err != nil {
			return err
		}
	}
	return waitForCeph(ctx, confPath, func(status cephStatus) bool { return status.Osdmap.NumUpOsds >= 3 })
}

func waitForCeph(ctx context.Context, confPath string, ready func(cephStatus) bool) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if status, err := checkCephStatus(ctx, confPath); err == nil && ready(status) {
				return nil
			}
		}
	}
}

func runCeph(ctx context.Context, confPath string, args ...string) error {
	cmd := exec.CommandContext(ctx, "ceph", append([]string{"--conf", confPath}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ceph %s: %w, output: %s", strings.Join(args, " "), err, output)
	}
	return nil
}

type cephStatus struct {
	Monmap struct {
		NumMons int `json:"num_mons"`
	} `json:"monmap"`
	Osdmap struct {
		NumUpOsds int `json:"num_up_osds"`
	} `json:"osdmap"`
}

func checkCephStatus(ctx context.Context, confPath string) (cephStatus, error) {
	output, err := exec.CommandContext(ctx, "ceph", "--conf", confPath, "status", "--format", "json").Output()
	if err != nil {
		return cephStatus{}, err
	}
	var status cephStatus
	err = json.Unmarshal(output, &status)
	return status, err
}

type LogDemux struct {
	mu   sync.Mutex
	outs map[io.Writer]struct{}
}

func (ld *LogDemux) Write(p []byte) (int, error) {
	ld.mu.Lock()
	defer ld.mu.Unlock()
	for writer := range ld.outs {
		written, err := writer.Write(p)
		if err != nil {
			return 0, err
		}
		if written != len(p) {
			return 0, fmt.Errorf("short write: expected %d, got %d", len(p), written)
		}
	}
	return len(p), nil
}

func (ld *LogDemux) Attach(writer io.Writer) func() {
	ld.mu.Lock()
	if ld.outs == nil {
		ld.outs = make(map[io.Writer]struct{})
	}
	ld.outs[writer] = struct{}{}
	ld.mu.Unlock()
	return func() {
		ld.mu.Lock()
		delete(ld.outs, writer)
		ld.mu.Unlock()
	}
}

func (ld *LogDemux) AttachPipe() (*io.PipeReader, func()) {
	pr, pw := io.Pipe()
	ld.mu.Lock()
	if ld.outs == nil {
		ld.outs = make(map[io.Writer]struct{})
	}
	ld.outs[pw] = struct{}{}
	ld.mu.Unlock()
	return pr, func() {
		ld.mu.Lock()
		defer ld.mu.Unlock()
		delete(ld.outs, pw)
		_ = pw.Close()
	}
}
