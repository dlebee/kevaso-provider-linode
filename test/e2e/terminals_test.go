//go:build e2e

package e2e

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
)

// term is kvo on a pseudo-terminal of the test's own, the way a person's terminal drives it.
type term struct {
	t   *testing.T
	f   *os.File
	cmd *exec.Cmd
	mu  sync.Mutex
	out bytes.Buffer
	pos int // how far expect has read
}

func (c *fleet) startTerm(t *testing.T, rows, cols uint16, args ...string) *term {
	t.Helper()
	cmd := c.cmd("", []string{"TERM=xterm-256color"}, c.kvo, args...)
	cmd.Stdin = nil
	f, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: rows, Cols: cols})
	if err != nil {
		t.Fatal(err)
	}
	tm := &term{t: t, f: f, cmd: cmd}
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := f.Read(buf)
			tm.mu.Lock()
			tm.out.Write(buf[:n])
			tm.mu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = f.Close() })
	return tm
}

func (tm *term) send(s string) {
	tm.t.Helper()
	if _, err := tm.f.Write([]byte(s)); err != nil {
		tm.t.Fatal(err)
	}
}

// expect waits for re in what came out since the last match.
func (tm *term) expect(re string, d time.Duration) []string {
	tm.t.Helper()
	r := regexp.MustCompile(re)
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		tm.mu.Lock()
		s := tm.out.String()[tm.pos:]
		if loc := r.FindStringSubmatchIndex(s); loc != nil {
			m := r.FindStringSubmatch(s)
			tm.pos += loc[1]
			tm.mu.Unlock()
			return m
		}
		tm.mu.Unlock()
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.t.Fatalf("no %q in %s; got:\n%q", re, d, tm.out.String()[tm.pos:])
	return nil
}

func (tm *term) wait(d time.Duration) int {
	tm.t.Helper()
	done := make(chan error, 1)
	go func() { done <- tm.cmd.Wait() }()
	select {
	case err := <-done:
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode()
		}
		if err != nil {
			tm.t.Fatal(err)
		}
		return 0
	case <-time.After(d):
		tm.t.Fatalf("kvo didn't exit in %s; output:\n%q", d, tm.out.String())
	}
	return -1
}

// ptyExec: kvo exec -it on a real terminal: its size, a resize, a full-screen program's size
// (curses), and the command's exit code.
func (c *fleet) ptyExec(t *testing.T) {
	tm := c.startTerm(t, 30, 100, "exec", "-it", "silo", "term", "-n", "e2e", "--", "sh")
	tm.send("stty size | sed 's/^/SZ=/'\n")
	tm.expect(`SZ=30 100`, 30*time.Second)
	if err := pty.Setsize(tm.f, &pty.Winsize{Rows: 41, Cols: 123}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second) // SIGWINCH → kvo → the stream's resize
	tm.send("stty size | sed 's/^/SZ=/'\n")
	tm.expect(`SZ=41 123`, 15*time.Second)
	tm.send(`python3 -c "import curses; s = curses.initscr(); y, x = s.getmaxyx(); curses.endwin(); print('CURSES', y, x, sep='_')"` + "\n")
	tm.expect(`CURSES_41_123`, 15*time.Second)
	tm.send("echo TERM=$TERM; tty\n")
	tm.expect(`TERM=xterm`, 10*time.Second)
	tm.expect(`/dev/pts/\d+`, 10*time.Second)
	tm.send("exit 7\n")
	if code := tm.wait(15 * time.Second); code != 7 {
		t.Fatalf("kvo exec -it exit code %d, want 7", code)
	}
	// Without a terminal too.
	if out, err := c.kvoe("exec", "silo", "term", "--", "sh", "-c", "echo out; exit 3"); !strings.Contains(out, "out") || exitCode(err) != 3 {
		t.Fatalf("kvo exec exit code: %v %q", err, out)
	}
	t.Log("kvo exec -it: a 30x100 pty, resized to 41x123 (stty and curses saw it), exit code 7 passed back")
}

func exitCode(err error) int {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	if err != nil {
		return -1
	}
	return 0
}

// ptySSH: kvo ssh silo NAME is a login shell on a terminal.
func (c *fleet) ptySSH(t *testing.T) {
	tm := c.startTerm(t, 24, 80, "ssh", "silo", "term", "-n", "e2e")
	tm.send("echo SHELL0=$0; id -u | sed 's/^/UID=/'; stty size | sed 's/^/SZ=/'\n")
	m := tm.expect(`SHELL0=([^$;\s]\S*)`, 30*time.Second)
	tm.expect(`UID=\d+`, 10*time.Second)
	tm.expect(`SZ=24 80`, 10*time.Second)
	tm.send("exit 4\n")
	if code := tm.wait(15 * time.Second); code != 4 {
		t.Fatalf("kvo ssh exit code %d, want 4", code)
	}
	t.Logf("kvo ssh: a shell (%s) on a 24x80 terminal, exit code 4", strings.TrimSpace(m[1]))
}

func sum(path string) string {
	f, _ := os.Open(path)
	defer func() { _ = f.Close() }()
	h := sha256.New()
	_, _ = io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil))
}

// copies: kvo cp in and out, a 96 MiB file and a folder.
func (c *fleet) copies(t *testing.T) {
	dir := t.TempDir()
	big := filepath.Join(dir, "big.bin")
	f, _ := os.Create(big)
	if _, err := io.CopyN(f, rand.Reader, 96<<20); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	want := sum(big)
	start := time.Now()
	c.mustKvo(t, "cp", big, "silo/term:/tmp/big.bin")
	in := time.Since(start)
	if out := c.mustKvo(t, "exec", "silo", "term", "--", "sha256sum", "/tmp/big.bin"); !strings.HasPrefix(out, want) {
		t.Fatalf("in the silo: %s, want %s", out, want)
	}
	start = time.Now()
	back := filepath.Join(dir, "back.bin")
	c.mustKvo(t, "cp", "silo/term:/tmp/big.bin", back)
	if got := sum(back); got != want {
		t.Fatalf("copied back: %s, want %s", got, want)
	}
	t.Logf("kvo cp: 96 MiB in (%s) and out (%s), same sha256", in.Round(time.Second), time.Since(start).Round(time.Second))

	tree := filepath.Join(dir, "tree")
	files := map[string]string{"a.txt": "alpha\n", "sub/b.txt": "beta\n", "sub/deeper/c.bin": strings.Repeat("c", 100000), "sub/empty": ""}
	for p, s := range files {
		_ = os.MkdirAll(filepath.Dir(filepath.Join(tree, p)), 0o755)
		_ = os.WriteFile(filepath.Join(tree, p), []byte(s), 0o640)
	}
	_ = os.Chmod(filepath.Join(tree, "a.txt"), 0o755)
	c.mustKvo(t, "cp", tree, "silo/term:/tmp/tree")
	out := c.mustKvo(t, "exec", "silo", "term", "--", "sh", "-c", "cd /tmp/tree && find . -type f | sort && cat a.txt sub/b.txt && stat -c '%a' a.txt && wc -c < sub/deeper/c.bin")
	for _, w := range []string{"./a.txt", "./sub/b.txt", "./sub/deeper/c.bin", "./sub/empty", "alpha", "beta", "755", "100000"} {
		if !strings.Contains(out, w) {
			t.Fatalf("the folder in the silo: no %q in\n%s", w, out)
		}
	}
	c.mustKvo(t, "exec", "silo", "term", "--", "sh", "-c", "echo made-inside > /tmp/tree/sub/new.txt")
	outDir := filepath.Join(dir, "out")
	c.mustKvo(t, "cp", "silo/term:/tmp/tree", outDir)
	for p, s := range files {
		got, err := os.ReadFile(filepath.Join(outDir, p))
		if err != nil || string(got) != s {
			// kvo cp DIR DST copies into DST/tree when DST exists; here it didn't.
			t.Fatalf("copied back %s: %v (%d bytes)", p, err, len(got))
		}
	}
	if got, _ := os.ReadFile(filepath.Join(outDir, "sub/new.txt")); string(got) != "made-inside\n" {
		t.Fatalf("a file made in the silo: %q", got)
	}
	t.Log("kvo cp: a folder in and out, contents and modes kept")
}

// killedClient: a long command whose client is killed is stopped on the host within seconds.
func (c *fleet) killedClient(t *testing.T, host machine) {
	for i, tty := range []bool{false, true} {
		marker := fmt.Sprintf("%d%d", 4000+time.Now().Unix()%1000, i)
		args := []string{"exec", "silo", "term", "-n", "e2e", "--", "sleep", marker}
		if tty {
			args = append([]string{"exec", "-it"}, args[1:]...)
		}
		var cmd *exec.Cmd
		if tty {
			tm := c.startTerm(t, 24, 80, args...)
			cmd = tm.cmd
		} else {
			cmd = c.cmd("", nil, c.kvo, args...)
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
		}
		count := func() string {
			out, _ := c.b.ssh(c.ctx, host, "ps -eo args | grep -c '^sleep "+marker+"$'")
			return strings.TrimSpace(out)
		}
		c.eventually(t, "sleep "+marker+" running on the host", 30*time.Second, func() (bool, string) { n := count(); return n == "1", n })
		_ = cmd.Process.Signal(syscall.SIGKILL)
		killed := time.Now()
		c.eventually(t, fmt.Sprintf("sleep %s gone after its client was killed (tty %v)", marker, tty), 15*time.Second, func() (bool, string) { n := count(); return n == "0", n })
		t.Logf("kvo exec (tty %v) killed: its command gone from the host %s later", tty, time.Since(killed).Round(time.Second))
		_ = cmd.Wait()
	}
}
