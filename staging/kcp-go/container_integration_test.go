package kcp

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"
)

// TestContainerIntegration runs an end-to-end KCP test inside Docker containers.
// Requires Docker to be installed.
//
// Creates two containers on an isolated bridge network:
//   - server: KCP echo server listening on :9999
//   - client: sends 100 round-trip messages, verifies correctness
func TestContainerIntegration(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available, skipping container integration test")
	}

	const (
		image   = "kcp-test"
		network = "kcp-test-net"
	)

	// Build the test image.
	build := exec.Command("docker", "build", "-t", image, "-f", "container_test/Dockerfile", ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("docker build failed: %v\n%s", err, out)
	}

	// Create isolated bridge network.
	exec.Command("docker", "network", "rm", network).Run() // cleanup stale
	if out, err := exec.Command("docker", "network", "create", network).CombinedOutput(); err != nil {
		t.Fatalf("docker network create failed: %v\n%s", err, out)
	}

	// cleanup tears down containers and network.
	cleanup := func() {
		exec.Command("docker", "rm", "-f", "server", "client").Run()
		exec.Command("docker", "network", "rm", network).Run()
	}
	defer cleanup()

	// Start server container (detached).
	serverRun := exec.Command("docker", "run",
		"-d", "--name", "server",
		"--network", network,
		image,
		"-mode", "server", "-addr", ":9999",
	)
	if out, err := serverRun.CombinedOutput(); err != nil {
		cleanup()
		t.Fatalf("docker run server failed: %v\n%s", err, out)
	}

	// Run client container (foreground, captures exit code).
	clientRun := exec.Command("docker", "run",
		"--name", "client",
		"--network", network,
		image,
		"-mode", "client", "-addr", "server:9999",
	)
	out, err := clientRun.CombinedOutput()
	outStr := string(out)

	if err != nil {
		// Grab server logs for debugging.
		logs, _ := exec.Command("docker", "logs", "server").CombinedOutput()
		cleanup()
		t.Fatalf("client failed: %v\nclient output:\n%s\nserver logs:\n%s", err, outStr, logs)
	}

	fmt.Println(outStr)

	if !strings.Contains(outStr, "PASS: 100 round-trips verified") {
		logs, _ := exec.Command("docker", "logs", "server").CombinedOutput()
		t.Fatalf("client did not report success\nclient output:\n%s\nserver logs:\n%s", outStr, logs)
	}
}
