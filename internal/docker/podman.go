package docker

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/yusing/godoxy/agent/pkg/agent"
)

// podmanLibpodAPIVersion is the libpod API version used for dependency lookup.
// Podman 4 and newer keep serving this version on the same socket as the
// Docker-compatible API.
const podmanLibpodAPIVersion = "v4.0.0"

const podmanRequestTimeout = 3 * time.Second

// podmanDependencies resolves a container's compose dependencies on Podman.
//
// podman-compose stores compose depends_on as podman `--requires`, which is
// only exposed by the libpod API, not by the Docker-compatible API. Returns
// compose service names when known, container names otherwise, and nil for
// non-podman hosts or when the libpod API is unreachable.
func podmanDependencies(ctx context.Context, c *Container) []string {
	if c.ContainerID == "" {
		return nil
	}
	httpClient, base, ok := podmanEndpoint(c)
	if !ok {
		return nil
	}
	return fetchPodmanDependencies(ctx, httpClient, base, c.ContainerID)
}

// podmanEndpoint returns the HTTP client and base URL that reach the host's
// libpod API, either through a local unix socket or an agent-managed host.
func podmanEndpoint(c *Container) (*http.Client, string, bool) {
	if agent.IsDockerHostAgent(c.DockerCfg.URL) {
		if c.Agent == nil {
			return nil, "", false
		}
		// The agent forwards any path to its docker/podman socket.
		return c.Agent.HTTPClient(), agent.DockerHost, true
	}
	socketPath, ok := unixSocketPath(c.DockerCfg.URL)
	if !ok {
		return nil, "", false
	}
	return unixHTTPClient(socketPath), "http://podman", true
}

func unixHTTPClient(socketPath string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "unix", socketPath)
			},
		},
	}
}

func fetchPodmanDependencies(ctx context.Context, httpClient *http.Client, base, containerID string) []string {
	ctx, cancel := context.WithTimeout(ctx, podmanRequestTimeout)
	defer cancel()

	pc := &podmanClient{
		base: strings.TrimSuffix(base, "/") + "/" + podmanLibpodAPIVersion + "/libpod",
		http: httpClient,
	}

	var inspect struct {
		Dependencies []string `json:"Dependencies"`
	}
	if err := pc.get(ctx, "/containers/"+url.PathEscape(containerID)+"/json", &inspect); err != nil {
		// Not a podman host, or the libpod API is not reachable.
		return nil
	}
	if len(inspect.Dependencies) == 0 {
		return nil
	}

	var containers []struct {
		ID     string            `json:"Id"`
		Names  []string          `json:"Names"`
		Labels map[string]string `json:"Labels"`
	}
	if err := pc.get(ctx, "/containers/json?all=true", &containers); err != nil {
		return nil
	}

	names := make(map[string]string, len(containers))
	for _, c := range containers {
		name := ""
		if len(c.Names) > 0 {
			name = strings.TrimPrefix(c.Names[0], "/")
		}
		if svc := c.Labels["com.docker.compose.service"]; svc != "" {
			name = svc
		}
		if name != "" {
			names[c.ID] = name
		}
	}

	deps := make([]string, 0, len(inspect.Dependencies))
	for _, id := range inspect.Dependencies {
		if name := names[id]; name != "" {
			deps = append(deps, name)
		}
	}
	return deps
}

// unixSocketPath returns the socket path when host points at a unix socket.
func unixSocketPath(host string) (string, bool) {
	u, err := url.Parse(host)
	if err != nil || u.Scheme != "unix" {
		return "", false
	}
	path := u.Path
	if u.Host != "" { // unix://var/run/docker.sock
		path = "/" + u.Host + path
	}
	if path == "" {
		return "", false
	}
	return path, true
}

type podmanClient struct {
	base string
	http *http.Client
}

func (c *podmanClient) get(ctx context.Context, path string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("podman: unexpected status %s", resp.Status)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}
