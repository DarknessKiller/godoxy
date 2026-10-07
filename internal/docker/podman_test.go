package docker

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yusing/godoxy/internal/types"
)

func TestUnixSocketPath(t *testing.T) {
	tests := []struct {
		name string
		host string
		want string
		ok   bool
	}{
		{name: "absolute", host: "unix:///tmp/podman.sock", want: "/tmp/podman.sock", ok: true},
		{name: "abbreviated", host: "unix://var/run/docker.sock", want: "/var/run/docker.sock", ok: true},
		{name: "tcp", host: "tcp://127.0.0.1:2375"},
		{name: "http", host: "http://socket-proxy:2375"},
		{name: "empty", host: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path, ok := unixSocketPath(tc.host)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, path)
		})
	}
}

func TestPodmanDependenciesSkipsUnsupportedHosts(t *testing.T) {
	tests := []struct {
		name string
		c    *Container
	}{
		{
			name: "tcp host",
			c:    &Container{DockerCfg: types.DockerProviderConfig{URL: "tcp://127.0.0.1:2375"}, ContainerID: "container-id"},
		},
		{
			name: "empty container id",
			c:    &Container{DockerCfg: types.DockerProviderConfig{URL: "unix:///tmp/podman.sock"}},
		},
		{
			name: "agent host without resolved agent",
			c:    &Container{DockerCfg: types.DockerProviderConfig{URL: "agent://some-agent"}, ContainerID: "container-id"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Nil(t, podmanDependencies(t.Context(), tc.c))
		})
	}
}

func TestFetchPodmanDependencies(t *testing.T) {
	const (
		appID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		dbID  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/v4.0.0/libpod/containers/"+appID+"/json", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `{"Dependencies":["`+dbID+`"]}`)
	})
	mux.HandleFunc("/v4.0.0/libpod/containers/json", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `[{"Id":"`+dbID+`","Names":["proj_db_1"],"Labels":{"com.docker.compose.service":"db"}}]`)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	deps := fetchPodmanDependencies(t.Context(), srv.Client(), srv.URL, appID)
	require.Equal(t, []string{"db"}, deps)
}

func TestFetchPodmanDependenciesNonPodmanHost(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	require.Nil(t, fetchPodmanDependencies(t.Context(), srv.Client(), srv.URL, "container-id"))
}
