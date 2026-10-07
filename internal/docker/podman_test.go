package docker

import (
	"testing"

	"github.com/stretchr/testify/require"
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

func TestPodmanDependenciesSkipsNonUnixHosts(t *testing.T) {
	require.Nil(t, podmanDependencies(t.Context(), "tcp://127.0.0.1:2375", "container-id"))
	require.Nil(t, podmanDependencies(t.Context(), "unix:///tmp/podman.sock", ""))
}
