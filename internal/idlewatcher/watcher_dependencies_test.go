package idlewatcher

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/yusing/godoxy/internal/agentpool"
	"github.com/yusing/godoxy/internal/docker"
	"github.com/yusing/godoxy/internal/entrypoint"
	"github.com/yusing/godoxy/internal/health"
	"github.com/yusing/godoxy/internal/homepage"
	idlewatchertypes "github.com/yusing/godoxy/internal/idlewatcher/runtime"
	"github.com/yusing/godoxy/internal/loadbalancer"
	nettypes "github.com/yusing/godoxy/internal/net/types"
	"github.com/yusing/godoxy/internal/routing"
	godoxytypes "github.com/yusing/godoxy/internal/types"
	watcherEvents "github.com/yusing/godoxy/internal/watcher/events"
	"github.com/yusing/goutils/http/reverseproxy"
	"github.com/yusing/goutils/task"
)

func TestNewWatcherReloadClearsDependencies(t *testing.T) {
	w, parent, mainRoute, _ := newDependencyReloadTest(t, "clear-deps", []string{"old"})
	require.Len(t, w.dependsOn, 1)
	w.sendEvent(WakeEventError, "stale attempt", errors.New("stale wake error"))
	require.NotEmpty(t, w.events.Get())

	reloaded, err := NewWatcher(parent, mainRoute, idlewatcherTestConfig("clear-deps", nil))
	require.NoError(t, err)
	require.Same(t, w, reloaded)
	require.Empty(t, reloaded.dependsOn)
	require.Empty(t, reloaded.cfg.DependsOn)
	require.Empty(t, reloaded.events.Get())
}

func TestNewWatcherReloadReplacesDependencies(t *testing.T) {
	w, parent, mainRoute, _ := newDependencyReloadTest(t, "replace-deps", []string{"old"})
	require.Len(t, w.dependsOn, 1)

	reloaded, err := NewWatcher(parent, mainRoute, idlewatcherTestConfig("replace-deps", []string{"new"}))
	require.NoError(t, err)
	require.Same(t, w, reloaded)
	require.Len(t, reloaded.dependsOn, 1)
	require.Equal(t, "new-container", reloaded.dependsOn[0].cfg.ContainerName())
	require.Equal(t, []string{"new-container"}, reloaded.cfg.DependsOn)
}

func TestNewWatcherReloadKeepsDependenciesAfterResolutionError(t *testing.T) {
	w, parent, mainRoute, providerCloses := newDependencyReloadTest(t, "bad-deps", []string{"old"})
	require.Len(t, w.dependsOn, 1)

	reloaded, err := NewWatcher(parent, mainRoute, idlewatcherTestConfig("bad-deps", []string{"new:unsupported"}))
	require.Error(t, err)
	require.Nil(t, reloaded)
	require.Len(t, w.dependsOn, 1)
	require.Equal(t, "old-container", w.dependsOn[0].cfg.ContainerName())
	require.Equal(t, []string{"old-container"}, w.cfg.DependsOn)
	require.Zero(t, providerCloses.Load())

	watcherMapMu.RLock()
	_, newDepExists := watcherMap["new-id"]
	watcherMapMu.RUnlock()
	require.False(t, newDepExists)
}

func TestNewWatcherReloadDoesNotCreateDependenciesAfterResolutionError(t *testing.T) {
	w, parent, mainRoute, providerCloses := newDependencyReloadTest(t, "mixed-bad-deps", []string{"old"})
	require.Len(t, w.dependsOn, 1)

	reloaded, err := NewWatcher(parent, mainRoute, idlewatcherTestConfig("mixed-bad-deps", []string{"new", "bad:unsupported"}))
	require.Error(t, err)
	require.Nil(t, reloaded)
	require.Len(t, w.dependsOn, 1)
	require.Equal(t, "old-container", w.dependsOn[0].cfg.ContainerName())
	require.Equal(t, []string{"old-container"}, w.cfg.DependsOn)
	require.Zero(t, providerCloses.Load())

	watcherMapMu.RLock()
	_, newDepExists := watcherMap["new-id"]
	watcherMapMu.RUnlock()
	require.False(t, newDepExists)
}

func TestNewWatcherReloadKeepsDependenciesAfterProviderError(t *testing.T) {
	w, parent, mainRoute, providerCloses := newDependencyReloadTest(t, "provider-error", []string{"old"})
	require.Len(t, w.dependsOn, 1)

	newDockerProvider = func(context.Context, godoxytypes.DockerProviderConfig, string) (idlewatchertypes.Provider, error) {
		return nil, errors.New("provider unavailable")
	}

	reloaded, err := NewWatcher(parent, mainRoute, idlewatcherTestConfig("provider-error", []string{"new"}))
	require.ErrorContains(t, err, "provider unavailable")
	require.Nil(t, reloaded)
	require.Len(t, w.dependsOn, 1)
	require.Equal(t, "old-container", w.dependsOn[0].cfg.ContainerName())
	require.Equal(t, []string{"old-container"}, w.cfg.DependsOn)
	require.Zero(t, providerCloses.Load())

	watcherMapMu.RLock()
	_, newDepExists := watcherMap["new-id"]
	watcherMapMu.RUnlock()
	require.False(t, newDepExists)
}

func newDependencyReloadTest(t *testing.T, id string, dependsOn []string) (*Watcher, *task.Task, *idlewatcherTestRoute, *atomic.Int64) {
	t.Helper()

	oldDockerProvider := newDockerProvider
	oldProxmoxProvider := newProxmoxProvider
	providerCloses := &atomic.Int64{}
	newDockerProvider = func(context.Context, godoxytypes.DockerProviderConfig, string) (idlewatchertypes.Provider, error) {
		return &idlewatcherTestRuntimeProvider{closes: providerCloses}, nil
	}
	newProxmoxProvider = func(context.Context, string, uint64) (idlewatchertypes.Provider, error) {
		return &idlewatcherTestRuntimeProvider{closes: providerCloses}, nil
	}
	t.Cleanup(func() {
		newDockerProvider = oldDockerProvider
		newProxmoxProvider = oldProxmoxProvider

		watcherMapMu.Lock()
		clear(watcherMap)
		watcherMapMu.Unlock()
	})

	parent := task.GetTestTask(t).Subtask("idlewatcher_"+id, true)
	t.Cleanup(func() {
		parent.FinishAndWait(nil)
	})

	routeProvider := &idlewatcherTestRouteProvider{routes: make(map[string]routing.Route)}
	mainRoute := newIdlewatcherTestRoute("main", routeProvider, idlewatcherTestConfig(id, dependsOn))
	routeProvider.routes["old"] = newIdlewatcherTestRoute("old", routeProvider, nil)
	routeProvider.routes["new"] = newIdlewatcherTestRoute("new", routeProvider, nil)

	w, err := NewWatcher(parent, mainRoute, mainRoute.cfg)
	require.NoError(t, err)
	return w, parent, mainRoute, providerCloses
}

func idlewatcherTestConfig(id string, dependsOn []string) *idlewatchertypes.Config {
	return &idlewatchertypes.Config{
		Docker: &idlewatchertypes.DockerConfig{
			ContainerID:   id,
			ContainerName: id + "-container",
		},
		IdleTimeout: time.Hour,
		WakeTimeout: time.Second,
		StopTimeout: time.Second,
		DependsOn:   dependsOn,
	}
}

type idlewatcherTestRuntimeProvider struct {
	closes *atomic.Int64
}

func (*idlewatcherTestRuntimeProvider) ContainerPause(context.Context) error   { return nil }
func (*idlewatcherTestRuntimeProvider) ContainerUnpause(context.Context) error { return nil }
func (*idlewatcherTestRuntimeProvider) ContainerStart(context.Context) error   { return nil }
func (*idlewatcherTestRuntimeProvider) ContainerStop(context.Context, idlewatchertypes.Signal, int) error {
	return nil
}

func (*idlewatcherTestRuntimeProvider) ContainerKill(context.Context, idlewatchertypes.Signal) error {
	return nil
}

func (*idlewatcherTestRuntimeProvider) ContainerStatus(context.Context) (idlewatchertypes.ContainerStatus, error) {
	return idlewatchertypes.ContainerStatusStopped, nil
}

func (*idlewatcherTestRuntimeProvider) Watch(ctx context.Context) (<-chan watcherEvents.Event, <-chan error) {
	eventCh := make(chan watcherEvents.Event)
	errCh := make(chan error)
	go func() {
		<-ctx.Done()
		close(eventCh)
		close(errCh)
	}()
	return eventCh, errCh
}

func (p *idlewatcherTestRuntimeProvider) Close() {
	p.closes.Add(1)
}

type idlewatcherTestRouteProvider struct {
	routes map[string]routing.Route
}

func (p *idlewatcherTestRouteProvider) Activate(task.Parent) routing.ProviderActivation {
	return routing.ProviderActivation{Provider: p.String(), EventLoopReady: true}
}
func (p *idlewatcherTestRouteProvider) LoadRoutes(context.Context) error { return nil }
func (p *idlewatcherTestRouteProvider) GetRoute(alias string) (routing.Route, bool) {
	r, ok := p.routes[alias]
	return r, ok
}

func (p *idlewatcherTestRouteProvider) IterRoutes(yield func(string, routing.Route) bool) {
	for alias, r := range p.routes {
		if !yield(alias, r) {
			return
		}
	}
}
func (p *idlewatcherTestRouteProvider) NumRoutes() int { return len(p.routes) }
func (p *idlewatcherTestRouteProvider) FindService(_, service string) (routing.Route, bool) {
	return p.GetRoute(service)
}

func (p *idlewatcherTestRouteProvider) Statistics() routing.ProviderStats {
	return routing.ProviderStats{Type: routing.ProviderTypeDocker}
}

func (p *idlewatcherTestRouteProvider) GetType() routing.ProviderType {
	return routing.ProviderTypeDocker
}
func (p *idlewatcherTestRouteProvider) ShortName() string { return "test" }
func (p *idlewatcherTestRouteProvider) String() string    { return "test" }

type idlewatcherTestRoute struct {
	name      string
	cfg       *idlewatchertypes.Config
	provider  routing.Provider
	started   chan struct{}
	targetURL *nettypes.URL
	container *docker.Container
	healthMon health.HealthMonitor
}

func newIdlewatcherTestRoute(name string, provider routing.Provider, cfg *idlewatchertypes.Config) *idlewatcherTestRoute {
	started := make(chan struct{})
	close(started)
	targetURL := nettypes.NewURL(&url.URL{Scheme: "http", Host: name + ".test"})

	return &idlewatcherTestRoute{
		name:      name,
		cfg:       cfg,
		provider:  provider,
		started:   started,
		targetURL: targetURL,
		container: &docker.Container{
			ContainerID:   name + "-id",
			ContainerName: name + "-container",
			Labels: map[string]string{
				"com.docker.compose.project": "idlewatcher-test",
			},
		},
	}
}

func (r *idlewatcherTestRoute) Start(task.Parent) error             { return nil }
func (r *idlewatcherTestRoute) Task() *task.Task                    { return nil }
func (r *idlewatcherTestRoute) Finish(any)                          {}
func (r *idlewatcherTestRoute) Key() string                         { return r.name }
func (r *idlewatcherTestRoute) Name() string                        { return r.name }
func (r *idlewatcherTestRoute) ProviderName() string                { return "test" }
func (r *idlewatcherTestRoute) GetProvider() routing.Provider       { return r.provider }
func (r *idlewatcherTestRoute) ListenURL() *nettypes.URL            { return nil }
func (r *idlewatcherTestRoute) TargetURL() *nettypes.URL            { return r.targetURL }
func (r *idlewatcherTestRoute) HealthMonitor() health.HealthMonitor { return r.healthMon }
func (r *idlewatcherTestRoute) SetHealthMonitor(m health.HealthMonitor) {
	r.healthMon = m
}
func (r *idlewatcherTestRoute) References() []string                        { return nil }
func (r *idlewatcherTestRoute) ShouldExclude() bool                         { return false }
func (r *idlewatcherTestRoute) Started() <-chan struct{}                    { return r.started }
func (r *idlewatcherTestRoute) IdlewatcherConfig() *idlewatchertypes.Config { return r.cfg }
func (r *idlewatcherTestRoute) HealthCheckConfig() health.HealthCheckConfig {
	return health.HealthCheckConfig{}
}
func (r *idlewatcherTestRoute) LoadBalanceConfig() *loadbalancer.Config { return nil }
func (r *idlewatcherTestRoute) HomepageItem() homepage.Item             { return homepage.Item{} }
func (r *idlewatcherTestRoute) DisplayName() string                     { return r.name }
func (r *idlewatcherTestRoute) ContainerInfo() *docker.Container        { return r.container }
func (r *idlewatcherTestRoute) InboundMTLSProfileRef() string           { return "" }
func (r *idlewatcherTestRoute) RouteMiddlewares() map[string]godoxytypes.LabelMap {
	return nil
}
func (r *idlewatcherTestRoute) GetAgent() *agentpool.Agent { return nil }
func (r *idlewatcherTestRoute) IsDocker() bool             { return true }
func (r *idlewatcherTestRoute) IsAgent() bool              { return false }
func (r *idlewatcherTestRoute) UseLoadBalance() bool       { return false }
func (r *idlewatcherTestRoute) UseIdleWatcher() bool       { return r.cfg != nil }
func (r *idlewatcherTestRoute) UseHealthCheck() bool       { return false }
func (r *idlewatcherTestRoute) UseAccessLog() bool         { return false }
func (r *idlewatcherTestRoute) ReverseProxy() *reverseproxy.ReverseProxy {
	return nil
}
func (r *idlewatcherTestRoute) ServeHTTP(http.ResponseWriter, *http.Request) {}
func (r *idlewatcherTestRoute) MarshalZerologObject(*zerolog.Event)          {}

// A watcher that is both its own idle route and another route's dependency gets
// NewWatcher called a second time with a dependency-synthesized config. That
// config carries neverTick so the dependency never auto-sleeps, and adopting it
// would silently disable the route's own idle timeout.
func TestNewWatcherReloadIgnoresDependencyConfig(t *testing.T) {
	w, parent, mainRoute, _ := newDependencyReloadTest(t, "dep-clobber", nil)
	require.Equal(t, time.Hour, w.cfg.IdleTimeout)

	depCfg := idlewatcherTestConfig("dep-clobber", nil)
	depCfg.IdleTimeout = neverTick
	depCfg.WakeTimeout = 42 * time.Second
	depCfg.StopTimeout = 10 * time.Second
	depCfg.StopMethod = idlewatchertypes.ContainerStopMethodKill
	depCfg.StopSignal = "SIGKILL"
	want := w.cfg.IdlewatcherConfigBase

	reloaded, err := NewWatcher(parent, mainRoute, depCfg)
	require.NoError(t, err)
	require.Same(t, w, reloaded)
	require.Equal(t, time.Hour, reloaded.cfg.IdleTimeout, "route keeps its own idle timeout")
	require.Equal(t, want, reloaded.cfg.IdlewatcherConfigBase, "route keeps all of its own base config")
}

// A genuine reload must still be adopted.
func TestNewWatcherReloadAdoptsUpdatedConfig(t *testing.T) {
	w, parent, mainRoute, _ := newDependencyReloadTest(t, "adopt-cfg", nil)
	require.Equal(t, time.Hour, w.cfg.IdleTimeout)

	newCfg := idlewatcherTestConfig("adopt-cfg", nil)
	newCfg.IdleTimeout = 2 * time.Hour
	newCfg.WakeTimeout = 5 * time.Second
	newCfg.StopTimeout = 10 * time.Second
	newCfg.StopMethod = idlewatchertypes.ContainerStopMethodKill
	newCfg.StopSignal = "SIGKILL"

	reloaded, err := NewWatcher(parent, mainRoute, newCfg)
	require.NoError(t, err)
	require.Same(t, w, reloaded)
	require.Equal(t, 2*time.Hour, reloaded.cfg.IdleTimeout)
	require.Equal(t, newCfg.IdlewatcherConfigBase, reloaded.cfg.IdlewatcherConfigBase)
}

func TestNewWatcherReloadDependencyOnlyConfig(t *testing.T) {
	for _, tc := range []struct {
		name        string
		idleTimeout time.Duration
		adopt       bool
	}{
		{"refresh", neverTick, true},
		{"promote_to_route", 2 * time.Hour, true},
		{"zero_unchanged", 0, false},
		{"negative_unchanged", -1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, parent, mainRoute, _ := newDependencyReloadTest(t, "dep-only", []string{"old"})
			dep := w.dependsOn[0].Watcher
			require.Equal(t, neverTick, dep.cfg.IdleTimeout)
			want := dep.cfg.IdlewatcherConfigBase
			cfg := idlewatcherTestConfig("old-id", nil)
			cfg.IdleTimeout = tc.idleTimeout
			cfg.WakeTimeout = 42 * time.Second
			cfg.StopTimeout = 10 * time.Second
			cfg.StopMethod = idlewatchertypes.ContainerStopMethodKill
			cfg.StopSignal = "SIGKILL"
			if tc.adopt {
				want = cfg.IdlewatcherConfigBase
			}
			depRoute, ok := mainRoute.provider.GetRoute("old")
			require.True(t, ok)
			reloaded, err := NewWatcher(parent, depRoute, cfg)
			require.NoError(t, err)
			require.Same(t, dep, reloaded)
			require.Equal(t, want, reloaded.cfg.IdlewatcherConfigBase)
		})
	}
}

func TestNewWatcherReloadRefreshesDependencySettings(t *testing.T) {
	for _, tc := range []struct {
		name        string
		explicit    bool
		idleTimeout time.Duration
	}{
		{name: "inherited"},
		{name: "explicit_zero", explicit: true},
		{name: "explicit_negative", explicit: true, idleTimeout: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, parent, mainRoute, _ := newDependencyReloadTest(t, "parent-reload", []string{"old"})
			dep := w.dependsOn[0].Watcher
			require.Equal(t, neverTick, dep.cfg.IdleTimeout)
			cfg := idlewatcherTestConfig("parent-reload", []string{"old"})
			updated := idlewatchertypes.IdlewatcherConfigBase{
				IdleTimeout: neverTick,
				WakeTimeout: 42 * time.Second,
				StopTimeout: 10 * time.Second,
				StopMethod:  idlewatchertypes.ContainerStopMethodKill,
				StopSignal:  "SIGKILL",
			}
			if tc.explicit {
				depRoute, ok := mainRoute.provider.GetRoute("old")
				require.True(t, ok)
				depCfg := idlewatcherTestConfig("old-id", nil)
				depCfg.IdlewatcherConfigBase = updated
				depCfg.IdleTimeout = tc.idleTimeout
				depRoute.(*idlewatcherTestRoute).cfg = depCfg
			} else {
				cfg.IdlewatcherConfigBase = updated
				cfg.IdleTimeout = time.Hour
			}

			reloaded, err := NewWatcher(parent, mainRoute, cfg)
			require.NoError(t, err)
			require.Same(t, w, reloaded)
			require.Len(t, reloaded.dependsOn, 1)
			require.Same(t, dep, reloaded.dependsOn[0].Watcher)
			require.Equal(t, updated, dep.cfg.IdlewatcherConfigBase)
		})
	}
}

type stopRecordingProvider struct {
	stops atomic.Int64
}

func (*stopRecordingProvider) ContainerPause(context.Context) error   { return nil }
func (*stopRecordingProvider) ContainerUnpause(context.Context) error { return nil }
func (*stopRecordingProvider) ContainerStart(context.Context) error   { return nil }
func (*stopRecordingProvider) ContainerKill(context.Context, idlewatchertypes.Signal) error {
	return nil
}
func (p *stopRecordingProvider) ContainerStop(context.Context, idlewatchertypes.Signal, int) error {
	p.stops.Add(1)
	return nil
}
func (*stopRecordingProvider) ContainerStatus(context.Context) (idlewatchertypes.ContainerStatus, error) {
	return idlewatchertypes.ContainerStatusRunning, nil
}
func (*stopRecordingProvider) Watch(context.Context) (<-chan watcherEvents.Event, <-chan error) {
	return nil, nil
}
func (*stopRecordingProvider) Close() {}

func TestStopDependenciesKeepsSharedDependency(t *testing.T) {
	shared := newTestWatcher(t)
	dependent := newTestWatcher(t)
	other := newTestWatcher(t)

	shared.cfg.StopMethod = idlewatchertypes.ContainerStopMethodStop
	sharedProvider := &stopRecordingProvider{}
	shared.provider.Store(sharedProvider)
	shared.state.Store(&containerState{status: idlewatchertypes.ContainerStatusRunning})

	dependent.dependsOn = []*dependency{{Watcher: shared}}
	other.dependsOn = []*dependency{{Watcher: shared}}

	for _, w := range []*Watcher{shared, dependent, other} {
		watcherMapMu.Lock()
		watcherMap[w.cfg.Key()] = w
		watcherMapMu.Unlock()
		t.Cleanup(func() {
			watcherMapMu.Lock()
			delete(watcherMap, w.cfg.Key())
			watcherMapMu.Unlock()
		})
	}

	other.state.Store(&containerState{status: idlewatchertypes.ContainerStatusRunning})
	require.NoError(t, dependent.stopDependencies())
	require.Zero(t, sharedProvider.stops.Load(), "shared dependency stopped while another dependent runs")

	other.state.Store(&containerState{status: idlewatchertypes.ContainerStatusStopped})
	require.NoError(t, dependent.stopDependencies())
	require.EqualValues(t, 1, sharedProvider.stops.Load(), "shared dependency not stopped after the last dependent stopped")
}

type idlewatcherTestEntrypoint struct {
	routing.Entrypoint
	routes   []routing.Route
	byName   map[string]routing.Route
	statuses map[string]health.HealthStatus
}

func (ep *idlewatcherTestEntrypoint) IterRoutes(yield func(routing.Route) bool) {
	for _, r := range ep.routes {
		if !yield(r) {
			return
		}
	}
}

func (ep *idlewatcherTestEntrypoint) GetRoute(alias string) (routing.Route, bool) {
	r, ok := ep.byName[alias]
	return r, ok
}

func (ep *idlewatcherTestEntrypoint) NumRoutes() int { return len(ep.routes) }

func (ep *idlewatcherTestEntrypoint) GetHealthInfoSimple() map[string]health.HealthStatus {
	return ep.statuses
}

func TestStopDependenciesKeepsAlwaysOnDependent(t *testing.T) {
	shared := newTestWatcher(t)
	dependent := newTestWatcher(t)

	shared.cfg.StopMethod = idlewatchertypes.ContainerStopMethodStop
	sharedProvider := &stopRecordingProvider{}
	shared.provider.Store(sharedProvider)
	shared.state.Store(&containerState{status: idlewatchertypes.ContainerStatusRunning})
	dependent.dependsOn = []*dependency{{Watcher: shared}}

	provider := &idlewatcherTestRouteProvider{routes: make(map[string]routing.Route)}
	// an always-on route (no idlewatcher config) depending on shared's container
	alwaysOn := newIdlewatcherTestRoute("always-on", provider, nil)
	alwaysOn.container.Labels["com.docker.compose.depends_on"] = "shared"
	sharedRoute := newIdlewatcherTestRoute("shared", provider, nil)
	sharedRoute.container.ContainerID = shared.cfg.Docker.ContainerID
	provider.routes["shared"] = sharedRoute

	ep := &idlewatcherTestEntrypoint{
		routes:   []routing.Route{alwaysOn},
		byName:   map[string]routing.Route{"always-on": alwaysOn},
		statuses: map[string]health.HealthStatus{"always-on": health.StatusHealthy},
	}
	entrypoint.SetCtx(shared.task, ep)

	require.NoError(t, dependent.stopDependencies())
	require.Zero(t, sharedProvider.stops.Load(), "dependency stopped while an always-on route depends on it")

	ep.statuses["always-on"] = health.StatusUnhealthy
	require.NoError(t, dependent.stopDependencies())
	require.EqualValues(t, 1, sharedProvider.stops.Load(), "dependency not stopped after the always-on route went down")
}
