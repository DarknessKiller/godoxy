package idlewatcher

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dockerapi "github.com/docker/docker/api/types/container"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/yusing/godoxy/internal/agentpool"
	"github.com/yusing/godoxy/internal/docker"
	entrypoint "github.com/yusing/godoxy/internal/entrypoint"
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

// recordingIdlewatcherProvider records lifecycle actions per container so tests
// can assert which containers were stopped.
type recordingIdlewatcherProvider struct {
	idlewatcherTestRuntimeProvider
	record func(action string)
}

func (p *recordingIdlewatcherProvider) ContainerStart(context.Context) error {
	p.record("start")
	return nil
}

func (p *recordingIdlewatcherProvider) ContainerStop(context.Context, idlewatchertypes.Signal, int) error {
	p.record("stop")
	return nil
}

// fakeHealthMonitor is a mutable health monitor for entrypoint routes.
type fakeHealthMonitor struct {
	status health.HealthStatus
}

func (m *fakeHealthMonitor) Start(task.Parent) error      { return nil }
func (m *fakeHealthMonitor) Finish(any)                   {}
func (m *fakeHealthMonitor) Task() *task.Task             { return nil }
func (m *fakeHealthMonitor) String() string               { return m.status.String() }
func (m *fakeHealthMonitor) Status() health.HealthStatus  { return m.status }
func (m *fakeHealthMonitor) Uptime() time.Duration        { return 0 }
func (m *fakeHealthMonitor) Latency() time.Duration       { return 0 }
func (m *fakeHealthMonitor) Detail() string               { return "" }
func (m *fakeHealthMonitor) Name() string                 { return "fake" }
func (m *fakeHealthMonitor) MarshalJSON() ([]byte, error) { return []byte(`"fake"`), nil }
func (m *fakeHealthMonitor) MarshalZerologObject(e *zerolog.Event) {
	e.Str("status", m.status.String())
}

func newRecordingProvider(t *testing.T, record func(action, id string)) {
	t.Helper()
	oldDockerProvider := newDockerProvider
	oldProxmoxProvider := newProxmoxProvider
	newDockerProvider = func(_ context.Context, _ godoxytypes.DockerProviderConfig, id string) (idlewatchertypes.Provider, error) {
		return &recordingIdlewatcherProvider{
			idlewatcherTestRuntimeProvider: idlewatcherTestRuntimeProvider{closes: &atomic.Int64{}},
			record:                         func(action string) { record(action, id) },
		}, nil
	}
	newProxmoxProvider = func(_ context.Context, _ string, _ uint64) (idlewatchertypes.Provider, error) {
		return &recordingIdlewatcherProvider{
			idlewatcherTestRuntimeProvider: idlewatcherTestRuntimeProvider{closes: &atomic.Int64{}},
			record:                         func(action string) { record(action, "") },
		}, nil
	}
	t.Cleanup(func() {
		newDockerProvider = oldDockerProvider
		newProxmoxProvider = oldProxmoxProvider

		watcherMapMu.Lock()
		clear(watcherMap)
		watcherMapMu.Unlock()
	})
}

func recordedActions(mu *sync.Mutex, actions *[]string) []string {
	mu.Lock()
	defer mu.Unlock()
	return append([]string(nil), *actions...)
}

func appendAction(mu *sync.Mutex, actions *[]string, action, id string) {
	mu.Lock()
	defer mu.Unlock()
	*actions = append(*actions, action+":"+id)
}

func setRunning(ws ...*Watcher) {
	for _, w := range ws {
		w.storeState(&containerState{status: idlewatchertypes.ContainerStatusRunning})
	}
}

// Part 1: a dependency shared by several watchers must stay up until the last
// of them stops.
func TestStopDependenciesKeepsSharedDependencyForRunningWatcher(t *testing.T) {
	var mu sync.Mutex
	var actions []string
	newRecordingProvider(t, func(action, id string) { appendAction(&mu, &actions, action, id) })

	parent := task.GetTestTask(t).Subtask("idlewatcher_shared_dep", true)
	t.Cleanup(func() { parent.FinishAndWait(nil) })

	routeProvider := &idlewatcherTestRouteProvider{routes: make(map[string]routing.Route)}
	newDependent := func(id string) *idlewatcherTestRoute {
		cfg := idlewatcherTestConfig(id, []string{"dep"})
		cfg.StopMethod = idlewatchertypes.ContainerStopMethodStop
		return newIdlewatcherTestRoute(id, routeProvider, cfg)
	}
	mainRoute := newDependent("main")
	otherRoute := newDependent("other")
	depRoute := newIdlewatcherTestRoute("dep", routeProvider, nil)
	routeProvider.routes["main"] = mainRoute
	routeProvider.routes["other"] = otherRoute
	routeProvider.routes["dep"] = depRoute

	w1, err := NewWatcher(parent, mainRoute, mainRoute.cfg)
	require.NoError(t, err)
	w2, err := NewWatcher(parent, otherRoute, otherRoute.cfg)
	require.NoError(t, err)
	require.NotSame(t, w1, w2)
	require.Same(t, w1.dependsOn[0].Watcher, w2.dependsOn[0].Watcher,
		"shared dependency resolves to one watcher")
	dep := w1.dependsOn[0].Watcher
	setRunning(w1, w2, dep)

	// w1 idles out; w2's watcher is still running, so the dependency stays up.
	require.NoError(t, w1.stopByMethod())
	require.Equal(t, []string{"stop:main"}, recordedActions(&mu, &actions))
	mu.Lock()
	actions = nil
	mu.Unlock()

	// Model the runtime's stop event for w1 before w2 idles out.
	w1.storeState(&containerState{status: idlewatchertypes.ContainerStatusStopped})

	require.NoError(t, w2.stopByMethod())
	require.Equal(t, []string{"stop:other", "stop:dep-id"},
		recordedActions(&mu, &actions))
}

// Race: dependents idling at the same time must still stop the shared
// dependency, exactly once. Without the stopping flag and the stop flight,
// both guards see each other's stale running state and both skip, leaving
// the dependency running forever.
func TestStopDependenciesConcurrentIdlesStopSharedDependencyOnce(t *testing.T) {
	var mu sync.Mutex
	var actions []string
	newRecordingProvider(t, func(action, id string) { appendAction(&mu, &actions, action, id) })

	parent := task.GetTestTask(t).Subtask("idlewatcher_concurrent", true)
	t.Cleanup(func() { parent.FinishAndWait(nil) })

	routeProvider := &idlewatcherTestRouteProvider{routes: make(map[string]routing.Route)}
	newDependent := func(id string) *idlewatcherTestRoute {
		cfg := idlewatcherTestConfig(id, []string{"dep"})
		cfg.StopMethod = idlewatchertypes.ContainerStopMethodStop
		return newIdlewatcherTestRoute(id, routeProvider, cfg)
	}
	mainRoute := newDependent("main")
	otherRoute := newDependent("other")
	depRoute := newIdlewatcherTestRoute("dep", routeProvider, nil)
	routeProvider.routes["main"] = mainRoute
	routeProvider.routes["other"] = otherRoute
	routeProvider.routes["dep"] = depRoute

	w1, err := NewWatcher(parent, mainRoute, mainRoute.cfg)
	require.NoError(t, err)
	w2, err := NewWatcher(parent, otherRoute, otherRoute.cfg)
	require.NoError(t, err)
	dep := w1.dependsOn[0].Watcher
	setRunning(w1, w2, dep)

	// Both dependents idle out at the same time.
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Go(func() { errs <- w1.stopByMethod() })
	wg.Go(func() { errs <- w2.stopByMethod() })
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	acts := recordedActions(&mu, &actions)
	depStops := 0
	for _, a := range acts {
		if a == "stop:dep-id" {
			depStops++
		}
	}
	require.Equal(t, 1, depStops, "shared dependency must stop exactly once")
	require.Contains(t, acts, "stop:main")
	require.Contains(t, acts, "stop:other")
}

// Part 2: an always-on route has no watcher in watcherMap; a dependency it
// declares via container labels must stay up while the route is not down.
// The route's container is built through docker.FromDocker so the
// proxy.depends_on label path (label consumed during route construction,
// read later from ActualLabels by Dependencies) is exercised end to end.
func TestStopDependenciesKeepsDependencyForAlwaysOnRoute(t *testing.T) {
	var mu sync.Mutex
	var actions []string
	newRecordingProvider(t, func(action, id string) { appendAction(&mu, &actions, action, id) })

	parent := task.GetTestTask(t).Subtask("idlewatcher_always_on", true)
	t.Cleanup(func() { parent.FinishAndWait(nil) })

	ep := entrypoint.NewEntrypoint(parent, nil)
	entrypoint.SetCtx(parent, ep)

	routeProvider := &idlewatcherTestRouteProvider{routes: make(map[string]routing.Route)}
	cfg := idlewatcherTestConfig("main-id", []string{"dep"})
	cfg.StopMethod = idlewatchertypes.ContainerStopMethodStop
	mainRoute := newIdlewatcherTestRoute("main", routeProvider, cfg)
	depRoute := newIdlewatcherTestRoute("dep", routeProvider, nil)
	// The guard resolves dependency entries through the compose project and
	// service labels, the same way NewWatcher resolves them.
	depRoute.container.Labels["com.docker.compose.service"] = "dep"
	routeProvider.routes["main"] = mainRoute
	routeProvider.routes["dep"] = depRoute

	alwaysOn := newIdlewatcherTestRoute("always-on", routeProvider, nil)
	alwaysOn.container = docker.FromDocker(t.Context(), &dockerapi.Summary{
		Names: []string{"/always-on"},
		State: "running",
		Labels: map[string]string{
			"com.docker.compose.project": "idlewatcher-test",
			"com.docker.compose.service": "always-on",
			docker.LabelDependsOn:        "dep:service_started",
		},
	}, godoxytypes.DockerProviderConfig{})
	ep.ExcludedRoutes().Add(alwaysOn)

	w, err := NewWatcher(parent, mainRoute, mainRoute.cfg)
	require.NoError(t, err)
	dep := w.dependsOn[0].Watcher
	setRunning(w, dep)

	// Route has no health monitor (status unknown): the dependency stays up.
	require.NoError(t, w.stopByMethod())
	require.Equal(t, []string{"stop:main-id"}, recordedActions(&mu, &actions))
	mu.Lock()
	actions = nil
	mu.Unlock()

	// Model the runtime's stop event, then a healthy route still pins.
	w.storeState(&containerState{status: idlewatchertypes.ContainerStatusStopped})
	mon := &fakeHealthMonitor{status: health.StatusHealthy}
	alwaysOn.SetHealthMonitor(mon)
	require.NoError(t, w.stopByMethod())
	require.Empty(t, recordedActions(&mu, &actions))
	mu.Lock()
	actions = nil
	mu.Unlock()

	// A down route does not pin its dependency awake.
	mon.status = health.StatusUnhealthy
	require.NoError(t, w.stopByMethod())
	require.Equal(t, []string{"stop:dep-id"}, recordedActions(&mu, &actions))
}
