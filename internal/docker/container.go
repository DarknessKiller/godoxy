package docker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/yusing/godoxy/agent/pkg/agent"
	"github.com/yusing/godoxy/internal/agentpool"
	env "github.com/yusing/godoxy/internal/env"
	idlewatcher "github.com/yusing/godoxy/internal/idlewatcher/runtime"
	"github.com/yusing/godoxy/internal/serialization"
	"github.com/yusing/godoxy/internal/types"
	gperr "github.com/yusing/goutils/errs"
	strutils "github.com/yusing/goutils/strings"
)

var DummyContainer = new(Container)

var EnvDockerHost = env.String("DOCKER_HOST")

var (
	ErrNetworkNotFound = errors.New("network not found")
	ErrNoNetwork       = errors.New("no network found")
)

func FromDocker(ctx context.Context, c *container.Summary, dockerCfg types.DockerProviderConfig) (res *Container) {
	actualLabels := maps.Clone(c.Labels)

	_, isExplicit := c.Labels[LabelAliases]
	helper := containerHelper{c}
	if !isExplicit {
		// walk through all labels to check if any label starts with NSProxy.
		for lbl := range c.Labels {
			if strings.HasPrefix(lbl, NSProxy+".") {
				isExplicit = true
				break
			}
		}
	}
	network := helper.getDeleteLabel(LabelNetwork)

	excludeValue, hasExclude := c.Labels[LabelExclude]
	delete(c.Labels, LabelExclude)
	excludeFlags, excludeErr := parseExcludeLabel(excludeValue, hasExclude)
	res = &Container{
		DockerCfg:     dockerCfg,
		Image:         helper.parseImage(),
		ContainerName: helper.getName(),
		ContainerID:   c.ID,

		Labels:       c.Labels,
		ActualLabels: actualLabels,

		Mounts: helper.getMounts(),

		Network:            network,
		PublicPortMapping:  helper.getPublicPortMapping(),
		PrivatePortMapping: helper.getPrivatePortMapping(),

		Aliases:            helper.getAliases(),
		ExcludeFlags:       excludeFlags,
		IsExplicit:         isExplicit,
		IsHostNetworkMode:  c.HostConfig.NetworkMode == "host",
		HealthCheckEnabled: hasHealthCheck(c.Status),
		Running:            c.Status == "running" || c.State == "running",
		State:              c.State,
	}
	if excludeErr != nil {
		addError(res, excludeErr)
	}

	if agent.IsDockerHostAgent(dockerCfg.URL) {
		var ok bool
		pool := agentpool.FromCtx(ctx)
		if pool != nil {
			res.Agent, ok = pool.Get(dockerCfg.URL)
		}
		if !ok {
			addError(res, fmt.Errorf("agent %q not found", dockerCfg.URL))
		}
	}

	setPrivateHostname(res, helper)
	setPublicHostname(res)
	loadDeleteIdlewatcherLabels(ctx, res, helper)

	if res.PrivateHostname == "" && res.PublicHostname == "" && res.Running {
		addError(res, ErrNoNetwork)
	}
	return res
}

func parseExcludeLabel(value string, present bool) (ExcludeFlag, error) {
	if !present {
		return 0, nil
	}

	if boolValue, boolErr := strconv.ParseBool(value); boolErr == nil {
		if boolValue {
			return ExcludeProxy, nil
		}
		return 0, nil
	}

	var flags ExcludeFlag
	var all, other bool
	for item := range strings.SplitSeq(value, ",") {
		switch strings.TrimSpace(item) {
		case "proxy":
			flags |= ExcludeProxy
			other = true
		case "healthcheck":
			flags |= ExcludeHealthCheck
			other = true
		case "all":
			flags |= ExcludeAll
			all = true
		default:
			return 0, fmt.Errorf(
				"invalid %s value %q: expected a boolean or a comma-separated list of proxy, healthcheck, or all",
				LabelExclude,
				value,
			)
		}
	}

	if all && other {
		return 0, fmt.Errorf("invalid %s value %q: all cannot be combined with other values", LabelExclude, value)
	}
	return flags, nil
}

func hasHealthCheck(status string) bool {
	return strings.HasSuffix(status, "("+string(container.Healthy)+")") ||
		strings.HasSuffix(status, "("+string(container.Unhealthy)+")") ||
		strings.HasSuffix(status, "(health: "+string(container.Starting)+")")
}

func IsBlacklisted(c *Container) bool {
	return IsBlacklistedImage(c.Image) || isDatabase(c)
}

func UpdatePorts(ctx context.Context, c *Container) error {
	dockerClient, err := NewClient(ctx, c.DockerCfg)
	if err != nil {
		return err
	}
	defer dockerClient.Close()

	inspect, err := dockerClient.ContainerInspect(ctx, c.ContainerID)
	if err != nil {
		return err
	}

	for port := range inspect.Config.ExposedPorts {
		proto, portStr := port.Proto(), port.Port()
		portInt, _ := nat.ParsePort(portStr)
		if portInt == 0 {
			continue
		}
		c.PublicPortMapping[portInt] = container.Port{
			PublicPort:  uint16(portInt), //nolint:gosec
			PrivatePort: uint16(portInt), //nolint:gosec
			Type:        proto,
		}
	}
	return nil
}

func DockerComposeProject(c *Container) string {
	return c.Labels["com.docker.compose.project"]
}

func DockerComposeService(c *Container) string {
	return c.Labels["com.docker.compose.service"]
}

// dependenciesLabel returns the raw dependency list from proxy.depends_on,
// falling back to the docker compose label.
func dependenciesLabel(c *Container) string {
	// ActualLabels is read instead of Labels: the idlewatcher label pass
	// deletes proxy.depends_on before the config is built.
	if deps := c.ActualLabels[LabelDependsOn]; deps != "" {
		return deps
	}
	return c.ActualLabels["com.docker.compose.depends_on"]
}

// Dependencies parses the container dependency label.
// One-liners are comma or space separated; multiline and list-like values are
// YAML, matching the idlewatcher config deserialization.
func Dependencies(c *Container) []string {
	raw := dependenciesLabel(c)
	if raw == "" {
		return nil
	}
	if strings.IndexByte(raw, '\n') != -1 || raw[0] == '-' || raw[0] == '[' {
		var deps []string
		if err := strutils.UnmarshalYAML([]byte(raw), &deps); err == nil {
			return deps
		}
	}
	return strutils.CommaSeperatedList(raw)
}

var databaseMPs = map[string]struct{}{
	"/var/lib/postgresql/data": {},
	"/var/lib/mysql":           {},
	"/var/lib/mongodb":         {},
	"/var/lib/mariadb":         {},
	"/var/lib/memcached":       {},
	"/var/lib/rabbitmq":        {},
}

func isDatabase(c *Container) bool {
	if c.Mounts != nil { // only happens in test
		for _, m := range c.Mounts.Iter {
			if _, ok := databaseMPs[m]; ok {
				return true
			}
		}
	}

	for _, v := range c.PrivatePortMapping {
		switch v.PrivatePort {
		// postgres, mysql or mariadb, redis, memcached, mongodb
		case 5432, 3306, 6379, 11211, 27017:
			return true
		}
	}
	return false
}

func isLocal(c *Container) bool {
	if strings.HasPrefix(c.DockerCfg.URL, "unix://") {
		return true
	}
	// treat it as local if the docker host is the same as the environment variable
	if c.DockerCfg.URL == EnvDockerHost {
		return true
	}
	url, err := url.Parse(c.DockerCfg.URL)
	if err != nil {
		return false
	}
	hostname := url.Hostname()
	if hostname == "localhost" {
		return true
	}
	ip := net.ParseIP(hostname)
	if ip != nil {
		return ip.IsLoopback() || ip.IsUnspecified()
	}
	return false
}

func setPublicHostname(c *Container) {
	if !c.Running {
		return
	}
	if isLocal(c) {
		c.PublicHostname = "127.0.0.1"
		return
	}
	url, err := url.Parse(c.DockerCfg.URL)
	if err != nil {
		c.PublicHostname = "127.0.0.1"
		return
	}
	c.PublicHostname = url.Hostname()
}

func setPrivateHostname(c *Container, helper containerHelper) {
	if !isLocal(c) && c.Agent == nil {
		return
	}
	if helper.NetworkSettings == nil {
		return
	}
	if c.Network != "" {
		v, hasNetwork := helper.NetworkSettings.Networks[c.Network]
		if hasNetwork && v.IPAddress != "" {
			c.PrivateHostname = v.IPAddress
			return
		}
		var hasComposeNetwork bool
		// try {project_name}_{network_name}
		if proj := DockerComposeProject(c); proj != "" {
			newNetwork := fmt.Sprintf("%s_%s", proj, c.Network)
			v, hasComposeNetwork = helper.NetworkSettings.Networks[newNetwork]
			if hasComposeNetwork && v.IPAddress != "" {
				c.Network = newNetwork // update network to the new one
				c.PrivateHostname = v.IPAddress
				return
			}
		}
		if hasNetwork || hasComposeNetwork { // network is found, but no IP assigned yet
			return
		}
		nearest := gperr.DoYouMeanField(c.Network, helper.NetworkSettings.Networks)
		addError(c, fmt.Errorf("network %q not found, %w", c.Network, nearest))
		return
	}
	// fallback to first network if no network is specified
	for k, v := range helper.NetworkSettings.Networks {
		if v.IPAddress != "" {
			c.Network = k // update network to the first network
			c.PrivateHostname = v.IPAddress
			return
		}
	}
}

func loadDeleteIdlewatcherLabels(ctx context.Context, c *Container, helper containerHelper) {
	hasIdleTimeout := false
	cfg := make(map[string]any, len(idlewatcherLabels))
	for lbl, key := range idlewatcherLabels {
		value := helper.getDeleteLabel(lbl)
		if lbl == LabelDependsOn {
			// Resolved after the loop so the podman fallback only runs for
			// containers that actually have an idle timeout.
			continue
		}
		if value == "" {
			continue
		}
		cfg[key] = value
		if lbl == LabelIdleTimeout {
			hasIdleTimeout = true
		}
	}
	if hasIdleTimeout {
		// Raw value: serialization parses comma-separated one-liners and YAML lists.
		if raw := dependenciesLabel(c); raw != "" {
			cfg[idlewatcherLabels[LabelDependsOn]] = raw
		} else if deps := podmanDependencies(ctx, c.DockerCfg.URL, c.ContainerID); len(deps) > 0 {
			// podman-compose stores compose depends_on as podman --requires,
			// which only the libpod API exposes.
			cfg[idlewatcherLabels[LabelDependsOn]] = deps
		}
	}

	if _, present := helper.Labels[LabelIdleNotifyTo]; present {
		to := any(helper.getDeleteLabel(LabelIdleNotifyTo))
		if to == "" {
			to = []string{}
		}
		cfg["notify"] = map[string]any{"to": to}
	}

	// set only if idlewatcher is enabled
	if hasIdleTimeout {
		idwCfg := new(idlewatcher.IdlewatcherConfig)
		idwCfg.Docker = &idlewatcher.DockerConfig{
			DockerCfg:     c.DockerCfg,
			ContainerID:   c.ContainerID,
			ContainerName: c.ContainerName,
		}

		if err := serialization.MapUnmarshalValidate(cfg, idwCfg); err != nil {
			addError(c, err)
		} else {
			c.IdlewatcherConfig = idwCfg
		}
	}
}

func addError(c *Container, err error) {
	if c.Errors == nil {
		c.Errors = new(ContainerError)
	}
	c.Errors.Add(err)
}
