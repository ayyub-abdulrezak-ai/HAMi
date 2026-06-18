/*
Copyright 2024 The HAMi Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/procfs"
	cli "github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	klog "k8s.io/klog/v2"
)

const (
	defaultNodeLabel       = "hami.io/device-plugin.config"
	defaultProcessToSignal = "nvidia-device-plugin"
	defaultResyncInterval  = 30 * time.Second
)

type options struct {
	oneshot         bool
	kubeconfig      string
	nodeName        string
	nodeLabel       string
	baseConfigFile  string
	poolsSrcdir     string
	configFileDst   string
	processToSignal string
	resyncInterval  time.Duration
}

func main() {
	o := &options{}
	app := cli.NewApp()
	app.Name = "config-manager"
	app.Usage = "watches a node label and hot-reloads the HAMi device plugin config"
	app.Before = func(c *cli.Context) error { return validate(o) }
	app.Action = func(c *cli.Context) error { return run(o) }
	app.Flags = []cli.Flag{
		&cli.BoolFlag{
			Name:        "oneshot",
			Usage:       "write config once and exit (used by init container)",
			Destination: &o.oneshot,
			EnvVars:     []string{"ONESHOT"},
		},
		&cli.StringFlag{
			Name:        "kubeconfig",
			Usage:       "path to kubeconfig file; defaults to in-cluster config",
			Destination: &o.kubeconfig,
			EnvVars:     []string{"KUBECONFIG"},
		},
		&cli.StringFlag{
			Name:        "node-name",
			Usage:       "name of this node",
			Destination: &o.nodeName,
			EnvVars:     []string{"NODE_NAME"},
		},
		&cli.StringFlag{
			Name:        "node-label",
			Value:       defaultNodeLabel,
			Usage:       "node label key to watch for pool selection",
			Destination: &o.nodeLabel,
			EnvVars:     []string{"NODE_LABEL"},
		},
		&cli.StringFlag{
			Name:        "base-config-file",
			Usage:       "path to the base device-config.yaml",
			Destination: &o.baseConfigFile,
			EnvVars:     []string{"BASE_CONFIG_FILE"},
		},
		&cli.StringFlag{
			Name:        "pools-srcdir",
			Usage:       "directory containing pool patch files (one file per pool name)",
			Destination: &o.poolsSrcdir,
			EnvVars:     []string{"POOLS_SRCDIR"},
		},
		&cli.StringFlag{
			Name:        "config-file-dst",
			Usage:       "path to write the merged config file",
			Destination: &o.configFileDst,
			EnvVars:     []string{"CONFIG_FILE_DST"},
		},
		&cli.StringFlag{
			Name:        "process-to-signal",
			Value:       defaultProcessToSignal,
			Usage:       "name of the process to send SIGHUP to after a config update",
			Destination: &o.processToSignal,
			EnvVars:     []string{"PROCESS_TO_SIGNAL"},
		},
		&cli.DurationFlag{
			Name:        "resync-interval",
			Value:       defaultResyncInterval,
			Usage:       "how often to re-merge config to pick up base ConfigMap changes",
			Destination: &o.resyncInterval,
			EnvVars:     []string{"RESYNC_INTERVAL"},
		},
	}
	if err := app.Run(os.Args); err != nil {
		klog.Error(err)
		os.Exit(1)
	}
}

func validate(o *options) error {
	if o.nodeName == "" {
		return fmt.Errorf("--node-name must not be empty")
	}
	if o.baseConfigFile == "" {
		return fmt.Errorf("--base-config-file must not be empty")
	}
	if o.poolsSrcdir == "" {
		return fmt.Errorf("--pools-srcdir must not be empty")
	}
	if o.configFileDst == "" {
		return fmt.Errorf("--config-file-dst must not be empty")
	}
	return nil
}

// labelState holds the current node label value, updated by the K8s informer.
type labelState struct {
	mu    sync.RWMutex
	value string
}

func (s *labelState) set(v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value = v
}

func (s *labelState) get() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.value
}

func run(o *options) error {
	restConfig, err := clientcmd.BuildConfigFromFlags("", o.kubeconfig)
	if err != nil {
		return fmt.Errorf("building kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("building clientset: %w", err)
	}

	label := &labelState{}

	// trigger is a non-blocking channel used to coalesce reconcile requests.
	trigger := make(chan struct{}, 1)
	notify := func() {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}

	lw := cache.NewListWatchFromClient(
		clientset.CoreV1().RESTClient(),
		"nodes",
		corev1.NamespaceAll,
		fields.OneTermEqualSelector("metadata.name", o.nodeName),
	)
	_, ctrl := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: lw,
		ObjectType:    &corev1.Node{},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				label.set(obj.(*corev1.Node).Labels[o.nodeLabel])
				notify()
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldVal := oldObj.(*corev1.Node).Labels[o.nodeLabel]
				newVal := newObj.(*corev1.Node).Labels[o.nodeLabel]
				if oldVal != newVal {
					label.set(newVal)
					notify()
				}
			},
		},
		ResyncPeriod: 0,
	})

	stop := make(chan struct{})
	defer close(stop)
	go ctrl.Run(stop)

	if !cache.WaitForCacheSync(stop, ctrl.HasSynced) {
		return fmt.Errorf("timed out waiting for cache sync")
	}

	// In oneshot mode write the initial config and exit (used by the init container).
	// No SIGHUP is sent because the device plugin has not started yet.
	if o.oneshot {
		content, err := mergeConfig(label.get(), o)
		if err != nil {
			return err
		}
		return writeConfig(content, o.configFileDst)
	}

	ticker := time.NewTicker(o.resyncInterval)
	defer ticker.Stop()

	var lastContent string
	for {
		select {
		case <-trigger:
		case <-ticker.C:
		}

		content, err := mergeConfig(label.get(), o)
		if err != nil {
			klog.Errorf("Merging config: %v", err)
			continue
		}
		if content == lastContent {
			continue
		}
		if err := writeConfig(content, o.configFileDst); err != nil {
			klog.Errorf("Writing config: %v", err)
			continue
		}
		lastContent = content
		klog.Infof("Config updated (label=%q), sending SIGHUP to %s", label.get(), o.processToSignal)
		if err := signalProcess(o.processToSignal); err != nil {
			klog.Errorf("Sending SIGHUP: %v", err)
		}
	}
}

// mergeConfig reads the base config and, if labelValue is non-empty, deep-merges
// the matching pool patch on top of it. The result is returned as a YAML string.
func mergeConfig(labelValue string, o *options) (string, error) {
	baseBytes, err := os.ReadFile(o.baseConfigFile)
	if err != nil {
		return "", fmt.Errorf("reading base config: %w", err)
	}

	var base map[string]interface{}
	if err := yaml.Unmarshal(baseBytes, &base); err != nil {
		return "", fmt.Errorf("parsing base config: %w", err)
	}

	if labelValue != "" {
		patchPath := filepath.Join(o.poolsSrcdir, labelValue)
		patchBytes, err := os.ReadFile(patchPath)
		if err != nil {
			return "", fmt.Errorf("reading pool patch %q: %w", labelValue, err)
		}
		var patch map[string]interface{}
		if err := yaml.Unmarshal(patchBytes, &patch); err != nil {
			return "", fmt.Errorf("parsing pool patch %q: %w", labelValue, err)
		}
		mergeMaps(base, patch)
	}

	out, err := yaml.Marshal(base)
	if err != nil {
		return "", fmt.Errorf("marshaling merged config: %w", err)
	}
	return string(out), nil
}

// mergeMaps recursively merges src into dst. Where both values are maps it
// recurses; otherwise src values overwrite dst values.
func mergeMaps(dst, src map[string]interface{}) {
	for k, sv := range src {
		if dv, ok := dst[k]; ok {
			dstMap, dstIsMap := dv.(map[string]interface{})
			srcMap, srcIsMap := sv.(map[string]interface{})
			if dstIsMap && srcIsMap {
				mergeMaps(dstMap, srcMap)
				continue
			}
		}
		dst[k] = sv
	}
}

// writeConfig atomically writes content to dst via a temp file rename.
func writeConfig(content, dst string) error {
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".config-manager-*")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.WriteString(content); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("writing temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("closing temp file: %w", err)
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("renaming to destination: %w", err)
	}
	return nil
}

// signalProcess finds the named process via /proc and sends SIGHUP.
func signalProcess(name string) error {
	procs, err := procfs.AllProcs()
	if err != nil {
		return fmt.Errorf("listing procs: %w", err)
	}
	for _, p := range procs {
		cmdline, err := p.CmdLine()
		if err != nil || len(cmdline) == 0 {
			continue
		}
		if filepath.Base(cmdline[0]) == name {
			if err := syscall.Kill(p.PID, syscall.SIGHUP); err != nil {
				return fmt.Errorf("kill pid %d: %w", p.PID, err)
			}
			klog.Infof("Sent SIGHUP to %s (pid %d)", name, p.PID)
			return nil
		}
	}
	return fmt.Errorf("process %q not found", name)
}
