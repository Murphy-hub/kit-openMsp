package election

import (
	"context"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"
	"os"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
)

// Config holds the configuration for a leader election
type Config struct {
	Client     clientset.Interface
	ElectionID string
	Callbacks  leaderelection.LeaderCallbacks
	Logger     *zap.SugaredLogger
}

type Elector struct {
	Config
	Elector *leaderelection.LeaderElector
}

func (e Elector) Run(ctx context.Context) {
	retryCount := 0 // Count of previous attempts, biased by one for "session" labels.
	wait.NonSlidingUntilWithContext(ctx, func(ctx context.Context) {
		if retryCount > 0 {
			e.Logger.Warnf("leader election session terminated unexpectedly, retry times: %v", retryCount)
		}
		e.Logger.Info("starting leader election")
		e.Elector.Run(ctx)
		retryCount++
	}, time.Second)
}

func (e Elector) IsLeader() bool {
	return e.Elector.IsLeader()
}

// NewElector returns an instance of Elector based on config.
func NewElector(config Config) Elector {
	pod, err := GetPodDetails()
	if err != nil {
		// XXX remove this fatal log and bubble up the error
		config.Logger.Fatalf("failed to obtain pod info: %v", err)
	}

	es := Elector{
		Config: config,
	}

	broadcaster := record.NewBroadcaster()
	hostname, _ := os.Hostname()

	recorder := broadcaster.NewRecorder(scheme.Scheme, apiv1.EventSource{
		Component: "leader-elector",
		Host:      hostname,
	})

	lock := resourcelock.ConfigMapLock{
		ConfigMapMeta: metav1.ObjectMeta{Namespace: pod.Namespace,
			Name: config.ElectionID},
		Client: config.Client.CoreV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity:      pod.Name,
			EventRecorder: recorder,
		},
	}

	ttl := 30 * time.Second
	le, err := leaderelection.NewLeaderElector(
		leaderelection.LeaderElectionConfig{
			Lock:            &lock,
			LeaseDuration:   ttl,
			RenewDeadline:   ttl / 2,
			RetryPeriod:     ttl / 4,
			Callbacks:       config.Callbacks,
			ReleaseOnCancel: true,
		})

	if err != nil {
		es.Logger.Fatalf("failed to start elector: %v", err)
	}

	es.Elector = le
	return es
}
