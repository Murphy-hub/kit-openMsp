package election

import (
	"fmt"
	"os"
)

// PodInfo contains runtime information about the pod running the Ingres controller
type PodInfo struct {
	Name      string
	Namespace string
	NodeIP    string
	// Labels selectors of the running pod
	// This is used to search for other Ingress controller pods
	Labels map[string]string
}

// GetPodDetails returns runtime information about the pod:
// name, namespace and IP of the node where it is running
func GetPodDetails() (*PodInfo, error) {
	podName := os.Getenv("PODNAME")
	podNs := os.Getenv("PODNAMESPACE")
	nodeIP := os.Getenv("NODEIP")

	if podName == "" || podNs == "" || nodeIP == "" {
		return nil, fmt.Errorf("unable to get POD information (missing POD_NAME or POD_NAMESPACE or NODE_IP environment variable")
	}

	return &PodInfo{
		Name:      podName,
		Namespace: podNs,
		NodeIP:    nodeIP,
	}, nil
}
