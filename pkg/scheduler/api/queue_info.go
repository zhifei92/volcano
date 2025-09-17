/*
Copyright 2018 The Kubernetes Authors.

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

package api

import (
	"encoding/json"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

// QueueID is UID type, serves as unique ID for each queue
type QueueID types.UID

// QueueInfo will have all details about queue
type QueueInfo struct {
	UID  QueueID
	Name string

	Weight int32

	// Weights is a list of slash sperated float numbers.
	// Each of them is a weight corresponding the
	// hierarchy level.
	Weights string
	// Hierarchy is a list of node name along the
	// path from the root to the node itself.
	Hierarchy string

	Queue *scheduling.Queue

	// jdosDeviceMap represents the resource mapping relationship within the queue,
	// parsed from a JSON structure similar to the following:
	//
	// {
	//  "A100": {
	//     "nvidia.com/gpu": "nvidia.com/gpu-A100",
	//     "vgpu.com/core": "vgpu.com/core-A100",
	//     "vgpu.com/memory": "vgpu.com/memory-A100"
	//   },
	//    "K80": {
	//     "nvidia.com/gpu": "nvidia.com/gpu-K80",
	//     "vgpu.com/core": "vgpu.com/core-K80",
	//     "vgpu.com/memory": "vgpu.com/memory-K80"
	//   }
	// }
	// The key is the type of the device(such as A100), and the value is a map of resource names to resource names.
	JDosDeviceMap map[string]map[v1.ResourceName]v1.ResourceName
}

// NewQueueInfo creates new queueInfo object
func NewQueueInfo(queue *scheduling.Queue) *QueueInfo {
	return &QueueInfo{
		UID:  QueueID(queue.Name),
		Name: queue.Name,

		Weight:    queue.Spec.Weight,
		Hierarchy: queue.Annotations[v1beta1.KubeHierarchyAnnotationKey],
		Weights:   queue.Annotations[v1beta1.KubeHierarchyWeightAnnotationKey],

		Queue: queue,

		JDosDeviceMap: parseDeviceMap(queue),
	}
}

// Clone is used to clone queueInfo object
func (q *QueueInfo) Clone() *QueueInfo {
	return &QueueInfo{
		UID:           q.UID,
		Name:          q.Name,
		Weight:        q.Weight,
		Hierarchy:     q.Hierarchy,
		Weights:       q.Weights,
		Queue:         q.Queue,
		JDosDeviceMap: deepCopyMap(q.JDosDeviceMap),
	}
}

// Reclaimable return whether queue is reclaimable
func (q *QueueInfo) Reclaimable() bool {
	if q == nil {
		return false
	}

	if q.Queue == nil {
		return false
	}

	if q.Queue.Spec.Reclaimable == nil {
		return true
	}

	return *q.Queue.Spec.Reclaimable
}

func (q *QueueInfo) IsJDosDeviceMapQueue() bool {
	return len(q.JDosDeviceMap) != 0
}

func parseDeviceMap(queue *scheduling.Queue) map[string]map[v1.ResourceName]v1.ResourceName {
	annotations := queue.Annotations
	if annotations == nil {
		return nil
	}
	deviceMap, ok := annotations[JDosDeviceMapAnnotation]
	if !ok {
		return nil
	}
	jdosDeviceMap := map[string]map[v1.ResourceName]v1.ResourceName{}
	if err := json.Unmarshal([]byte(deviceMap), &jdosDeviceMap); err != nil {
		klog.ErrorS(err, "Unmarshal device map failed")
		return nil
	}
	klog.V(3).InfoS("Parse device map successfully", "deviceMap", jdosDeviceMap)
	return jdosDeviceMap
}

func deepCopyMap(m map[string]map[v1.ResourceName]v1.ResourceName) map[string]map[v1.ResourceName]v1.ResourceName {
	m2 := make(map[string]map[v1.ResourceName]v1.ResourceName, len(m))

	for outerKey, innerMap := range m {
		newInnerMap := make(map[v1.ResourceName]v1.ResourceName, len(innerMap))
		for innerKey, value := range innerMap {
			newInnerMap[innerKey] = value
		}
		m2[outerKey] = newInnerMap
	}

	return m2
}
