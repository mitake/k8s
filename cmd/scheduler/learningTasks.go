package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/tensorflow/k8s/cmd/scheduler/k8stype"
	"github.com/tensorflow/k8s/pkg/spec"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kwatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

type learningTask struct {
	name               string
	tfjob              *spec.TfJob
	pods               []*k8stype.Pod
	nrRequiredReplicas int
}

type learningTaskMaker struct {
	learningTasks map[string]*learningTask

	podCh      chan *k8stype.Pod
	ltCh       chan *learningTask
	orphanPods map[string][]*k8stype.Pod

	client *kubernetes.Clientset
}

type Event struct {
	Type   kwatch.EventType
	Object *spec.TfJob
}

type rawEvent struct {
	Type   kwatch.EventType
	Object json.RawMessage
}

func pollEvent(decoder *json.Decoder) (*Event, *metav1.Status, error) {
	re := &rawEvent{}
	err := decoder.Decode(re)
	if err != nil {
		if err == io.EOF {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("fail to decode raw event from apiserver (%v)", err)
	}

	if re.Type == kwatch.Error {
		status := &metav1.Status{}
		err = json.Unmarshal(re.Object, status)
		if err != nil {
			return nil, nil, fmt.Errorf("fail to decode (%s) into metav1.Status (%v)", re.Object, err)
		}
		return nil, status, nil
	}

	ev := &Event{
		Type:   re.Type,
		Object: &spec.TfJob{},
	}
	err = json.Unmarshal(re.Object, ev.Object)
	if err != nil {
		return nil, nil, fmt.Errorf("fail to unmarshal Cluster object from data (%s): %v", re.Object, err)
	}
	return ev, nil, nil
}

func listTfJobsURI(ns string) string {
	return fmt.Sprintf("/apis/%s/%s/namespaces/%s/%s", spec.CRDGroup, spec.CRDVersion, ns, spec.CRDKindPlural)
}

func tfJobWatch(jobch chan *spec.TfJob, c *kubernetes.Clientset) {
	restcli := c.CoreV1Client.RESTClient()

	b, err := restcli.Get().RequestURI(listTfJobsURI("default")).DoRaw()
	if err != nil {
		fmt.Printf("failed to list tf jobs: %s\n", err)
		os.Exit(1)
	}

	list := &spec.TfJobList{}
	if err := json.Unmarshal(b, list); err != nil {
		fmt.Printf("failed to list tf jobs: %s\n", err)
		os.Exit(1)
	}

	jobch <- &list.Items[0]

	version := list.Metadata.ResourceVersion
	fmt.Printf("initial version: %s\n", version)

	for {
		result := restcli.Get().RequestURI(fmt.Sprintf("/apis/%s/%s/%s?watch=true&resourceVersion=%s",
			spec.CRDGroup, spec.CRDVersion, spec.CRDKindPlural, version)).Do()
		body, err := result.Raw()
		if err != nil {
			fmt.Printf("failed to watch tfJob: %s\n", err)
			os.Exit(1)
		}

		decoder := json.NewDecoder(bytes.NewReader(body))
		ev, st, err := pollEvent(decoder)
		if st != nil || err != nil {
			fmt.Printf("decoding resp body failed: %v %v\n", st, err)
			os.Exit(1)
		}

		fmt.Printf("received tfjob object: %v\n", ev.Object)

		jobch <- ev.Object
		version = ev.Object.Metadata.ResourceVersion
	}
}

func (maker *learningTaskMaker) run() {
	jobCh := make(chan *spec.TfJob)
	go tfJobWatch(jobCh, maker.client)

	for {
		select {
		case pod := <-maker.podCh:

			fmt.Printf("learningTaskMaker: handling newly arrived pod %s\n", pod.PodInfo.Name)
			var lt *learningTask
			var ok bool
			jobName := pod.PodInfo.Labels["tf_job_name"]
			if lt, ok = maker.learningTasks[jobName]; !ok {
				// tf job object isn't observed
				var orphans []*k8stype.Pod
				if orphans, ok = maker.orphanPods[jobName]; !ok {
					maker.orphanPods[jobName] = make([]*k8stype.Pod, 0)
					orphans = maker.orphanPods[jobName]
				}
				orphans = append(orphans, pod)
			} else {
				lt.pods = append(lt.pods, pod)

				if len(lt.pods) == lt.nrRequiredReplicas {
					fmt.Printf("lt job %s requires %d pods and all of them are ready, launching\n", jobName, lt.nrRequiredReplicas)
					maker.ltCh <- lt
				}
			}

		case job := <-jobCh:
			fmt.Printf("newly arrived job name: %s\n", job.Metadata.Name)

			jobName := job.Metadata.Name
			if _, ok := maker.learningTasks[jobName]; ok {
				// FIXME: jobname shouldn't be ID
				fmt.Printf("duplicated tf job %s\n", jobName)
				os.Exit(1)
			}

			lt := &learningTask{
				name:  jobName,
				tfjob: job,
				pods:  make([]*k8stype.Pod, 0),
			}
			for _, r := range job.Spec.ReplicaSpecs {
				lt.nrRequiredReplicas += int(*r.Replicas)
			}

			maker.learningTasks[jobName] = lt

			if pods, ok := maker.orphanPods[jobName]; ok {
				lt.pods = pods

				if len(lt.pods) == lt.nrRequiredReplicas {
					fmt.Printf("lt job %s requires %d pods and all of them are ready, launching\n", jobName, lt.nrRequiredReplicas)
					maker.ltCh <- lt
				}
			}

		}
	}
}

func runLearningTaskMaker(podCh chan *k8stype.Pod, client *kubernetes.Clientset) chan *learningTask {
	maker := &learningTaskMaker{
		learningTasks: make(map[string]*learningTask),
		podCh:         podCh,
		ltCh:          make(chan *learningTask),
		client:        client,
		orphanPods:    make(map[string][]*k8stype.Pod),
	}

	go maker.run()

	return maker.ltCh
}
