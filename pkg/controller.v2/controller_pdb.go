// Copyright 2018 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"fmt"

	tfv1alpha2 "github.com/kubeflow/tf-operator/pkg/apis/tensorflow/v1alpha2"
	"github.com/kubeflow/tf-operator/pkg/generator"
	log "github.com/sirupsen/logrus"
	"k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var (
	groupVersionKind = schema.GroupVersionKind{
		Group:   tfv1alpha2.GroupName,
		Version: tfv1alpha2.GroupVersion,
		Kind:    tfv1alpha2.Kind,
	}
)

func asOwner(tfJob *tfv1alpha2.TFJob) metav1.OwnerReference {
	trueVar := true
	// Both api.OwnerReference and metatypes.OwnerReference are combined into that.
	return metav1.OwnerReference{
		APIVersion:         groupVersionKind.GroupVersion().String(),
		Kind:               groupVersionKind.Kind,
		Name:               tfJob.ObjectMeta.Name,
		UID:                tfJob.ObjectMeta.UID,
		Controller:         &trueVar,
		BlockOwnerDeletion: &trueVar,
	}
}

func (tc *TFJobController) reconcilePdb(tfjob *tfv1alpha2.TFJob) error {
	pdb, err := tc.getPdbForTFJob(tfjob)
	if err != nil {
		return err
	}

	if pdb != nil {
		return nil
	}

	nrReplicas := int32(0)
	for _, spec := range tfjob.Spec.TFReplicaSpecs {
		nrReplicas += *spec.Replicas
	}

	if nrReplicas == 1 {
		// gang scheduling isn't required by a non distributed training process
		return nil
	}

	minAvailable := intstr.FromInt(int(nrReplicas))
	pdb = &v1beta1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tf-job-pdb-" + tfjob.ObjectMeta.Name,
			OwnerReferences: []metav1.OwnerReference{
				asOwner(tfjob),
			},
		},
		Spec: v1beta1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: generator.GenLabels(tfjob.Name),
			},
		},
	}

	log.Infof("Creating PDB: %v", pdb.ObjectMeta.Name)
	_, err = tc.kubeClientSet.PolicyV1beta1().PodDisruptionBudgets(tfjob.Namespace).Create(pdb)
	return err
}

func (tc *TFJobController) addPdb(obj interface{}) {
	// TODO: handle this gracefully.
}

func (tc *TFJobController) updatePdb(old, cur interface{}) {
	// TODO: handle this gracefully.
}

func (tc *TFJobController) deletePdb(obj interface{}) {
	// TODO: handle this gracefully.
}

func (tc *TFJobController) getPdbForTFJob(tfjob *tfv1alpha2.TFJob) (*v1beta1.PodDisruptionBudget, error) {
	// Create selector
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: generator.GenLabels(tfjob.Name),
	})
	if err != nil {
		return nil, fmt.Errorf("couldn't convert Job selector: %v", err)
	}

	pdbs, err := tc.pdbLister.PodDisruptionBudgets(tfjob.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	if len(pdbs) != 1 {
		return nil, fmt.Errorf("multiple PDBs for a single TFJob")
	} else if len(pdbs) == 0 {
		return nil, nil // the PDB isn't created yet
	}

	return pdbs[0], nil
}
