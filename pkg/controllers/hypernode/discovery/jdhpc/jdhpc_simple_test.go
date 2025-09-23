/*
Copyright 2025 The Volcano Authors.

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

package jdhpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"volcano.sh/volcano/pkg/controllers/hypernode/api"
)

func TestJDHPCDiscoverer_Name(t *testing.T) {
	jd := &jdHPCDiscoverer{}
	assert.Equal(t, "jdhpc", jd.Name())
}

func TestJDHPCDiscoverer_getCredentialsFromSecret(t *testing.T) {
	tests := []struct {
		name        string
		secretName  string
		secretData  map[string][]byte
		expectError bool
		expectedAK  string
		expectedSK  string
	}{
		{
			name:       "ValidSecret",
			secretName: "test-secret",
			secretData: map[string][]byte{
				"accessKey":       []byte("test-ak"),
				"secretAccessKey": []byte("test-sk"),
			},
			expectError: false,
			expectedAK:  "test-ak",
			expectedSK:  "test-sk",
		},
		{
			name:       "MissingAccessKey",
			secretName: "test-secret",
			secretData: map[string][]byte{
				"secretAccessKey": []byte("test-sk"),
			},
			expectError: true,
		},
		{
			name:       "MissingSecretAccessKey",
			secretName: "test-secret",
			secretData: map[string][]byte{
				"accessKey": []byte("test-ak"),
			},
			expectError: true,
		},
		{
			name:        "SecretNotFound",
			secretName:  "nonexistent",
			secretData:  map[string][]byte{},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset()

			if tc.secretData != nil {
				secret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      tc.secretName,
						Namespace: "default",
					},
					Data: tc.secretData,
				}
				_, err := fakeClient.CoreV1().Secrets("default").Create(
					context.TODO(), secret, metav1.CreateOptions{},
				)
				if err != nil {
					t.Fatalf("Failed to create test secret: %v", err)
				}
			}

			jd := &jdHPCDiscoverer{
				kubeClient: fakeClient,
			}

			ak, sk, err := jd.getCredentialsFromSecret(tc.secretName, "default")
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedAK, ak)
				assert.Equal(t, tc.expectedSK, sk)
			}
		})
	}
}

func TestJDHPCDiscoverer_NewJDHPCDiscoverer(t *testing.T) {
	tests := []struct {
		name        string
		config      api.DiscoveryConfig
		expectError bool
	}{
		{
			name: "ValidConfig",
			config: api.DiscoveryConfig{
				Source: "jdhpc",
				Config: map[string]interface{}{
					"endpoint": "https://test.example.com",
					"regionId": "cn-north-1",
				},
				Credentials: &api.Credentials{
					SecretRef: &api.SecretRef{
						Name:      "test-secret",
						Namespace: "default",
					},
				},
			},
			expectError: false,
		},
		{
			name: "NilConfig",
			config: api.DiscoveryConfig{
				Source: "jdhpc",
				Config: map[string]interface{}{},
			},
			expectError: false, // Should handle nil config gracefully
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset()

			// Create test secret
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-secret",
					Namespace: "default",
				},
				Data: map[string][]byte{
					"accessKey":       []byte("test-ak"),
					"secretAccessKey": []byte("test-sk"),
				},
			}
			_, err := fakeClient.CoreV1().Secrets("default").Create(
				context.TODO(), secret, metav1.CreateOptions{},
			)
			if err != nil {
				t.Fatalf("Failed to create test secret: %v", err)
			}

			discoverer := NewJDHPCDiscoverer(tc.config, fakeClient, nil)
			assert.NotNil(t, discoverer)
			assert.Equal(t, "jdhpc", discoverer.Name())
		})
	}
}
