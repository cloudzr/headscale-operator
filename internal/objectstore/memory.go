/*
Copyright 2025.

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

package objectstore

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

// Memory is an in-memory Store for tests. The zero value is ready to use.
type Memory struct {
	mu      sync.Mutex
	objects map[string]memObject
	// Now returns the timestamp recorded on Put. Defaults to time.Now.
	Now func() time.Time
	// Err, when set, is returned by every operation (simulates an outage).
	Err error
	// Puts counts successful Put calls per key.
	Puts map[string]int
}

type memObject struct {
	data     []byte
	modified time.Time
}

// SetObject stores data under key with an explicit modification time,
// bypassing Puts accounting. Useful to seed archives in tests.
func (m *Memory) SetObject(key string, data []byte, modified time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.objects == nil {
		m.objects = map[string]memObject{}
	}
	m.objects[key] = memObject{data: slices.Clone(data), modified: modified}
}

func (m *Memory) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	if m.objects == nil {
		m.objects = map[string]memObject{}
	}
	if m.Puts == nil {
		m.Puts = map[string]int{}
	}
	now := time.Now
	if m.Now != nil {
		now = m.Now
	}
	m.objects[key] = memObject{data: slices.Clone(data), modified: now()}
	m.Puts[key]++
	return nil
}

func (m *Memory) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return nil, m.Err
	}
	obj, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("%s: %w", key, ErrNotFound)
	}
	return slices.Clone(obj.data), nil
}

func (m *Memory) List(_ context.Context, prefix string) ([]Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return nil, m.Err
	}
	var out []Object
	for key, obj := range m.objects {
		if strings.HasPrefix(key, prefix) {
			out = append(out, Object{Key: key, Size: int64(len(obj.data)), LastModified: obj.modified})
		}
	}
	slices.SortFunc(out, func(a, b Object) int { return strings.Compare(a.Key, b.Key) })
	return out, nil
}
