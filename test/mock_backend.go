package test

import (
	"sync"

	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// MockBpdb is a mock for the bpdb/Bpdb interface which tracks how many times AllSchemas has been
// called.
type MockBpdb struct {
	allSchemasCalls int32
	mutex           *sync.RWMutex
}

// NewMockBpdb creates a new mock backend.
func NewMockBpdb() *MockBpdb {
	return &MockBpdb{0, &sync.RWMutex{}}
}

// GetAllSchemasCalls returns the number of times AllSchemas() has been called.
func (m *MockBpdb) GetAllSchemasCalls() int32 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.allSchemasCalls
}

// AllSchemas increments the number of AllSchemas calls and return nils.
func (m *MockBpdb) AllSchemas() ([]bpdb.AnnotatedSchema, error) {
	m.mutex.Lock()
	m.allSchemasCalls++
	m.mutex.Unlock()
	return make([]bpdb.AnnotatedSchema, 0), nil
}

// Schema returns nils.
func (m *MockBpdb) Schema(name string) (*bpdb.AnnotatedSchema, error) {
	return nil, nil
}

// UpdateSchema returns nil.
func (m *MockBpdb) UpdateSchema(update *core.ClientUpdateSchemaRequest, user string) error {
	return nil
}

// CreateSchema returns nil.
func (m *MockBpdb) CreateSchema(schema *scoop_protocol.Config, user string) error {
	return nil
}

// Migration returns nils.
func (m *MockBpdb) Migration(table string, to int) ([]*scoop_protocol.Operation, error) {
	return nil, nil
}

// DropSchema return nil.
func (m *MockBpdb) DropSchema(schema *bpdb.AnnotatedSchema, reason string, exists bool, user string) error {
	return nil
}
