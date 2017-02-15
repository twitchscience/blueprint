package test

import (
	"sync"

	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// MockBpdb is a mock for the bpdb/Bpdb interface which tracks how many times AllSchemas has been
// called and whether the DB is in maintenance mode.
type MockBpdb struct {
	allSchemasMutex  *sync.RWMutex
	maintenanceMutex *sync.RWMutex

	allSchemasCalls int32
	maintenanceMode bool
}

// NewMockBpdb creates a new mock backend.
func NewMockBpdb() *MockBpdb {
	return &MockBpdb{&sync.RWMutex{}, &sync.RWMutex{}, 0, false}
}

// GetAllSchemasCalls returns the number of times AllSchemas() has been called.
func (m *MockBpdb) GetAllSchemasCalls() int32 {
	m.allSchemasMutex.RLock()
	defer m.allSchemasMutex.RUnlock()
	return m.allSchemasCalls
}

// AllSchemas increments the number of AllSchemas calls and return nils.
func (m *MockBpdb) AllSchemas() ([]bpdb.AnnotatedSchema, error) {
	m.allSchemasMutex.Lock()
	m.allSchemasCalls++
	m.allSchemasMutex.Unlock()
	return make([]bpdb.AnnotatedSchema, 0), nil
}

// Schema returns nils.
func (m *MockBpdb) Schema(name string) (*bpdb.AnnotatedSchema, error) {
	return nil, nil
}

// UpdateSchema returns nil.
func (m *MockBpdb) UpdateSchema(update *core.ClientUpdateSchemaRequest, user string) (string, error) {
	return "", nil
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

// IsInMaintenanceMode returns current value (starts as false, can be set by SetMaintenanceMode).
func (m *MockBpdb) IsInMaintenanceMode() bool {
	m.maintenanceMutex.RLock()
	defer m.maintenanceMutex.RUnlock()
	return m.maintenanceMode
}

// SetMaintenanceMode sets the maintenance mode in memory and returns nil.
func (m *MockBpdb) SetMaintenanceMode(switchingOn bool, reason string) error {
	m.maintenanceMutex.Lock()
	m.maintenanceMode = switchingOn
	m.maintenanceMutex.Unlock()
	return nil
}
