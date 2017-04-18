package test

import (
	"sync"

	"github.com/twitchscience/blueprint/bpdb"
	"github.com/twitchscience/blueprint/core"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// MockBpdb is a mock for the bpdb/Bpdb interface which tracks whether the DB is in maintenance mode.
type MockBpdb struct {
	maintenanceMutex *sync.RWMutex

	maintenanceMode bool
}

// MockBpSchemaBackend is a mock for the bpdb/BpSchemaBackend interface which tracks how many times AllSchemas has been called
type MockBpSchemaBackend struct {
	allSchemasMutex *sync.RWMutex

	allSchemasCalls int32
}

// MockBpKinesisConfigBackend is a mock for the bpdb/BpKinesisConfigBackend interface
type MockBpKinesisConfigBackend struct {
}

// NewMockBpdb creates a new mock backend.
func NewMockBpdb() *MockBpdb {
	return &MockBpdb{&sync.RWMutex{}, false}
}

// NewMockBpSchemaBackend creates a new mock schema backend.
func NewMockBpSchemaBackend() *MockBpSchemaBackend {
	return &MockBpSchemaBackend{&sync.RWMutex{}, 0}
}

// NewMockBpKinesisConfigBackend creates a new mock kinesis config backend.
func NewMockBpKinesisConfigBackend() *MockBpKinesisConfigBackend {
	return &MockBpKinesisConfigBackend{}
}

// GetAllSchemasCalls returns the number of times AllSchemas() has been called.
func (m *MockBpSchemaBackend) GetAllSchemasCalls() int32 {
	m.allSchemasMutex.RLock()
	defer m.allSchemasMutex.RUnlock()
	return m.allSchemasCalls
}

// AllSchemas increments the number of AllSchemas calls and return nils.
func (m *MockBpSchemaBackend) AllSchemas() ([]bpdb.AnnotatedSchema, error) {
	m.allSchemasMutex.Lock()
	m.allSchemasCalls++
	m.allSchemasMutex.Unlock()
	return make([]bpdb.AnnotatedSchema, 0), nil
}

// Schema returns nils.
func (m *MockBpSchemaBackend) Schema(name string) (*bpdb.AnnotatedSchema, error) {
	return nil, nil
}

// UpdateSchema returns nil.
func (m *MockBpSchemaBackend) UpdateSchema(update *core.ClientUpdateSchemaRequest, user string) *core.WebError {
	return nil
}

// CreateSchema returns nil.
func (m *MockBpSchemaBackend) CreateSchema(schema *scoop_protocol.Config, user string) *core.WebError {
	return nil
}

// Migration returns nils.
func (m *MockBpSchemaBackend) Migration(table string, to int) ([]*scoop_protocol.Operation, error) {
	return nil, nil
}

// DropSchema return nil.
func (m *MockBpSchemaBackend) DropSchema(schema *bpdb.AnnotatedSchema, reason string, exists bool, user string) error {
	return nil
}

// AllKinesisConfigs returns nil
func (m *MockBpKinesisConfigBackend) AllKinesisConfigs() ([]bpdb.AnnotatedKinesisConfig, error) {
	return make([]bpdb.AnnotatedKinesisConfig, 0), nil
}

// KinesisConfig returns nil
func (m *MockBpKinesisConfigBackend) KinesisConfig(account int64, streamType string, name string) (*bpdb.AnnotatedKinesisConfig, error) {
	return nil, nil
}

// UpdateKinesisConfig returns nil
func (m *MockBpKinesisConfigBackend) UpdateKinesisConfig(update *bpdb.AnnotatedKinesisConfig, user string) *core.WebError {
	return nil
}

// CreateKinesisConfig returns nil
func (m *MockBpKinesisConfigBackend) CreateKinesisConfig(config *bpdb.AnnotatedKinesisConfig, user string) *core.WebError {
	return nil
}

// DropKinesisConfig returns nil
func (m *MockBpKinesisConfigBackend) DropKinesisConfig(config *bpdb.AnnotatedKinesisConfig, reason string, user string) error {
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

// ActiveUsersLast30Days returns nils.
func (m *MockBpdb) ActiveUsersLast30Days() ([]*bpdb.ActiveUser, error) {
	return nil, nil
}

// DailyChangesLast30Days returns nils.
func (m *MockBpdb) DailyChangesLast30Days() ([]*bpdb.DailyChange, error) {
	return nil, nil
}
