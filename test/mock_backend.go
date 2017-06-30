package test

import (
	"errors"
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

// MockBpEventMetadataBackend is a mock for the bpdb/BpEventMetadataBackend interface
type MockBpEventMetadataBackend struct {
	returnMap             map[string]bpdb.EventMetadata
	allEventMetadataMutex *sync.RWMutex
	allEventMetadataCalls int32
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

// NewMockBpEventMetadataBackend creates a mock event metadata backend.
func NewMockBpEventMetadataBackend(returnMap map[string]bpdb.EventMetadata) *MockBpEventMetadataBackend {
	return &MockBpEventMetadataBackend{returnMap, &sync.RWMutex{}, 0}
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

// Schema returns nils except when the event name is "this-table-exists" or "this-event-exists"
func (m *MockBpSchemaBackend) Schema(name string) (*bpdb.AnnotatedSchema, error) {
	if name == "this-table-exists" || name == "this-event-exists" {
		return &bpdb.AnnotatedSchema{}, nil
	}
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

// AllEventMetadata increments the number of AllEventMetadata calls
func (m *MockBpEventMetadataBackend) AllEventMetadata() (*bpdb.AllEventMetadata, error) {
	m.allEventMetadataMutex.Lock()
	m.allEventMetadataCalls++
	m.allEventMetadataMutex.Unlock()
	if eventMetadata, exists := m.returnMap["this-table-exists"]; exists {
		metadata := map[string]map[string]bpdb.EventMetadataRow{"this-table-exists": eventMetadata.Metadata}
		return &bpdb.AllEventMetadata{Metadata: metadata}, nil
	}
	// ret := make(map[string]map[string]bpdb.EventMetadataRow)
	// ret["rare-event"] = make(map[string]bpdb.EventMetadataRow)
	// ret["rare-event"]["comment"] = bpdb.EventMetadataRow{
	// 	MetadataValue: "Test comment",
	// }
	return &bpdb.AllEventMetadata{}, nil
}

// GetAllEventMetadataCalls returns the number of times EventMetadata() has been called.
func (m *MockBpEventMetadataBackend) GetAllEventMetadataCalls() int32 {
	m.allEventMetadataMutex.RLock()
	defer m.allEventMetadataMutex.RUnlock()
	return m.allEventMetadataCalls
}

// // EventMetadata returns nil except when update.EventName is in the returnMap
// func (m *MockBpEventMetadataBackend) EventMetadata(name string) (*bpdb.EventMetadata, error) {
// 	if eventMetadata, exists := m.returnMap[name]; exists {
// 		m.allEventMetadataMutex.Lock()
// 		m.allEventMetadataCalls++
// 		m.allEventMetadataMutex.Unlock()
// 		return &eventMetadata, nil
// 	}
// 	return nil, fmt.Errorf("no metadata found for event %s", name)
// }

// UpdateEventMetadata returns nil if update.EventName is in the returnMap
func (m *MockBpEventMetadataBackend) UpdateEventMetadata(update *core.ClientUpdateEventMetadataRequest, user string) *core.WebError {
	if _, exists := m.returnMap[update.EventName]; exists {
		return nil
	}
	return core.NewUserWebError(errors.New("schema does not exist"))
}

// AllKinesisConfigs returns nil
func (m *MockBpKinesisConfigBackend) AllKinesisConfigs() ([]scoop_protocol.AnnotatedKinesisConfig, error) {
	return make([]scoop_protocol.AnnotatedKinesisConfig, 0), nil
}

// KinesisConfig returns nil
func (m *MockBpKinesisConfigBackend) KinesisConfig(account int64, streamType string, name string) (*scoop_protocol.AnnotatedKinesisConfig, error) {
	return nil, nil
}

// UpdateKinesisConfig returns nil
func (m *MockBpKinesisConfigBackend) UpdateKinesisConfig(update *scoop_protocol.AnnotatedKinesisConfig, user string) *core.WebError {
	return nil
}

// CreateKinesisConfig returns nil
func (m *MockBpKinesisConfigBackend) CreateKinesisConfig(config *scoop_protocol.AnnotatedKinesisConfig, user string) *core.WebError {
	return nil
}

// DropKinesisConfig returns nil
func (m *MockBpKinesisConfigBackend) DropKinesisConfig(config *scoop_protocol.AnnotatedKinesisConfig, reason string, user string) error {
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
