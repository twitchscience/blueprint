package scoop_protocol

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"time"

	"github.com/twitchscience/scoop_protocol/msg_signer"
)

type ColumnDefinition struct {
	InboundName           string
	OutboundName          string
	Transformer           string // this should match one of the types in redshift types
	ColumnCreationOptions string
	SupportingColumns     string // comma separated list used by mapping transformers
}

type Config struct {
	EventName string
	Columns   []ColumnDefinition
	Version   int
}

type Action string

const (
	ADD                Action = "add"
	DELETE             Action = "delete"
	RENAME             Action = "rename"
	REQUEST_DROP_EVENT Action = "request_drop_event" // mark a table for manual deletion
	DROP_EVENT         Action = "drop_event"         // table has been dropped
	CANCEL_DROP_EVENT  Action = "cancel_drop_event"  // used to unmark the table for deletion
)

// Operation represents a single change to a schema
type Operation struct {
	Action         Action
	Name           string
	ActionMetadata map[string]string
}

type EventMetadataType string

const (
	COMMENT   EventMetadataType = "comment"
	EDGE_TYPE EventMetadataType = "edge_type"
)

func NewAddOperation(outbound, inbound, type_, options, columns string) Operation {
	return Operation{
		Action: ADD,
		Name:   outbound,
		ActionMetadata: map[string]string{
			"inbound":            inbound,
			"column_type":        type_,
			"column_options":     options,
			"supporting_columns": columns,
		},
	}
}

func NewDeleteOperation(outbound string) Operation {
	return Operation{
		Action:         DELETE,
		Name:           outbound,
		ActionMetadata: map[string]string{},
	}
}

func NewRenameOperation(current, new string) Operation {
	return Operation{
		Action: RENAME,
		Name:   current,
		ActionMetadata: map[string]string{
			"new_outbound": new,
		},
	}
}

func NewRequestDropEventOperation(reason string) Operation {
	return Operation{
		Action:         REQUEST_DROP_EVENT,
		Name:           "", // nil would be better, but most code can't handle it.
		ActionMetadata: map[string]string{"reason": reason},
	}
}

func NewDropEventOperation(reason string) Operation {
	return Operation{
		Action:         DROP_EVENT,
		Name:           "", // nil would be better, but most code can't handle it.
		ActionMetadata: map[string]string{"reason": reason},
	}
}

func NewCancelDropEventOperation(reason string) Operation {
	return Operation{
		Action:         CANCEL_DROP_EVENT,
		Name:           "", // nil would be better, but most code can't handle it.
		ActionMetadata: map[string]string{},
	}
}

type RowCopyRequest struct {
	KeyName      string
	TableName    string
	TableVersion int
}

type ManifestRowCopyRequest struct {
	ManifestURL string
	TableName   string
}

type LoadCheckRequest struct {
	ManifestURL string
}

type LoadCheckResponse struct {
	LoadStatus  LoadStatus
	ManifestURL string
}

type ScoopHealthCheck struct {
	RedshiftDBConnError *string
	IngesterDBConnError *string
}

type LoadStatus string

const (
	LoadNotFound   LoadStatus = "load-not-found"
	LoadFailed     LoadStatus = "load-failed"
	LoadInProgress LoadStatus = "load-in-progress"
	LoadComplete   LoadStatus = "load-complete"
)

type ScoopSigner interface {
	GetConfig(io.Reader) (*Config, error)
	GetRowCopyRequest(io.Reader) (*RowCopyRequest, error)
	SignJsonBody(interface{}) ([]byte, error)
	SignBody([]byte) ([]byte, error)
}

type FakeScoopSigner struct{}

type AuthScoopSigner struct {
	TimeSigner *msg_signer.TimeSigner
	Exp        time.Duration
}

type EventComment struct {
	EventName    string
	EventComment string
	UserName     string
}

var (
	BadVerified error = errors.New("Bad Signature")
)

// For now we are turning off the signer
func GetScoopSigner() ScoopSigner {
	return &FakeScoopSigner{}
}

func (s *AuthScoopSigner) SignJsonBody(o interface{}) ([]byte, error) {
	req, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	return s.SignBody(req)
}

func (s *AuthScoopSigner) SignBody(b []byte) ([]byte, error) {
	return s.TimeSigner.Sign(b), nil
}

func (s *AuthScoopSigner) GetConfig(body io.Reader) (*Config, error) {
	msg, verified := s.TimeSigner.PagedVerify(body, s.Exp)
	if !verified {
		return nil, BadVerified
	}

	c := new(Config)
	err := json.Unmarshal(msg, c)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (s *AuthScoopSigner) GetRowCopyRequest(body io.Reader) (*RowCopyRequest, error) {
	msg, verified := s.TimeSigner.PagedVerify(body, s.Exp)
	if !verified {
		return nil, BadVerified
	}

	c := new(RowCopyRequest)
	err := json.Unmarshal(msg, c)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (s *FakeScoopSigner) SignJsonBody(o interface{}) ([]byte, error) {
	req, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	return s.SignBody(req)
}

// does nothing
func (s *FakeScoopSigner) SignBody(b []byte) ([]byte, error) {
	return b, nil
}

func (s *FakeScoopSigner) GetConfig(b io.Reader) (*Config, error) {
	c := new(Config)
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(msg, c)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (s *FakeScoopSigner) GetRowCopyRequest(b io.Reader) (*RowCopyRequest, error) {
	c := new(RowCopyRequest)
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(msg, c)
	if err != nil {
		return nil, err
	}

	return c, nil
}
