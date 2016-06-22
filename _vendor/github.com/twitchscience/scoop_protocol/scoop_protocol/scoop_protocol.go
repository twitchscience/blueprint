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
}

type Config struct {
	EventName string
	Columns   []ColumnDefinition
	Version   int
}

type Action string

const (
	ADD Action = "add"
)

// Operation represents a single change to a schema
type Operation struct {
	Action        Action
	Inbound       string
	Outbound      string
	ColumnType    string
	ColumnOptions string
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
