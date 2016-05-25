package scoop_protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"strings"
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

type RowCopyRequest struct {
	KeyName   string
	TableName string
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
	BadVerified        error = errors.New("Bad Signature")
	transformerTypeMap       = map[string]string{
		"ipCity":       "varchar(64)",
		"ipCountry":    "varchar(2)",
		"ipRegion":     "varchar(64)",
		"ipAsn":        "varchar(128)",
		"ipAsnInteger": "int",
		"f@timestamp":  "datetime",
	}
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

func (c *Config) GetColumnCreationString() string {
	out := bytes.NewBuffer(make([]byte, 0, 256))
	out.WriteRune('(')
	for i, col := range c.Columns {
		out.WriteString(col.GetCreationForm())
		if i+1 != len(c.Columns) {
			out.WriteRune(',')
		}
	}
	out.WriteRune(')')
	return out.String()
}

func (col *ColumnDefinition) GetCreationForm() string {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	buf.WriteString(col.OutboundName)
	buf.WriteString(" ")
	if translatedType, ok := transformerTypeMap[col.Transformer]; ok {
		buf.WriteString(translatedType)
	} else if len(col.Transformer) > 0 && col.Transformer[0] == 'f' && col.Transformer[1] == '@' {
		// Its a function transformer
		canonicalName := col.Transformer[:strings.LastIndex(col.Transformer, "@")]
		if translatedType, ok := transformerTypeMap[canonicalName]; ok {
			buf.WriteString(translatedType)
		} else {
			buf.WriteString(col.Transformer)
		}
	} else {
		buf.WriteString(col.Transformer)
	}
	if len(col.ColumnCreationOptions) > 1 {
		buf.WriteString(col.ColumnCreationOptions)
	}
	return buf.String()
}
