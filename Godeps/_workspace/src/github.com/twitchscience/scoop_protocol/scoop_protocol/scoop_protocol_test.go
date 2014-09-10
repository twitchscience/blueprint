package scoop_protocol

import (
	"bytes"
	"fmt"
	"testing"
)

func TestConfig(t *testing.T) {
	s := GetScoopSigner()
	testConfig := Config{
		"test",
		[]ColumnDefinition{
			ColumnDefinition{"", "test1", "int", "options?"},
			ColumnDefinition{"", "test2", "int", "options?"},
			ColumnDefinition{"", "test3", "int", "options?"},
		},
	}
	b, erro := s.SignJsonBody(testConfig)
	if erro != nil {
		t.Log(erro)
		t.Fail()
	}
	c, err := s.GetConfig(bytes.NewReader(b))
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if fmt.Sprintf("%v", *c) != fmt.Sprintf("%v", testConfig) {
		t.Logf("Expected %v got %v\n", testConfig, c)
		t.Fail()
	}
}

func TestEmptyConfig(t *testing.T) {
	s := GetScoopSigner()
	testConfig := Config{
		"test",
		[]ColumnDefinition{
			ColumnDefinition{"", "test1", "int", "options?"},
			ColumnDefinition{"", "test2", "int", "options?"},
			ColumnDefinition{},
		},
	}
	b, erro := s.SignJsonBody(testConfig)
	if erro != nil {
		t.Log(erro)
		t.Fail()
	}
	c, err := s.GetConfig(bytes.NewReader(b))
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if fmt.Sprintf("%v", *c) != fmt.Sprintf("%v", testConfig) {
		t.Logf("Expected %v got %v\n", testConfig, c)
		t.Fail()
	}
}

func TestRowCopyRequest(t *testing.T) {
	s := GetScoopSigner()
	testConfig := RowCopyRequest{
		"key",
		"table",
	}
	b, erro := s.SignJsonBody(testConfig)
	if erro != nil {
		t.Log(erro)
		t.Fail()
	}
	c, err := s.GetRowCopyRequest(bytes.NewReader(b))
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if fmt.Sprintf("%v", *c) != fmt.Sprintf("%v", testConfig) {
		t.Logf("Expected %v got %v\n", testConfig, c)
		t.Fail()
	}
}

func TestColumnCreationString(t *testing.T) {
	testConfig := Config{
		"test",
		[]ColumnDefinition{
			ColumnDefinition{"", "test1", "int", "options?"},
			ColumnDefinition{"", "test2", "int", "options?"},
			ColumnDefinition{"", "test3", "ipCity", ""},
			ColumnDefinition{"", "test4", "ipRegion", ""},
			ColumnDefinition{"", "test5", "ipCountry", " sortkey"},
			ColumnDefinition{"", "test6", "f@timestamp@unix", ""},
			ColumnDefinition{"", "test7", "varchar", "(16)"},
		},
	}
	expected := `(test1 intoptions?,test2 intoptions?,test3 varchar(64),test4 varchar(64),test5 varchar(2) sortkey,test6 datetime,test7 varchar(16))`
	if expected != testConfig.GetColumnCreationString() {
		t.Logf("Expected %v got %v\n", expected, testConfig.GetColumnCreationString())
		t.Fail()
	}
}
