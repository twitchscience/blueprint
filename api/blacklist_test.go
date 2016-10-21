package api

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestBlacklist(t *testing.T) {
	var jsonFile *os.File
	jsonFile, err := ioutil.TempFile("./", "testJson")
	s := &server{"", nil, jsonFile.Name(), nil}
	if err != nil {
		t.Errorf("%v", err)
	}
	defer func() {
		err = os.Remove(jsonFile.Name())
		if err != nil {
			t.Errorf("%v", err)
		}
	}()
	s.configFilename = jsonFile.Name()
	_, err = jsonFile.WriteString(`{ "blacklist": ["^wow$", "^logs\\.dfp_.*$", "^logs\\.a.c_.*$"] }`)
	if err != nil {
		t.Errorf("%v", err)
	}
	err = jsonFile.Close()
	if err != nil {
		t.Errorf("%v", err)
	}

	var tests = []struct {
		input string
		want  bool
	}{
		{"", false},
		{"wow", true},
		{"wow_", false},

		{"logsadfp", false},
		{"logsadfp_", false},
		{"logsadfp_a", false},
		{"logsadfp_abc", false},

		{"logs.abc_", true},
		{"logs.aec", false},
		{"logs.ac", false},
		{"logs.a.c_", true},
		{"logs.a.c_wow", true},

		{"logs.dfp", false},
		{"logs.dfp_", true},
		{"logs.dfp_a", true},
		{"logs.dfp_abc", true},

		{"lOgs.dfp", false},
		{"Logs.dfp_", true},
		{"lOgs.dfp_a", true},
		{"loGs.dfp_abc", true},
	}

	for _, test := range tests {
		if got, err := s.isBlacklisted(test.input); got != test.want || err != nil {
			t.Errorf("blacklist(%v) = %v, err = %v", test.input, got, err)
		}
	}

}
