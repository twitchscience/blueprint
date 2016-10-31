package api

import "testing"

func TestBlacklist(t *testing.T) {
	jsonFile := createJSONFile(t, "testBlacklist")
	defer deleteJSONFile(t, jsonFile)
	writeConfig(t, jsonFile)

	s := New("", nil, jsonFile.Name(), nil, "").(*server)

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
		if got := s.isBlacklisted(test.input); got != test.want {
			t.Errorf("blacklist(%v) = %v", test.input, got)
		}
	}
}
