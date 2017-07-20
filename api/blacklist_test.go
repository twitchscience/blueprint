package api

import "testing"

func TestBlacklist(t *testing.T) {
	jsonFile := createJSONFile(t, "testBlacklist")
	defer deleteJSONFile(t, jsonFile)
	writeConfig(t, jsonFile)

	s := New("", nil, nil, nil, nil, jsonFile.Name(), nil, "", false, NewMockS3Uploader()).(*server)

	var tests = []struct {
		input string
		want  bool
	}{
		{"", false},
		{"wow", true},
		{"wow_", false},

		{"abc_", true},
		{"aec", false},
		{"ac", false},
		{"a.c_", true},
		{"a.c_wow", true},

		{"dfp", false},
		{"dfp_", true},
		{"dfp_a", true},
		{"dfp_abc", true},
	}

	for _, test := range tests {
		if got := s.isBlacklisted(test.input); got != test.want {
			t.Errorf("blacklist(%v) = %v", test.input, got)
		}
	}
}
