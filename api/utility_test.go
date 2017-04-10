package api

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func createJSONFile(t *testing.T, prefix string) *os.File {
	jsonFile, err := ioutil.TempFile("./", prefix)
	if err != nil {
		t.Fatal(err)
	}
	return jsonFile
}

func deleteJSONFile(t *testing.T, jsonFile *os.File) {
	if err := os.Remove(jsonFile.Name()); err != nil {
		t.Error(err)
	}
}

func writeConfig(t *testing.T, jsonFile *os.File) {
	c := config{
		CacheTimeoutSecs: 60,
		Blacklist:        []string{"^wow$", "^dfp_.*$", "^a.c_.*$"},
	}
	writeConfigToJSON(t, c, jsonFile)
}

func writeConfigToJSON(t *testing.T, c config, jsonFile io.WriteCloser) {
	jsonEncoding, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jsonFile.Write(jsonEncoding); err != nil {
		t.Fatal(err)
	}
	if err := jsonFile.Close(); err != nil {
		t.Fatal(err)
	}
}
