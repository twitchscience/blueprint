package auth

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zenazn/goji/web"

	"golang.org/x/oauth2"
)

const cookieSecret = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"

func TestGithubAuthorize(t *testing.T) {
	recorder := httptest.NewRecorder()
	request, err := http.NewRequest("GET", "http://example.com", nil)
	if err != nil {
		t.Fatalf("Error on creating request: %v", err)
	}

	authHandler := stubbedGithubAuth()
	stub := authHandler.networkManager.(*stubNetworkManager)
	stub.orgMemberResponse = &http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(bytes.NewBufferString("valid response")),
	}
	stub.adminResponse = &http.Response{
		StatusCode: 400,
		Body:       ioutil.NopCloser(bytes.NewBufferString("")),
	}

	cookieStore := authHandler.CookieStore
	session, err := cookieStore.Get(request, cookieName)
	if err != nil {
		t.Fatalf("Unable to get session: %v", err)
	}
	session.Values["login-time"] = time.Now().Unix()
	session.Values["login-name"] = "unknown_user"
	session.Values["auth-token"], err = json.Marshal(oauth2.Token{})
	if err != nil {
		t.Fatalf("Unable to marshal auth token: %v", err)
	}

	authHandler.AuthorizeOrForbid(
		&web.C{Env: make(map[interface{}]interface{})},
		http.HandlerFunc(happyHandler),
	).ServeHTTP(recorder, request)

	statusCode := recorder.Result().StatusCode
	if statusCode != 204 {
		t.Errorf("Expected No Content (204), got %v", statusCode)
	}
}

func stubbedGithubAuth() *GithubAuth {
	githubAuth := New("",
		"clientID",
		"clientSecret",
		cookieSecret,
		"requiredOrg",
		"adminTeam",
		"http://example.com/login").(*GithubAuth)

	githubAuth.networkManager = &stubNetworkManager{}

	return githubAuth
}

type stubNetworkManager struct {
	exchangeTokenResponse, userResponse, orgMemberResponse, adminResponse *http.Response
	exchangeTokenErr, userErr, orgMemberErr, adminErr                     error
}

func (s *stubNetworkManager) getExchangeTokenResponse(_, _ string) (*http.Response, error) {
	return s.exchangeTokenResponse, s.exchangeTokenErr
}

func (s *stubNetworkManager) getUser(_ *oauth2.Token) (*http.Response, error) {
	return s.userResponse, s.userErr
}

func (s *stubNetworkManager) getMembership(_ *oauth2.Token, fmtString, _, _ string) (*http.Response, error) {
	switch fmtString {
	case orgMemberTemplate:
		return s.orgMemberResponse, s.orgMemberErr
	case adminTemplate:
		return s.adminResponse, s.adminErr
	default:
		return nil, errors.New("do not recognize template")
	}
}

func (s *stubNetworkManager) oauthRedirectURL(_ string) string {
	return "http://example.org/"
}

func happyHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}
