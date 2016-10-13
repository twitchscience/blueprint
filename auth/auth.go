/*
	Authorization middleware using github OAuth, with support for using github enterprise.
*/

package auth

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/sessions"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/zenazn/goji/web"
	"golang.org/x/oauth2"
)

// DummyAuth creates a fake user.
func DummyAuth(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		c.Env["username"] = "unknown"
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// New creates and returns a github auth object
func New(githubServer string,
	clientID string,
	clientSecret string,
	cookieSecret string,
	requiredOrg string,
	loginURL string) Auth {

	fatalError := false
	if clientID == "" {
		logger.Error("Authentication ClientId missing")
		fatalError = true
	}
	if clientSecret == "" {
		logger.Error("Authentication ClientSecret missing")
		fatalError = true
	}
	if len(cookieSecret) != 32 {
		logger.WithField("cookie_secret", cookieSecret).
			Error("Missing or broken cookie secret, must be length 32")
		fatalError = true
	}
	if fatalError {
		logger.Fatal("Malformed auth input, exiting")
	}

	cookieStore := sessions.NewCookieStore([]byte(cookieSecret))
	cookieStore.Options = &sessions.Options{
		HttpOnly: true,
		Secure:   true,
	}
	return &GithubAuth{
		RequiredOrg:  requiredOrg,
		LoginURL:     loginURL,
		CookieStore:  cookieStore,
		GithubServer: githubServer,
		LoginTTL:     7 * 24 * time.Hour, // 1 week
		OauthConfig: &oauth2.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			Scopes:       []string{"read:org"},

			Endpoint: oauth2.Endpoint{
				AuthURL:  githubServer + "/login/oauth/authorize",
				TokenURL: githubServer + "/login/oauth/access_token",
			},
		},
	}
}

// GithubAuth is an object managing the auth flow with github
type GithubAuth struct {
	RequiredOrg  string // If empty, membership will not be tested
	LoginURL     string
	GithubServer string
	LoginTTL     time.Duration
	CookieStore  *sessions.CookieStore
	OauthConfig  *oauth2.Config
}

// AuthorizeOrForbid requires the user be logged in and have proper permissions,
// else 403s
func (a *GithubAuth) AuthorizeOrForbid(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		user := a.User(r)
		if user == nil || !user.IsMemberOfOrg {
			http.Error(w, "Please authenticate", http.StatusForbidden)
			http.SetCookie(w, &http.Cookie{Name: "displayName", MaxAge: 0})
			return
		}
		c.Env["username"] = user.Name

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// ExpireDisplayName expires the display name if the github auth is no longer valid.
func (a *GithubAuth) ExpireDisplayName(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		user := a.User(r)
		if user == nil || !user.IsMemberOfOrg {
			http.SetCookie(w, &http.Cookie{Name: "displayName", MaxAge: 0})
		}

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// User fetches the login information, or nil if you're not above an auth middleware
// If you're not using the middlewares, you probably want RequireLogin instead
func (a *GithubAuth) User(r *http.Request) *User {
	session, _ := a.CookieStore.Get(r, cookieName)

	loginTime, present := session.Values["login-time"]
	if !present {
		logger.Warn("No login-time value in cookie")
		return nil
	} else if time.Unix(loginTime.(int64), 0).Add(a.LoginTTL).Before(time.Now()) {
		logger.Warn("Login expired")
		return nil
	}

	tokenJSON, present := session.Values["auth-token"]
	if !present {
		logger.Warn("No token value in cookie")
		return nil
	}

	var token oauth2.Token
	err := json.Unmarshal(tokenJSON.([]byte), &token)

	if err != nil {
		logger.WithError(err).Warn("Failed to unmarshal token")
		return nil
	}

	isMember := true
	if a.RequiredOrg != "" {
		client := a.OauthConfig.Client(oauth2.NoContext, &token)

		checkMembershipURL := fmt.Sprintf("%s/api/v3/orgs/%s/members/%s",
			a.GithubServer, a.RequiredOrg, session.Values["login-name"])

		resp, err := client.Get(checkMembershipURL)
		if err != nil {
			logger.WithError(err).Warn("Failed to get membership")
			return nil
		}
		defer func() {
			err = resp.Body.Close()
			if err != nil {
				logger.WithError(err).Error("Failed to close response body")
			}
		}()

		isMember = resp.StatusCode >= 200 && resp.StatusCode <= 299
	}

	return &User{
		Name:          session.Values["login-name"].(string),
		IsMemberOfOrg: isMember,
	}
}

func (a *GithubAuth) requireUser(w http.ResponseWriter, r *http.Request) *User {
	user := a.User(r)
	if user == nil {
		http.Redirect(w, r, a.LoginURL+"?redirect_to="+r.RequestURI, http.StatusFound)
	}
	return user
}
