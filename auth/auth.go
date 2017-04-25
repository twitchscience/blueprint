/*
	Authorization middleware using github OAuth, with support for using github enterprise.
*/

package auth

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
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
	adminTeam string,
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
	cookieStore.Options.HttpOnly = true
	cookieStore.Options.Secure = true
	return &GithubAuth{
		RequiredOrg:  requiredOrg,
		AdminTeam:    adminTeam,
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
	AdminTeam    string
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
			clearCookies(w)
			return
		}
		c.Env["username"] = user.Name

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// AuthorizeOrForbidAdmin requires the user be logged in and a member of the admin team,
// else 403s.
func (a *GithubAuth) AuthorizeOrForbidAdmin(c *web.C, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := a.User(r)
		if user == nil {
			http.Error(w, "Please authenticate", http.StatusForbidden)
			clearCookies(w)
			return
		}

		if !user.IsAdmin {
			http.Error(w, "You do not have the necessary privileges", http.StatusForbidden)
			clearCookies(w)
			return
		}

		c.Env["username"] = user.Name
		h.ServeHTTP(w, r)
	})
}

// ExpireDisplayName expires the display name if the github auth is no longer valid.
func (a *GithubAuth) ExpireDisplayName(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		user := a.User(r)
		if user == nil || !user.IsMemberOfOrg {
			clearCookies(w)
		}

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func clearCookies(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{Name: "displayName", MaxAge: 0})
	http.SetCookie(w, &http.Cookie{Name: "isAdmin", MaxAge: 0})
}

func (a *GithubAuth) getGroupMembership(
	token *oauth2.Token,
	session *sessions.Session,
	fmtString string,
	groupName string,
	checkFn func(*http.Response) (bool, error)) (bool, error) {

	if groupName == "" {
		return true, nil
	}

	client := a.OauthConfig.Client(oauth2.NoContext, token)
	checkURL := fmt.Sprintf(fmtString, a.GithubServer, url.QueryEscape(groupName), session.Values["login-name"])
	resp, err := client.Get(checkURL)
	if err != nil {
		return false, fmt.Errorf("checking URL: %v", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WithError(cerr).Error("Failed to close response body")
		}
	}()

	return checkFn(resp)
}

func (a *GithubAuth) userIsOrgMember(token *oauth2.Token, session *sessions.Session) (bool, error) {
	return a.getGroupMembership(token, session, "%s/api/v3/orgs/%s/members/%s", a.RequiredOrg,
		func(resp *http.Response) (bool, error) {
			return resp.StatusCode >= 200 && resp.StatusCode <= 299, nil
		})
}

func (a *GithubAuth) userIsAdmin(token *oauth2.Token, session *sessions.Session) (bool, error) {
	return a.getGroupMembership(token, session, "%s/api/v3/teams/%s/memberships/%s", a.AdminTeam,
		func(resp *http.Response) (bool, error) {
			if resp.StatusCode != 200 {
				return false, nil
			}

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return false, fmt.Errorf("reading response body: %v", err)
			}

			var jsonResponse map[string]string
			if err = json.Unmarshal(body, &jsonResponse); err != nil {
				return false, fmt.Errorf("parsing response body: %v", err)
			}

			return jsonResponse["state"] == "active", nil
		})
}

// User fetches the login information, or nil if you're not above an auth middleware
// If you're not using the middlewares, you probably want RequireLogin instead
func (a *GithubAuth) User(r *http.Request) *User {
	session, _ := a.CookieStore.Get(r, cookieName)

	loginTime, present := session.Values["login-time"]
	if !present {
		logger.Warn("No login-time value in cookie")
		return nil
	}
	if time.Unix(loginTime.(int64), 0).Add(a.LoginTTL).Before(time.Now()) {
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

	isMember, err := a.userIsOrgMember(&token, session)
	if err != nil {
		logger.WithError(err).Warn("Failed to get membership")
		return nil
	}

	isAdmin, err := a.userIsAdmin(&token, session)
	if err != nil {
		logger.WithError(err).Warn("Failed to get admin status")
		return nil
	}

	return &User{
		Name:          session.Values["login-name"].(string),
		IsMemberOfOrg: isMember,
		IsAdmin:       isAdmin,
	}
}

func (a *GithubAuth) requireUser(w http.ResponseWriter, r *http.Request) *User {
	user := a.User(r)
	if user == nil {
		http.Redirect(w, r, a.LoginURL+"?redirect_to="+r.RequestURI, http.StatusFound)
	}
	return user
}
