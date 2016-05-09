/*
	Authorization middleware using github OAuth, with support for using github enterprise.
*/

package auth

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/sessions"
	"golang.org/x/oauth2"
)

// New creates and returns a github auth object
func New(githubServer string,
	clientID string,
	clientSecret string,
	cookieSecret string,
	requiredOrg string,
	loginURL string) Auth {

	if clientID == "" || clientSecret == "" {
		log.Fatalln("Authentication ClientId and ClientSecret missing")
	}

	if cookieSecret == "" || len(cookieSecret) != 32 {
		log.Fatalln("Missing/broken cookie secret! It must be length 32")
	}

	cfg := &GithubAuth{
		RequiredOrg:  requiredOrg,
		LoginURL:     loginURL,
		CookieStore:  sessions.NewCookieStore([]byte(cookieSecret)),
		GithubServer: githubServer,
		LoginTTL:     3600 * 24 * 7, // 7 days
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

	return cfg
}

// GithubAuth is an object managing the auth flow with github
type GithubAuth struct {
	RequiredOrg  string // If empty, membership will not be tested
	LoginURL     string
	GithubServer string
	LoginTTL     int64 // seconds
	CookieStore  *sessions.CookieStore
	OauthConfig  *oauth2.Config
}

// AuthorizeOrRedirect requires that the user be logged in and have proper permissions, else sends
// them to the login with a redirect.
func (a *GithubAuth) AuthorizeOrRedirect(h http.Handler) http.Handler {
	// Always use context.ClearHandler as the base middleware or you'll leak memory (unless you're using gorilla as your server)
	fn := func(w http.ResponseWriter, r *http.Request) {
		user := a.User(r)
		if user == nil {
			http.Redirect(w, r, a.LoginURL+"?redirect_to="+r.RequestURI, http.StatusFound)
			return
		}
		if user.IsMemberOfOrg == false {
			//return "access forbidden"" error in HttpResponse
			// do not redirect to loginURL, which will get into an endless loop
			logMsg := fmt.Sprintf("User %s is not a member of %s organization",
				user.Name, a.RequiredOrg)
			log.Println(logMsg)
			errMsg := fmt.Sprintf("You need to be a member of %s organization",
				a.RequiredOrg)
			http.Error(w, errMsg, http.StatusForbidden)
			return
		}

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// AuthorizeOrForbid requires the user be logged in and have proper permissions,
// else 403s
func (a *GithubAuth) AuthorizeOrForbid(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		user := a.User(r)
		if user == nil || user.IsMemberOfOrg == false {
			http.Error(w, "Please authenticate", http.StatusForbidden)
			return
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
		log.Println("No login-time value in cookie")
		return nil
	}

	if loginTime.(int64)+a.LoginTTL < time.Now().Unix() {
		log.Println("Login expired")
		return nil
	}

	tokenJSON, present := session.Values["auth-token"]
	if !present {
		log.Println("No token value in cookie")
		return nil
	}

	var token oauth2.Token
	err := json.Unmarshal(tokenJSON.([]byte), &token)

	if err != nil {
		log.Printf("Failed to unmarshal token: %v", err)
		return nil
	}

	isMember := true
	if a.RequiredOrg != "" {
		client := a.OauthConfig.Client(oauth2.NoContext, &token)

		checkMembershipURL := fmt.Sprintf("%s/api/v3/orgs/%s/members/%s",
			a.GithubServer, a.RequiredOrg, session.Values["login-name"])

		resp, err := client.Get(checkMembershipURL)
		if err != nil {
			log.Printf("Failed to get membership: %v", err)
			return nil
		}
		defer func() {
			err = resp.Body.Close()
			if err != nil {
				log.Printf("Error closing response body: %v.", err)
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
		return nil
	}
	return user
}
