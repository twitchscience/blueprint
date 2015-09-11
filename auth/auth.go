/*
	Auth providor using github externprise and secure session cookies with gorilla

Setup:
	Register Auth.LoginHandler and Auth.LogoutHandler at the URLs you specified
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

// Create a new Auth object
func New(githubServer string,
	clientId string,
	clientSecret string,
	cookieSecret string,
	requiredOrg string,
	loginURL string) Auth {

	if cookieSecret == "" || len(cookieSecret) != 32 {
		log.Fatalln("Missing/broken cookie secret! It must be length 32")
	}

	cfg := &GithubAuth{
		RequiredOrg:  requiredOrg,
		CookieSecret: cookieSecret,
		LoginUrl:     loginURL,
		CookieStore:  sessions.NewCookieStore([]byte(cookieSecret)),
		GithubServer: githubServer,
		LoginTTL:     3600 * 24 * 7, // 7 days
		OauthConfig: &oauth2.Config{
			ClientID:     clientId,
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

// Use New() to create this so you get the tasty defaults
type GithubAuth struct {
	RequiredOrg  string
	CookieSecret string
	LoginUrl     string
	GithubServer string
	LoginTTL     int64 // seconds
	CookieStore  *sessions.CookieStore
	OauthConfig  *oauth2.Config
}

// Require a user login
// Always use context.ClearHandler as the base middleware or you'll leak memory (unless you're using gorilla as your server)
func (a *GithubAuth) UserMiddleware(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		user := a.RequireUser(w, r)
		if user == nil {
			return
		}

		if user.IsMemberOfOrg == false && a.RequiredOrg != "" {
			return
		}

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// Fetch the login information, or nil if you're not above an auth middleware
// If you're not using the middlewares, you probably want RequireLogin instead
func (a *GithubAuth) User(r *http.Request) *AuthUser {
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

	tokenJson, present := session.Values["auth-token"]
	if !present {
		log.Println("No token value in cookie")
		return nil
	}

	var token oauth2.Token
	err := json.Unmarshal(tokenJson.([]byte), &token)

	if err != nil {
		log.Printf("Failed to unmarshal token: %v", err)
		return nil
	}

	isMember := false
	if a.RequiredOrg != "" {
		client := a.OauthConfig.Client(oauth2.NoContext, &token)

		checkMembershipURL := fmt.Sprintf("%s/api/v3/orgs/%s/members/%s",
			a.GithubServer, a.RequiredOrg, session.Values["login-name"])

		resp, err := client.Get(checkMembershipURL)
		if err != nil {
			log.Printf("Failed to get membership: %v", err)
			return nil
		}

		isMember = resp.StatusCode >= 200 && resp.StatusCode <= 299
	}

	return &AuthUser{
		Name:          session.Values["login-name"].(string),
		IsMemberOfOrg: isMember,
	}
}

func (a *GithubAuth) RequireUser(w http.ResponseWriter, r *http.Request) *AuthUser {
	user := a.User(r)
	if user == nil {
		http.Redirect(w, r, a.LoginUrl+"?redirect_to="+r.RequestURI, http.StatusFound)
		return nil
	}
	return user
}
