/*
Auth providor using Google+ and secure session cookies with gorilla

Setup:
At https://console.developers.google.com/ , set up a project with Google API access and get your credentials
Create an Auth object with Auth.New()

Register Auth.LoginHandler and Auth.LogoutHandler at the URLs you specified

Use Auth.AdminMiddleware and Auth.UserMiddleware on areas of the site that require each level of protection
Inside handlers on that area of the site, use Auth.User to grab the login information, or Auth.IsAdmin to test for admin powers

*/

package auth

import (
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/sessions"

	"code.google.com/p/goauth2/oauth"
)

// Create a new Auth object
func New(admin_emails []string,
	googleClientId string,
	googleClientSecret string,
	cookieSecret string,
	loginURL string,
	fullLoginURL string,
	logoutURL string) Auth {

	cfg := &GoogleAuth{}
	cfg.AdminEmails = make(map[string]bool)
	for _, a := range admin_emails {
		cfg.AdminEmails[strings.ToLower(a)] = true
	}

	if cookieSecret == "" || len(cookieSecret) != 32 {
		log.Fatalln("Missing/broken cookie secret! It must be length 32")
	}

	cfg.GoogleClientID = googleClientId
	cfg.GoogleClientSecret = googleClientSecret
	cfg.CookieSecret = cookieSecret
	cfg.LoginUrl = loginURL
	cfg.LogoutUrl = logoutURL

	cfg.OauthConfig = &oauth.Config{
		ClientId:     googleClientId,
		ClientSecret: googleClientSecret,
		RedirectURL:  fullLoginURL,
		Scope:        "https://www.googleapis.com/auth/plus.profile.emails.read",
		AuthURL:      "https://accounts.google.com/o/oauth2/auth",
		TokenURL:     "https://accounts.google.com/o/oauth2/token",
	}

	cfg.CookieStore = sessions.NewCookieStore([]byte(cookieSecret))

	cfg.LoginTTL = 3600 * 24 * 7 // 7 days
	cfg.ApiUrl = "https://www.googleapis.com/plus/v1/people/me"

	return cfg
}

// Use New() to create this so you get the tasty defaults
type GoogleAuth struct {
	AdminEmails        map[string]bool // admin emails -> true
	GoogleClientID     string          // From the google api dashboard
	GoogleClientSecret string          // From the google api dashboard
	CookieSecret       string          // server-specific secret for signing cookies
	LoginUrl           string
	LogoutUrl          string
	LoginTTL           int64 // seconds
	ApiUrl             string
	CookieStore        *sessions.CookieStore
	OauthConfig        *oauth.Config
}

// Require a user login
// Always use context.ClearHandler as the base middleware or you'll leak memory (unless you're using gorilla as your server)
func (a *GoogleAuth) UserMiddleware(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		user := a.RequireUser(w, r)
		if user == nil {
			return
		}

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// Require an admin login
// Always use context.ClearHandler as the base middleware or you'll leak memory (unless you're using gorilla as your server)
func (a *GoogleAuth) AdminMiddleware(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		user := a.RequireUser(w, r)
		if user == nil {
			return
		}

		if !a.RequireAdmin(w, r) {
			return
		}

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// Fetch the login information, or nil if you're not above an auth middleware
// If you're not using the middlewares, you probably want RequireLogin instead
func (a *GoogleAuth) User(r *http.Request) *AuthUser {
	session, _ := a.CookieStore.Get(r, "auth-session")

	tsIface, present := session.Values["login-time"]
	ts, typeOk := tsIface.(int64)
	if !(present && typeOk && ts+a.LoginTTL > time.Now().Unix()) {
		return nil
	}

	return &AuthUser{
		Email: session.Values["login-email"].(string),
		Name:  session.Values["login-name"].(string),
	}
}

// Return true if this is an admin, false if they're logged out or not on the admin list
// See also: RequireAdmin
func (a *GoogleAuth) IsAdmin(r *http.Request) bool {
	user := a.User(r)
	if user == nil {
		return false
	}
	return a.AdminEmails[user.Email]
}

//If logged in, return email and name. Redirect to login handler if not logged in and return "", ""
//On email == "", most callers will want to return immediately
//See also the UserMiddleware, which is a friendlier way to use this
func (a *GoogleAuth) RequireUser(w http.ResponseWriter, r *http.Request) *AuthUser {
	user := a.User(r)
	if user == nil {
		http.Redirect(w, r, a.LoginUrl+"?redirect_to="+r.RequestURI, http.StatusFound)
		return nil
	}
	return user
}

// Return true if admin. If false, the caller should return immediately.
// If not logged in, forces log-in. If not admin, redirects to the not authorized page.
// See AdminMiddleWare, which is a friendlier way to use this
func (a *GoogleAuth) RequireAdmin(w http.ResponseWriter, r *http.Request) (isAdmin bool) {
	user := a.RequireUser(w, r)
	if user == nil {
		return false
	}
	isAdmin = a.AdminEmails[user.Email]
	if !isAdmin {
		http.Error(w, "Nope", http.StatusUnauthorized)
	}
	return
}
