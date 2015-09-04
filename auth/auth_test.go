package auth_test

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"

	"github.com/gorilla/context"
	"github.com/twitchscience/blueprint/auth"
	"github.com/zenazn/goji"
	"github.com/zenazn/goji/web"
)

var (
	admins        string
	clientId      string
	clientSecret  string
	cookieSecret  string
	integration   bool
	fullLoginUrl  string
	authenticator auth.Auth
)

func homeHandler(w http.ResponseWriter, r *http.Request) {
	username, email := "(not logged in)", "(not logged in)"
	user := authenticator.User(r)

	if user != nil {
		username, email = user.Name, user.Email
	}

	fmt.Fprintf(w, `<html><body>
		<ul>
		<li>DisplayName: %s</li>
		<li>Email: %s</li>
		<li><a href="/login">Login</a></li>
		<li><a href="/logout">Logout</a></li>
		<li><a href="/user/greet">User Greeting</a></li>
		<li><a href="/admin/secret">Admin Page</a></li>
		</ul>
		</body></html>`, username, email)
}

func loggedInHandler(w http.ResponseWriter, r *http.Request) {
	user := authenticator.User(r)
	fmt.Fprintf(w, "Hello, %s", user.Name)
}

func adminHandler(w http.ResponseWriter, r *http.Request) {
	user := authenticator.User(r)
	fmt.Fprintf(w, "All the secret data, %s", user.Name)
}

func init() {
	flag.BoolVar(&integration, "serve", false, "Run server integration test")
	flag.StringVar(&admins, "admins", "", "Comma separated list of admin emails")
	flag.StringVar(&clientId, "client_id", "", "Google API client ID")
	flag.StringVar(&clientSecret, "client_secret", "", "Google API client secret")
	flag.StringVar(&cookieSecret, "cookie_secret", "", "Cookie secret key")
	flag.StringVar(&fullLoginUrl, "full_login_url", "", "Fully qualified login url (for google redirect)")
	flag.Parse()
}

func TestIntegrationGoogle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skip integration tests in short mode")
	}

	if !integration {
		t.Skip("Skip integration test when serve flag (-serve) isn't set")
	}

	if clientId == "" || clientSecret == "" || fullLoginUrl == "" {
		log.Println("Set client_id, client_secret, and full_login_url (redirect url) to match your credentials")
		log.Println("from https://console.developers.google.com/project")
		t.Skip("Skipping until you set your redirect and client creds")
	}

	if cookieSecret == "" {
		log.Println("Set cookie_secret to 32 random chars")
		log.Println(`python -c "import string,random;print ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(32))"`)
		t.Skip("Skipping until you set cookie secret")
	}

	authenticator = auth.New(strings.Split(admins, ","), clientId, clientSecret, cookieSecret, "/login", fullLoginUrl, "/logout")

	goji.Get("/login", authenticator.LoginHandler)
	goji.Get("/logout", authenticator.LogoutHandler)
	goji.Get("/", homeHandler)

	admin := web.New()
	goji.Handle("/admin/*", admin)
	admin.Use(authenticator.AdminMiddleware)
	admin.Get("/admin/secret", adminHandler)

	loggedIn := web.New()
	goji.Handle("/user/*", loggedIn)
	loggedIn.Use(authenticator.UserMiddleware)
	loggedIn.Get("/user/greet", loggedInHandler)

	goji.Use(context.ClearHandler) // THIS IS IMPORTANT - Prevent memory leaks

	log.Println("Go to your server -- probably at", fullLoginUrl)
	log.Println("Use -bind if you're listening on the wrong port")
	log.Println("Ctrl+C to finish the test")

	goji.Serve()
}
