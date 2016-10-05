package auth_test

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"testing"

	"github.com/gorilla/context"
	"github.com/twitchscience/blueprint/auth"
	"github.com/zenazn/goji"
	"github.com/zenazn/goji/web"
)

var (
	integration   bool
	cookieSecret  string
	clientID      string
	clientSecret  string
	githubServer  string
	requiredOrg   string
	authenticator auth.Auth
)

func homeHandler(w http.ResponseWriter, r *http.Request) {
	username := "(not logged in)"
	user := authenticator.User(r)

	if user != nil {
		username = user.Name
	}

	fmt.Fprintf(w, `<html><body>
		<ul>
		<li>DisplayName: %s</li>
		<li><a href="/login">Login</a></li>
		<li><a href="/logout">Logout</a></li>
		<li><a href="/user/greet">User Greeting</a></li>
		</ul>
		</body></html>`, username)
}

func loggedInHandler(w http.ResponseWriter, r *http.Request) {
	user := authenticator.User(r)
	fmt.Fprintf(w, "Hello, %s", user.Name)
}

func init() {
	flag.BoolVar(&integration, "serve", false, "Run server integration test")
	flag.StringVar(&cookieSecret, "cookieSecret", "", "32 character secret for signing cookies")
	flag.StringVar(&clientID, "clientID", "", "Google API client id")
	flag.StringVar(&clientSecret, "clientSecret", "", "Google API client secret")
	flag.StringVar(&githubServer, "githubServer", "http://github.com", "Github server to use for auth")
	flag.StringVar(&requiredOrg, "requiredOrg", "", "Org user need to belong to to use auth")
	flag.Parse()
}

func TestIntegrationGoogle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skip integration tests in short mode")
	}

	if !integration {
		t.Skip("Skip integration test when serve flag (-serve) isn't set")
	}

	if clientID == "" || clientSecret == "" || githubServer == "" {
		t.Skip("Auth creds and github server not set, see README.md for details")
	}

	if cookieSecret == "" {
		log.Println("Set cookie_secret to 32 random chars")
		log.Println(`python -c "import string,random;print ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(32))"`)
		t.Skip("Skipping until you set cookie secret")
	}

	authenticator = auth.New(githubServer,
		clientID,
		clientSecret,
		cookieSecret,
		requiredOrg,
		"/login")

	goji.Get("/login", authenticator.LoginHandler)
	goji.Get("/logout", authenticator.LogoutHandler)
	goji.Get("/github_oauth_cb", authenticator.AuthCallbackHandler)
	goji.Get("/", homeHandler)

	loggedIn := web.New()
	goji.Handle("/user/*", loggedIn)
	loggedIn.Use(authenticator.AuthorizeOrForbid)
	loggedIn.Get("/user/greet", loggedInHandler)

	goji.Use(context.ClearHandler) // THIS IS IMPORTANT - Prevent memory leaks

	log.Println("Use -bind if you're listening on the wrong port")
	log.Println("Ctrl+C to finish the test")

	goji.Serve()
}
