package auth

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/oauth2"
)

const (
	cookieName = "github-auth"
)

func verifyOrReport(test bool, w http.ResponseWriter, message string) bool {
	if test == false {
		log.Println(message)
		http.Error(w, message, http.StatusInternalServerError)
		return false
	}
	return true
}

func (a *GithubAuth) AuthCallbackHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("AuthCallbackHandler")
	session, _ := a.CookieStore.Get(r, cookieName)

	r.ParseForm()

	expectedState := session.Values["auth-state"]
	if !verifyOrReport(expectedState != nil, w, fmt.Sprintf("No auth state variable found in cookie")) {
		return
	}

	recievedState := r.FormValue("state")
	if !verifyOrReport(expectedState == recievedState, w,
		fmt.Sprintf("Invalid oauth state! Expected '%v' got '%v'", expectedState, recievedState)) {
		return
	}

	resp, err := http.PostForm(a.OauthConfig.Endpoint.TokenURL, url.Values{
		"client_id":     {a.OauthConfig.ClientID},
		"client_secret": {a.OauthConfig.ClientSecret},
		"code":          {r.FormValue("code")},
		"state":         {recievedState}})

	if !verifyOrReport(err == nil, w, fmt.Sprintf("Error getting token, result = %s", err)) {
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if !verifyOrReport(err == nil, w, fmt.Sprintf("Cannot fetch OAuth token: %v\n", err)) {
		return
	}

	statusCode := resp.StatusCode
	if !verifyOrReport(statusCode >= 200 && statusCode <= 299, w,
		fmt.Sprintf("Cannot fetch OAuth token: %v\nResponse: %s", resp.Status, body)) {
		return
	}

	vals, err := url.ParseQuery(string(body))
	if !verifyOrReport(err == nil, w, fmt.Sprintf("Cannot parse OAuth response: %v", err)) {
		return
	}

	token := &oauth2.Token{
		AccessToken: vals.Get("access_token"),
		TokenType:   vals.Get("token_type"),
	}

	bytes, err := json.Marshal(token)
	if !verifyOrReport(err == nil, w, fmt.Sprintf("Error marshalling oauth token: %v", err)) {
		return
	}
	session.Values["auth-token"] = bytes

	client := a.OauthConfig.Client(oauth2.NoContext, token)
	resp, err = client.Get(a.GithubServer + "/api/v3/user")
	if !verifyOrReport(err == nil, w, fmt.Sprintf("Error getting user info: %v", err)) {
		return
	}

	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if !verifyOrReport(err == nil, w, fmt.Sprintf("Error reading response for user info: %v", err)) {
		return
	}

	var user map[string]interface{}
	err = json.Unmarshal(body, &user)
	if !verifyOrReport(err == nil, w, fmt.Sprintf("Error unmarshalling user info: %v", err)) {
		return
	}

	if !verifyOrReport(user["login"] != nil, w, "user login not found in user info") {
		return
	}

	session.Values["login-time"] = time.Now().Unix()
	session.Values["login-name"] = user["login"]

	redirectTarget := session.Values["auth-redirect-to"].(string)
	delete(session.Values, "auth-redirect-to")
	delete(session.Values, "auth-state")
	session.Save(r, w)

	http.Redirect(w, r, "/"+redirectTarget, http.StatusFound)
}

func (a *GithubAuth) LoginHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("LoginHandler")

	// Generate random string to protect the user from CSRF attacks.
	// See http://tools.ietf.org/html/rfc6749#section-10.12 for more info
	bytes := make([]byte, 32)
	rand.Read(bytes)
	oauthStateString := fmt.Sprintf("%032x", bytes)

	// Store the state and where to redirect to after login in the cookie
	session, _ := a.CookieStore.Get(r, cookieName)
	session.Values["auth-redirect-to"] = r.FormValue("redirect_to")
	session.Values["auth-state"] = oauthStateString
	session.Save(r, w)

	log.Printf("Redirect To: %v State: %v", r.FormValue("redirect_to"), oauthStateString)

	url := a.OauthConfig.AuthCodeURL(oauthStateString, oauth2.AccessTypeOnline)
	http.Redirect(w, r, url, http.StatusTemporaryRedirect)
}

func (a *GithubAuth) LogoutHandler(w http.ResponseWriter, r *http.Request) {
	session, _ := a.CookieStore.Get(r, cookieName)

	delete(session.Values, "login-time")
	delete(session.Values, "login-name")
	delete(session.Values, "auth-state")
	delete(session.Values, "auth-token")
	delete(session.Values, "auth-redirect-to")
	session.Save(r, w)

	applicationAccessURL := fmt.Sprintf("%s/settings/connections/applications/%s",
		a.GithubServer, a.OauthConfig.ClientID)

	http.Redirect(w, r, applicationAccessURL, http.StatusFound)
}
