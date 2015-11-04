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

func (a *GithubAuth) exchangeToken(code string, state string) (*oauth2.Token, error) {
	resp, err := http.PostForm(a.OauthConfig.Endpoint.TokenURL, url.Values{
		"client_id":     {a.OauthConfig.ClientID},
		"client_secret": {a.OauthConfig.ClientSecret},
		"code":          {code},
		"state":         {state}})

	if err != nil {
		return nil, fmt.Errorf("Error getting token: %s", err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, fmt.Errorf("Error fetching OAuth token: %s", err)
	}

	statusCode := resp.StatusCode
	if statusCode < 200 || statusCode > 299 {
		return nil, fmt.Errorf("Got HTTP error code %d", statusCode)
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, fmt.Errorf("Cannot parse OAuth response: %s", err)
	}

	return &oauth2.Token{
		AccessToken: vals.Get("access_token"),
		TokenType:   vals.Get("token_type"),
	}, nil
}

func responseBodyToMap(r *http.Response) (map[string]interface{}, error) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)

	if err != nil {
		return nil, fmt.Errorf("Error reading body from response: %s", err)
	}

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)

	if err != nil {
		return nil, fmt.Errorf("Error unmarshal attempt: %s", err)
	}

	return result, nil
}

func (a *GithubAuth) AuthCallbackHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("AuthCallbackHandler")
	session, _ := a.CookieStore.Get(r, cookieName)

	r.ParseForm()

	expectedState := session.Values["auth-state"]
	if expectedState == nil {
		log.Printf("AuthCallbackHandler: No auth state variable found in cookie\n")
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	recievedState := r.FormValue("state")
	if expectedState != recievedState {
		log.Printf("Invalid oauth state! Expected '%v' got '%v'", expectedState, recievedState)
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	token, err := a.exchangeToken(r.FormValue("code"), recievedState)
	if err != nil {
		log.Printf("Unable to exchange token: %s", err)
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	client := a.OauthConfig.Client(oauth2.NoContext, token)
	resp, err := client.Get(a.GithubServer + "/api/v3/user")
	if err != nil {
		log.Printf("Error getting user info: %s", err)
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	userInfo, err := responseBodyToMap(resp)
	if err != nil {
		log.Println(err.Error())
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	if userInfo["login"] == nil {
		log.Println("User login not found in user info")
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	bytes, err := json.Marshal(token)
	if err != nil {
		log.Println("Error Marshalling oauth token:", err.Error())
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	session.Values["auth-token"] = bytes
	session.Values["login-time"] = time.Now().Unix()
	session.Values["login-name"] = userInfo["login"]

	redirectTarget := session.Values["auth-redirect-to"].(string)
	delete(session.Values, "auth-redirect-to")
	delete(session.Values, "auth-state")
	session.Save(r, w)

	http.Redirect(w, r, "/"+redirectTarget, http.StatusFound)
}

func (a *GithubAuth) LoginHandler(w http.ResponseWriter, r *http.Request) {
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
