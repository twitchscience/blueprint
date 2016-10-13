package auth

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/twitchscience/aws_utils/logger"
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

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logger.WithError(err).Error("Failed to close response body")
		}
	}()
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
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("Error reading body from response: %s", err)
	}
	defer func() {
		err = r.Body.Close()
		if err != nil {
			logger.WithError(err).Error("Failed to close response body")
		}
	}()

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)

	if err != nil {
		return nil, fmt.Errorf("Error unmarshal attempt: %s", err)
	}

	return result, nil
}

// AuthCallbackHandler receives the callback portion of the auth flow
func (a *GithubAuth) AuthCallbackHandler(w http.ResponseWriter, r *http.Request) {
	logger.Debug("AuthCallbackHandler")
	session, _ := a.CookieStore.Get(r, cookieName)

	err := r.ParseForm()
	if err != nil {
		logger.WithError(err).Error("Failed to parse form")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	expectedState := session.Values["auth-state"]
	if expectedState == nil {
		logger.Error("No auth state variable found in cookie")
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	receivedState := r.FormValue("state")
	if expectedState != receivedState {
		logger.WithFields(map[string]interface{}{
			"expected_state": expectedState,
			"received_state": receivedState,
		}).Error("Invalid oauth state")
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	token, err := a.exchangeToken(r.FormValue("code"), receivedState)
	if err != nil {
		logger.WithError(err).Error("Failed to exchange token")
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	client := a.OauthConfig.Client(oauth2.NoContext, token)
	resp, err := client.Get(a.GithubServer + "/api/v3/user")
	if err != nil {
		logger.WithError(err).Error("Failed to get user info")
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	userInfo, err := responseBodyToMap(resp)
	if err != nil {
		logger.WithError(err).Error("Failed to create map from response body")
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	if userInfo["login"] == nil {
		logger.Error("User login not found in user info")
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	bytes, err := json.Marshal(token)
	if err != nil {
		logger.WithError(err).Error("Failed to marshal oauth token")
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	if loginName, ok := userInfo["login"].(string); ok {
		http.SetCookie(w, &http.Cookie{Name: "displayName", Value: loginName,
			Secure: true, MaxAge: int(a.LoginTTL.Seconds())})
	} else {
		logger.Error("User login in user info is not a string")
		http.Error(w, "Error handling authentication response", http.StatusInternalServerError)
		return
	}

	session.Values["auth-token"] = bytes
	session.Values["login-time"] = time.Now().Unix()
	session.Values["login-name"] = userInfo["login"]

	redirectTarget := session.Values["auth-redirect-to"].(string)
	delete(session.Values, "auth-redirect-to")
	delete(session.Values, "auth-state")
	err = session.Save(r, w)
	if err != nil {
		logger.WithError(err).Error("Failed to save auth info to cookie")
		http.Error(w, "Error saving auth.", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/"+redirectTarget, http.StatusFound)
}

// LoginHandler handles the login portion of the auth flow
func (a *GithubAuth) LoginHandler(w http.ResponseWriter, r *http.Request) {
	// Generate random string to protect the user from CSRF attacks.
	// See http://tools.ietf.org/html/rfc6749#section-10.12 for more info
	bytes := make([]byte, 32)
	_, err := rand.Read(bytes)
	if err != nil {
		logger.WithError(err).Error("Failed to generate random string")
		http.Error(w, "Error logging in.", http.StatusInternalServerError)
		return
	}
	oauthStateString := fmt.Sprintf("%032x", bytes)

	// Store the state and where to redirect to after login in the cookie
	session, _ := a.CookieStore.Get(r, cookieName)
	session.Values["auth-redirect-to"] = r.FormValue("redirect_to")
	session.Values["auth-state"] = oauthStateString
	err = session.Save(r, w)
	if err != nil {
		logger.WithError(err).Error("Failed to save auth info to cookie")
		http.Error(w, "Error saving auth.", http.StatusInternalServerError)
		return
	}

	url := a.OauthConfig.AuthCodeURL(oauthStateString, oauth2.AccessTypeOnline)
	http.Redirect(w, r, url, http.StatusTemporaryRedirect)
}

// LogoutHandler handles the logout step of the auth flow
func (a *GithubAuth) LogoutHandler(w http.ResponseWriter, r *http.Request) {
	session, _ := a.CookieStore.Get(r, cookieName)

	http.SetCookie(w, &http.Cookie{Name: "displayName", MaxAge: 0})
	delete(session.Values, "login-time")
	delete(session.Values, "login-name")
	delete(session.Values, "auth-state")
	delete(session.Values, "auth-token")
	delete(session.Values, "auth-redirect-to")
	err := session.Save(r, w)
	if err != nil {
		logger.WithError(err).Error("Failed to wipe auth info from cookie")
		http.Error(w, "Error updating auth.", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/", http.StatusFound)
}

// DummyLoginHandler logs the unknown user in automatically.
func DummyLoginHandler(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: "displayName", Value: "unknown"})
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

// DummyLogoutHandler logs the unknown user out.
func DummyLogoutHandler(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: "displayName", MaxAge: 0})
	http.Redirect(w, r, "/", http.StatusFound)
}
