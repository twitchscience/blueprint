package auth

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/sessions"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/blueprint/core"
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
	if weberr := a.authCallbackHelper(w, r); weberr != nil {
		weberr.ReportError(w, "Failed to authorize user")
	}
}

func (a *GithubAuth) authCallbackHelper(w http.ResponseWriter, r *http.Request) *core.WebError {
	session, err := a.CookieStore.Get(r, cookieName)
	if err != nil {
		return core.NewServerWebError(fmt.Errorf("getting cookie: %v", err))
	}

	if err = r.ParseForm(); err != nil {
		return core.NewUserWebError(fmt.Errorf("parsing form: %v", err))
	}

	expectedState := session.Values["auth-state"]
	if expectedState == nil {
		return core.NewUserWebError(errors.New("no auth state found in cookie"))
	}

	receivedState := r.FormValue("state")
	if expectedState != receivedState {
		logger.WithFields(map[string]interface{}{
			"expected_state": expectedState,
			"received_state": receivedState,
		}).Warn("Invalid oauth state")
		return core.NewUserWebError(errors.New("invalid oauth state"))
	}

	token, err := a.exchangeToken(r.FormValue("code"), receivedState)
	if err != nil {
		return core.NewServerWebError(fmt.Errorf("exchanging token: %v", err))
	}

	resp, err := a.OauthConfig.Client(oauth2.NoContext, token).Get(a.GithubServer + "/api/v3/user")
	if err != nil {
		return core.NewServerWebError(fmt.Errorf("getting user infor from GitHub API: %v", err))
	}

	userInfo, err := responseBodyToMap(resp)
	if err != nil {
		return core.NewServerWebError(fmt.Errorf("creating map from response body: %v", err))
	} else if userInfo["login"] == nil {
		return core.NewServerWebError(errors.New("login not found in user info"))
	}

	bytes, err := json.Marshal(token)
	if err != nil {
		return core.NewServerWebError(fmt.Errorf("marshaling OAuth token: %v", err))
	}

	loginName, ok := userInfo["login"].(string)
	if !ok {
		return core.NewServerWebError(errors.New("login in user info not a string"))
	}
	session.Values["auth-token"] = bytes
	session.Values["login-time"] = time.Now().Unix()
	session.Values["login-name"] = userInfo["login"]

	isOrgMember, err := a.userIsOrgMember(token, session)
	if err != nil {
		return core.NewServerWebError(fmt.Errorf("getting org member status: %v", err))
	} else if !isOrgMember {
		delete(session.Values, "auth-token")
		delete(session.Values, "login-time")
		delete(session.Values, "login-name")

		clearCookies(w)
		http.SetCookie(w, &http.Cookie{
			Name:  "loginError",
			Value: "You are not a member of the approved org.",
		})

		return redirectAfterLoginAttempt(w, r, session)
	}

	http.SetCookie(w, &http.Cookie{
		Name:   "displayName",
		Value:  loginName,
		Secure: true,
		MaxAge: int(a.LoginTTL.Seconds()),
	})

	isAdmin, err := a.userIsAdmin(token, session)
	if err != nil {
		return core.NewServerWebError(fmt.Errorf("getting admin status: %v", err))
	}
	http.SetCookie(w, &http.Cookie{
		Name:   "isAdmin",
		Value:  strconv.FormatBool(isAdmin),
		Secure: true,
		MaxAge: int(a.LoginTTL.Seconds()),
	})

	return redirectAfterLoginAttempt(w, r, session)
}

func redirectAfterLoginAttempt(w http.ResponseWriter, r *http.Request, session *sessions.Session) *core.WebError {
	redirectTarget := session.Values["auth-redirect-to"].(string)
	delete(session.Values, "auth-redirect-to")
	delete(session.Values, "auth-state")

	if err := session.Save(r, w); err != nil {
		return core.NewServerWebError(fmt.Errorf("saving session info to cookie: %v", err))
	}

	http.Redirect(w, r, "/"+redirectTarget, http.StatusFound)
	return nil
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

	clearCookies(w)
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
	http.SetCookie(w, &http.Cookie{Name: "isAdmin", Value: "true"})
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

// DummyLogoutHandler logs the unknown user out.
func DummyLogoutHandler(w http.ResponseWriter, r *http.Request) {
	clearCookies(w)
	http.Redirect(w, r, "/", http.StatusFound)
}
